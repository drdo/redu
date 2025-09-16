use core::str;
#[cfg(not(target_os = "windows"))]
use std::os::unix::process::CommandExt;
use std::{
    borrow::Cow,
    collections::HashSet,
    ffi::OsStr,
    fmt::{self, Display, Formatter},
    io::{self, BufRead, BufReader, Lines, Read, Write},
    marker::PhantomData,
    mem,
    process::{Child, ChildStdout, Command, Stdio},
    str::Utf8Error,
};

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use log::info;
use scopeguard::defer;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("error launching restic process")]
pub struct LaunchError(#[source] pub io::Error);

#[derive(Debug, Error)]
pub enum RunError {
    #[error("error doing IO")]
    Io(#[from] io::Error),
    #[error("error reading input as UTF-8")]
    Utf8(#[from] Utf8Error),
    #[error("error parsing JSON")]
    Parse(#[from] serde_json::Error),
    #[error("the restic process exited with error code {}", if let Some(code) = .0 { code.to_string() } else { "None".to_string() } )]
    Exit(Option<i32>),
}

#[derive(Debug, Error)]
pub enum ErrorKind {
    #[error("error launching restic process")]
    Launch(#[from] LaunchError),
    #[error("error while running restic process")]
    Run(#[from] RunError),
}

impl From<io::Error> for ErrorKind {
    fn from(value: io::Error) -> Self {
        ErrorKind::Run(RunError::Io(value))
    }
}

impl From<Utf8Error> for ErrorKind {
    fn from(value: Utf8Error) -> Self {
        ErrorKind::Run(RunError::Utf8(value))
    }
}

impl From<serde_json::Error> for ErrorKind {
    fn from(value: serde_json::Error) -> Self {
        ErrorKind::Run(RunError::Parse(value))
    }
}

#[derive(Debug, Error)]
pub struct Error {
    #[source]
    pub kind: ErrorKind,
    pub stderr: Option<String>,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.stderr {
            Some(s) => write!(f, "restic error, stderr dump:\n{}", s),
            None => write!(f, "restic error"),
        }
    }
}

impl From<LaunchError> for Error {
    fn from(value: LaunchError) -> Self {
        Error { kind: ErrorKind::Launch(value), stderr: None }
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub id: String,
}

pub struct Restic {
    repository: Repository,
    password: Password,
    no_cache: bool,
}

#[derive(Debug)]
pub enum Repository {
    /// A repository string (restic: --repo)
    Repo(String),
    /// A repository file (restic: --repository-file)
    File(String),
}

#[derive(Debug)]
pub enum Password {
    /// A plain string (restic: RESTIC_PASSWORD env variable)
    Plain(String),
    /// A password command (restic: --password-command)
    Command(String),
    /// A password file (restic: --password-file)
    File(String),
}

impl Restic {
    pub fn new(
        repository: Repository,
        password: Password,
        no_cache: bool,
    ) -> Self {
        Restic { repository, password, no_cache }
    }

    pub fn config(&self) -> Result<Config, Error> {
        self.run_greedy_command(["cat", "config"])
    }

    pub fn snapshots(&self) -> Result<Vec<Snapshot>, Error> {
        self.run_greedy_command(["snapshots"])
    }

    pub fn ls(
        &self,
        snapshot: &str,
    ) -> Result<impl Iterator<Item = Result<File, Error>> + 'static, LaunchError>
    {
        fn parse_file(mut v: Value) -> Option<File> {
            let mut m = mem::take(v.as_object_mut()?);
            Some(File {
                path: Utf8PathBuf::from(m.remove("path")?.as_str()?),
                size: m.remove("size")?.as_u64()? as usize,
            })
        }

        Ok(self
            .run_lazy_command(["ls", snapshot])?
            .filter_map(|r| r.map(parse_file).transpose()))
    }

    // This is a trait object because of
    // https://github.com/rust-lang/rust/issues/125075
    fn run_lazy_command<T, A>(
        &self,
        args: impl IntoIterator<Item = A>,
    ) -> Result<Box<dyn Iterator<Item = Result<T, Error>> + 'static>, LaunchError>
    where
        T: DeserializeOwned + 'static,
        A: AsRef<OsStr>,
    {
        let child = self.run_command(args)?;
        Ok(Box::new(Iter::new(child)))
    }

    fn run_greedy_command<T, A>(
        &self,
        args: impl IntoIterator<Item = A>,
    ) -> Result<T, Error>
    where
        T: DeserializeOwned,
        A: AsRef<OsStr>,
    {
        let child = self.run_command(args)?;
        let id = child.id();
        defer! { info!("finished pid {}", id); }
        let output = child.wait_with_output().map_err(|e| Error {
            kind: ErrorKind::Run(RunError::Io(e)),
            stderr: None,
        })?;
        let r_value: Result<T, ErrorKind> = if output.status.success() {
            match str::from_utf8(&output.stdout) {
                Ok(s) => serde_json::from_str(s).map_err(|e| e.into()),
                Err(e) => Err(e.into()),
            }
        } else {
            Err(ErrorKind::Run(RunError::Exit(output.status.code())))
        };
        match r_value {
            Err(kind) => Err(Error {
                kind,
                stderr: Some(
                    String::from_utf8_lossy(&output.stderr).into_owned(),
                ),
            }),
            Ok(value) => Ok(value),
        }
    }

    fn run_command<A: AsRef<OsStr>>(
        &self,
        args: impl IntoIterator<Item = A>,
    ) -> Result<Child, LaunchError> {
        let mut cmd = Command::new("restic");
        // Need to detach process from terminal
        #[cfg(not(target_os = "windows"))]
        unsafe {
            cmd.pre_exec(|| {
                nix::unistd::setsid()?;
                Ok(())
            });
        }
        match &self.repository {
            Repository::Repo(repo) => cmd.arg("--repo").arg(repo),
            Repository::File(file) => cmd.arg("--repository-file").arg(file),
        };
        match &self.password {
            Password::Command(command) => {
                cmd.arg("--password-command").arg(command);
                cmd.stdin(Stdio::null());
            }
            Password::File(file) => {
                cmd.arg("--password-file").arg(file);
                cmd.stdin(Stdio::null());
            }
            Password::Plain(_) => {
                // passed via stdin after the process is started
                cmd.stdin(Stdio::piped());
            }
        };
        if self.no_cache {
            cmd.arg("--no-cache");
        }
        cmd.arg("--json");
        // pass --quiet to remove informational messages in stdout mixed up with the JSON we want
        // (https://github.com/restic/restic/issues/5236)
        cmd.arg("--quiet");
        cmd.args(args);
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(LaunchError)?;
        info!("running \"{cmd:?}\" (pid {})", child.id());
        if let Password::Plain(ref password) = self.password {
            let mut stdin = child
                .stdin
                .take()
                .expect("child has no stdin when it should have");
            stdin.write_all(password.as_bytes()).map_err(LaunchError)?;
            stdin.write_all(b"\n").map_err(LaunchError)?;
        }
        Ok(child)
    }
}

struct Iter<T> {
    child: Child,
    lines: Lines<BufReader<ChildStdout>>,
    finished: bool,
    _phantom_data: PhantomData<T>,
}

impl<T> Iter<T> {
    fn new(mut child: Child) -> Self {
        let stdout = child.stdout.take().unwrap();
        Iter {
            child,
            lines: BufReader::new(stdout).lines(),
            finished: false,
            _phantom_data: PhantomData,
        }
    }

    fn read_stderr<U>(&mut self, kind: ErrorKind) -> Result<U, Error> {
        let mut buf = String::new();
        // read_to_string would block forever if the child was still running.
        let _ = self.child.kill();
        match self.child.stderr.take().unwrap().read_to_string(&mut buf) {
            Err(e) => Err(Error {
                kind: ErrorKind::Run(RunError::Io(e)),
                stderr: None,
            }),
            Ok(_) => Err(Error { kind, stderr: Some(buf) }),
        }
    }

    fn finish(&mut self) {
        if !self.finished {
            info!("finished pid {}", self.child.id());
        }
    }
}

impl<T: DeserializeOwned> Iterator for Iter<T> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(r_line) = self.lines.next() {
            let r_value: Result<T, ErrorKind> =
                r_line.map_err(|e| e.into()).and_then(|line| {
                    serde_json::from_str(&line).map_err(|e| e.into())
                });
            Some(match r_value {
                Err(kind) => {
                    self.finish();
                    self.read_stderr(kind)
                }
                Ok(value) => Ok(value),
            })
        } else {
            self.finish();
            match self.child.wait() {
                Err(e) => {
                    Some(self.read_stderr(ErrorKind::Run(RunError::Io(e))))
                }
                Ok(status) => {
                    if status.success() {
                        None
                    } else {
                        Some(self.read_stderr(ErrorKind::Run(RunError::Exit(
                            status.code(),
                        ))))
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Snapshot {
    pub id: String,
    pub time: DateTime<Utc>,
    #[serde(default)]
    pub parent: Option<String>,
    pub tree: String,
    pub paths: HashSet<String>,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub uid: Option<u32>,
    #[serde(default)]
    pub gid: Option<u32>,
    #[serde(default)]
    pub excludes: HashSet<String>,
    #[serde(default)]
    pub tags: HashSet<String>,
    #[serde(default)]
    pub original_id: Option<String>,
    #[serde(default)]
    pub program_version: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct File {
    pub path: Utf8PathBuf,
    pub size: usize,
}

pub fn escape_for_exclude(path: &str) -> Cow<'_, str> {
    fn is_special(c: char) -> bool {
        ['*', '?', '[', '\\', '\r', '\n'].contains(&c)
    }

    fn char_backward(c: char) -> char {
        char::from_u32(
            (c as u32).checked_sub(1).expect(
                "char_backward: underflow when computing previous char",
            ),
        )
        .expect("char_backward: invalid resulting character")
    }

    fn char_forward(c: char) -> char {
        char::from_u32(
            (c as u32)
                .checked_add(1)
                .expect("char_backward: overflow when computing next char"),
        )
        .expect("char_forward: invalid resulting character")
    }

    fn push_as_inverse_range(buf: &mut String, c: char) {
        #[rustfmt::skip]
        let cs = [
            '[', '^',
            char::MIN, '-', char_backward(c),
            char_forward(c), '-', char::MAX,
            ']',
        ];
        for d in cs {
            buf.push(d);
        }
    }

    match path.find(is_special) {
        None => Cow::Borrowed(path),
        Some(index) => {
            let (left, right) = path.split_at(index);
            let mut escaped = String::with_capacity(path.len() + 1); // the +1 is for the extra \
            escaped.push_str(left);
            for c in right.chars() {
                match c {
                    '*' | '?' | '[' => {
                        escaped.push('[');
                        escaped.push(c);
                        escaped.push(']');
                    }
                    '\\' => {
                        #[cfg(target_os = "windows")]
                        escaped.push('\\');
                        #[cfg(not(target_os = "windows"))]
                        escaped.push_str("\\\\");
                    }
                    '\r' | '\n' => push_as_inverse_range(&mut escaped, c),
                    c => escaped.push(c),
                }
            }
            Cow::Owned(escaped)
        }
    }
}

#[cfg(test)]
mod test {
    use super::escape_for_exclude;

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn escape_for_exclude_test() {
        assert_eq!(
            escape_for_exclude("foo* bar?[somethin\\g]]]\r\n"),
            "foo[*] bar[?][[]somethin\\\\g]]][^\0-\u{000C}\u{000E}-\u{10FFFF}][^\0-\u{0009}\u{000B}-\u{10FFFF}]"
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn escape_for_exclude_test() {
        assert_eq!(
            escape_for_exclude("foo* bar?[somethin\\g]]]\r\n"),
            "foo[*] bar[?][[]somethin\\g]]][^\0-\u{000C}\u{000E}-\u{10FFFF}][^\0-\u{0009}\u{000B}-\u{10FFFF}]"
        );
    }
}
