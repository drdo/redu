use std::ffi::OsStr;
use std::io::{BufRead, BufReader, Lines, Read};
use std::marker::PhantomData;
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStdout, Command, ExitStatusError, Stdio};
use std::str::Utf8Error;

use camino::Utf8PathBuf;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;

use crate::types::{File, Snapshot};

// TODO: Guard against panic unwind leaving processes around

#[derive(Debug)]
pub struct LaunchError(std::io::Error);

#[derive(Debug)]
pub enum RunError {
    Io(std::io::Error),
    Utf8(Utf8Error),
    Parse(serde_json::Error),
    Exit(ExitStatusError),
}

impl From<std::io::Error> for RunError {
    fn from(value: std::io::Error) -> Self {
        RunError::Io(value)
    }
}

impl From<Utf8Error> for RunError {
    fn from(value: Utf8Error) -> Self {
        RunError::Utf8(value)
    }
}

impl From<serde_json::Error> for RunError {
    fn from(value: serde_json::Error) -> Self {
        RunError::Parse(value)
    }
}

impl From<ExitStatusError> for RunError {
    fn from(value: ExitStatusError) -> Self {
        RunError::Exit(value)
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    Launch(LaunchError),
    Run(RunError),
}

impl From<LaunchError> for ErrorKind {
    fn from(value: LaunchError) -> Self {
        ErrorKind::Launch(value)
    }
}

impl From<RunError> for ErrorKind {
    fn from(value: RunError) -> Self {
        ErrorKind::Run(value)
    }
}

impl From<std::io::Error> for ErrorKind {
    fn from(value: std::io::Error) -> Self {
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

impl From<ExitStatusError> for ErrorKind {
    fn from(value: ExitStatusError) -> Self {
        ErrorKind::Run(RunError::Exit(value))
    }
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    stderr: Option<String>,
}

impl From<LaunchError> for Error {
    fn from(value: LaunchError) -> Self {
        Error {
            kind: ErrorKind::Launch(value.into()),
            stderr: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub id: String,
}

pub struct Restic {
    repo: String,
    password_command: Option<String>,
}

impl Restic {
    pub fn new(
        repo: String,
        password_command: Option<String>,
    ) -> Self
    {
        Restic { repo, password_command }
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
    ) -> Result<impl Iterator<Item=Result<(File, usize), Error>> + 'static, LaunchError>
    {
        fn parse_file(mut v: Value) -> Option<File> {
            let mut m = std::mem::take(v.as_object_mut()?);
            Some(File {
                path: Utf8PathBuf::from(m.remove("path")?.as_str()?),
                size: m.remove("size")?.as_u64()? as usize,
            })
        }

        Ok(self.run_lazy_command(["ls", snapshot])?
            .filter_map(|r| r
                .map(|(value, bytes_read)|
                     parse_file(value).map(|file| (file, bytes_read))
                )
                .transpose()))
    }

    // This is a trait object because of
    // https://github.com/rust-lang/rust/issues/125075
    pub fn run_lazy_command<T, A>(
        &self,
        args: impl IntoIterator<Item=A>,
    ) -> Result<Box<dyn Iterator<Item=Result<(T, usize), Error>> + 'static>, LaunchError>
    where
        T: DeserializeOwned + 'static,
        A: AsRef<OsStr>
    {
        let child = self.run_command(args)?;
        Ok(Box::new(Iter::new(child)))
    }

    fn run_greedy_command<T, A>(
        &self,
        args: impl IntoIterator<Item=A>,
    ) -> Result<T, Error>
    where
        T: DeserializeOwned,
        A: AsRef<OsStr>,
    {
        let child = self.run_command(args)?;
        let output = child.wait_with_output()
            .map_err(|e| Error {
                kind: ErrorKind::Run(RunError::Io(e)),
                stderr: None
            })?;
        let r_value = try {
            output.status.exit_ok()?;
            serde_json::from_str(std::str::from_utf8(&output.stdout)?)?
        };
        match r_value {
            Err(kind) => Err(Error {
                kind,
                stderr: Some(String::from_utf8_lossy(&output.stderr).into_owned()),
            }),
            Ok(value) => Ok(value),
        }
    }

    fn run_command<A: AsRef<OsStr>>(
        &self,
        args: impl IntoIterator<Item=A>,
    ) -> Result<Child, LaunchError>
    {
        let mut cmd = Command::new("restic");
        // Need to detach process from terminal
        unsafe { cmd.pre_exec(|| { nix::unistd::setsid()?; Ok(()) }); }
        cmd.arg("--repo").arg(&self.repo);
        for password_command in &self.password_command {
            cmd.arg("--password-command").arg(password_command);
        }
        cmd.arg("--json");
        cmd.args(args);
        Ok(cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(LaunchError)?)
    }
}

struct Iter<T> {
    child: Child,
    lines: Lines<BufReader<ChildStdout>>,
    _phantom_data: PhantomData<T>,
}

impl<T> Iter<T> {
    fn new(mut child: Child) -> Self {
        let stdout = child.stdout.take().unwrap();
        Iter {
            child,
            lines: BufReader::new(stdout).lines(),
            _phantom_data: PhantomData::default(),
        }
    }

    fn read_stderr<U>(&mut self, kind: ErrorKind) -> Result<U, Error>
    {
        let mut buf = String::new();
        match self.child.stderr.take().unwrap().read_to_string(&mut buf) {
            Err(e) => Err(Error {
                kind: ErrorKind::Run(RunError::Io(e)),
                stderr: None,
            }),
            Ok(_) => Err(Error {
                kind,
                stderr: Some(buf),
            })
        }
    }
}

impl<T: DeserializeOwned> Iterator for Iter<T> {
    type Item = Result<(T, usize), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(line) = self.lines.next() {
            let r_value = try {
                let line = line?;
                let value = serde_json::from_str(&line)?;
                (value, line.len())
            };
            Some(match r_value {
                Err(kind) => self.read_stderr(kind),
                Ok(value) => Ok(value),
            })
        } else {
            match self.child.wait() {
                Err(e) => Some(self.read_stderr(ErrorKind::Run(RunError::Io(e)))),
                Ok(status) => match status.exit_ok() {
                    Err(e) => Some(self.read_stderr(e.into())),
                    Ok(()) => None,
                }
            }
        }
    }
}

