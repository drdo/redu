use std::cell::RefCell;
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::rc::Rc;

use camino::Utf8PathBuf;
use futures::{Stream, stream, StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::process::{ChildStdout, Command};
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};

use crate::types::{File, Snapshot};

#[derive(Debug)]
pub struct LaunchError(std::io::Error);

#[derive(Debug)]
pub enum ErrorKind {
    Launch(LaunchError),
    Io(std::io::Error),
    MaxLineLengthExceeded,
    Parse(serde_json::Error),
    Exit(ExitStatus),
}

impl From<LaunchError> for ErrorKind {
    fn from(value: LaunchError) -> Self {
        ErrorKind::Launch(value)
    }
}

impl From<std::io::Error> for ErrorKind {
    fn from(value: std::io::Error) -> Self {
        ErrorKind::Io(value)
    }
}

impl From<LinesCodecError> for ErrorKind {
    fn from(value: LinesCodecError) -> Self {
        match value {
            LinesCodecError::MaxLineLengthExceeded => ErrorKind::MaxLineLengthExceeded,
            LinesCodecError::Io(err) => ErrorKind::Io(err),
        }
    }
}

impl From<serde_json::Error> for ErrorKind {
    fn from(value: serde_json::Error) -> Self {
        ErrorKind::Parse(value)
    }
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    stderr: std::io::Result<Option<String>>,
}

impl From<LaunchError> for Error {
    fn from(value: LaunchError) -> Self {
        Error {
            kind: ErrorKind::Launch(value),
            stderr: Ok(None),
        }
    }
}

struct Handle {
    stdout: ChildStdout,
    stderr: Pin<Box<dyn Future<Output=Result<String, std::io::Error>>>>,
    wait: Pin<Box<dyn Future<Output=Result<ExitStatus, std::io::Error>>>>,
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

    pub async fn config(&self) -> Result<Config, Error> {
        self.run_greedy_command(["cat", "config"]).await
    }

    pub async fn snapshots(&self) -> Result<Vec<Snapshot>, Error> {
        self.run_greedy_command(["snapshots"]).await
    }

    pub fn ls<'a>(
        &'a self,
        snapshot: &str,
    ) -> Pin<Box<dyn Stream<Item=Result<(File, usize), Error>> + 'a>>
    {
        fn parse_file(mut v: Value) -> Option<File> {
            let mut m = std::mem::take(v.as_object_mut()?);
            Some(File {
                path: Utf8PathBuf::from(m.remove("path")?.as_str()?),
                size: m.remove("size")?.as_u64()? as usize,
            })
        }

        Box::pin(self.run_lazy_command::<Value, _>(["ls", snapshot])
            .try_filter_map({
                let mut accum = 0;
                move |(value, bytes_read)| async move {
                    accum += bytes_read;
                    Ok(parse_file(value).map(|file| {
                        let r = (file, accum);
                        accum = 0;
                        r
                    }))
                }
            })
        )
    }

    pub fn run_lazy_command<'a, T, A>(
        &'a self,
        args: impl IntoIterator<Item=A>,
    ) -> Pin<Box<dyn Stream<Item=Result<(T, usize), Error>> + 'a>>
    where
        T: DeserializeOwned,
        OsString: From<A>,
    {
        // We are creating a Stream that produces another Stream and flattening it
        // The point is to run the restic process as part of awaiting
        // on the first element of the stream
        let args = args
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<OsString>>();
        let wrapped_stream = stream::once(async move {
            let handle = self.run_command(args).await?;

            // This stream produces the entries
            let entries = FramedRead::new(handle.stdout, LinesCodec::new())
                .map(|line| {
                    let line = line?;
                    let value = serde_json::from_str(line.as_str())?;
                    let bytes_read = line.as_bytes().len();
                    Ok((value, bytes_read))
                });
            // This stream checks if the status is ok and then produces
            // either nothing or an error.
            let wait = Rc::new(RefCell::new(handle.wait));
            let final_status = stream::try_unfold((), move |()| {
                let wait = Rc::clone(&wait);
                async move {
                    let status = (&mut *wait.borrow_mut()).await?;
                    if status.success() {
                        Ok(None)
                    } else {
                        Err(ErrorKind::Exit(status))
                    }
                }
            });

            let stderr = Rc::new(RefCell::new(handle.stderr));
            Ok::<_, Error>(entries
               .chain(final_status)
               .or_else(move |kind| {
                   let stderr = Rc::clone(&stderr);
                   async move {
                       Err(Error {
                           kind,
                           stderr: (&mut *stderr.borrow_mut()).await.map(Some)
                       })
                   }
               })
            )
        });
        Box::pin(wrapped_stream.try_flatten())
    }

    async fn run_greedy_command<'a, T, A>(
        &self,
        args: impl IntoIterator<Item=A>,
    ) -> Result<T, Error>
    where
        T: DeserializeOwned,
        A: AsRef<OsStr> + 'static,
    {
        let mut handle = self.run_command(args).await?;
        let r_value = try {
            let stdout = {
                let mut buf = String::new();
                handle.stdout.read_to_string(&mut buf).await?;
                buf
            };
            let t = serde_json::from_str(&stdout)?;
            let status = handle.wait.await?;
            if status.success() {
                t
            } else {
                Err(ErrorKind::Exit(status))?
            }
        };
        match r_value {
            Err(e) => Err(Error {
                kind: e,
                stderr: handle.stderr.await.map(Some)
            }),
            Ok(value) => Ok(value),
        }
    }

    async fn run_command<'a, A: AsRef<OsStr> + 'static>(
        &self,
        args: impl IntoIterator<Item=A>,
    ) -> Result<Handle, Error>
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
        let mut child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(LaunchError)?;
        Ok(Handle {
            stdout: child.stdout.take().unwrap(),
            stderr: {
                let mut stderr = child.stderr.take().unwrap();
                Box::pin(async move {
                    let mut buf = String::new();
                    stderr.read_to_string(&mut buf).await?;
                    Ok::<String, std::io::Error>(buf)
                })
            },
            wait: Box::pin(async move { child.wait().await }),
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub id: String,
}
