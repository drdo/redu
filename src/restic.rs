use std::cell::RefCell;
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
pub enum Error {
    Launch(std::io::Error),
    Io(std::io::Error),
    MaxLineLengthExceeded,
    Parse {
        inner: serde_json::Error,
        stderr: String,
    },
    Exit {
        status: ExitStatus,
        stderr: String,
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<LinesCodecError> for Error {
    fn from(value: LinesCodecError) -> Self {
        match value {
            LinesCodecError::MaxLineLengthExceeded => Error::MaxLineLengthExceeded,
            LinesCodecError::Io(err) => Error::Io(err),
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

    /// Panics if we cannot launch restic
    async fn run_command<'a>(
        &self, args: impl IntoIterator<Item=&'a str>
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
            .map_err(Error::Launch)?;
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

    pub async fn config(&self) -> Result<Config, Error> {
        json_from_stdout(self.run_command(["cat", "config"]).await?).await
    }

    pub async fn snapshots(&self) -> Result<Vec<Snapshot>, Error> {
        json_from_stdout(self.run_command(["snapshots"]).await?).await
    }

    pub async fn ls(
        &self,
        snapshot: &str,
    ) -> Pin<Box<dyn Stream<Item=Result<File, Error>> + '_>>
    {
        fn parse_file(mut v: Value) -> Option<File> {
            let mut m = std::mem::take(v.as_object_mut()?);
            Some(File {
                path: Utf8PathBuf::from(m.remove("path")?.as_str()?),
                size: m.remove("size")?.as_u64()? as usize,
            })
        }

        // We are creating a Stream that produces another Stream and flattening it
        // The point is to run the restic process as part of awaiting
        // on the first element of the stream
        let snapshot = Box::from(snapshot);
        let wrapped_stream = stream::once(async move {
            let handle = self.run_command(["ls", &snapshot]).await?;
            let stderr = Rc::new(RefCell::new(handle.stderr));
            let wait = Rc::new(RefCell::new(handle.wait));

            // This stream produces the entries
            let entries = FramedRead::new(handle.stdout, LinesCodec::new())
                .err_into()
                .try_filter_map({
                    let stderr = Rc::clone(&stderr);
                    move |line| {
                        let r_value = serde_json::from_str::<Value>(line.as_str());
                        let stderr = Rc::clone(&stderr);
                        async move {
                            match r_value {
                                Err(inner) => {
                                    Err(Error::Parse {
                                        inner,
                                        stderr: (&mut *stderr.borrow_mut()).await?
                                    })
                                },
                                Ok(value) => Ok(Some(value))
                            }
                        }
                    }
                })
                .try_filter_map(move |value| async {
                    Ok(parse_file(value))
                });
            // This stream checks if the status is ok and then produces
            // either nothing or an error.
            let final_status = {
                stream::try_unfold((), move |()| {
                    let stderr = Rc::clone(&stderr);
                    let wait = Rc::clone(&wait);
                    async move {
                        let status = (&mut *wait.borrow_mut()).await?;
                        if status.success() {
                            Ok(None)
                        } else {
                            let x = &mut *stderr.borrow_mut();
                            let y = x;
                            Err(Error::Exit {
                                status,
                                stderr: y.await?
                            })
                        }
                    }
                })
            };
            Ok::<_, Error>(entries.chain(final_status))
        });
        Box::pin(wrapped_stream.try_flatten())
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub id: String,
}

////////////////////////////////////////////////////////////////////////////////
/// Panics if there is no stdout or no stderr.
async fn json_from_stdout<T: DeserializeOwned>(
    mut handle: Handle,
) -> Result<T, Error>
{
    let stdout = {
        let mut buf = String::new();
        handle.stdout.read_to_string(&mut buf).await?;
        buf
    };
    let t = match serde_json::from_str(&stdout) {
        Ok(t) => t,
        Err(err) => return Err(Error::Parse {
            inner: err,
            stderr: handle.stderr.await?
        })
    };
    let status = handle.wait.await?;
    if status.success() {
        Ok(t)
    } else {
        Err(Error::Exit { status, stderr: handle.stderr.await? })
    }
}
