use std::pin::Pin;
use std::process::{ExitStatus, Stdio};
use std::rc::Rc;

use camino::Utf8PathBuf;
use futures::{Stream, stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};

use crate::types::{File, Snapshot};

pub trait LineStream: Stream<Item=Result<String, LinesCodecError>> {}
impl<S: Stream<Item=Result<String, LinesCodecError>>> LineStream for S {}

pub type Output<T> = (T, Pin<Box<dyn LineStream>>);
pub type GreedyOutput<T> = Output<Result<T, Error>>;
pub type StreamOutput<T> = Output<Pin<Box<dyn Stream<Item=Result<T, Error>>>>>;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    MaxLineLengthExceeded,
    Parse(serde_json::Error),
    Exit(ExitStatus),
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

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Error::Parse(value)
    }
}

#[derive(Clone)]
pub struct Restic {
    repo: String,
    password_command: Option<String>,
}

impl Restic {
    pub fn new(repo: &str, password_command: Option<&str>) -> Self {
        Restic { repo: String::from(repo), password_command: password_command.map(String::from) }
    }

    /// Panics if we cannot launch restic
    async fn run_command<'a>(
        &self, args: impl IntoIterator<Item=&'a str>
    ) -> Child
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
        cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("should be able to launch restic")
    }

    pub async fn config(&self) -> GreedyOutput<Config>
    {
        json_from_stdout(self.run_command(["cat", "config"]).await).await
    }

    pub async fn snapshots(&self) -> GreedyOutput<Vec<Snapshot>>
    {
        json_from_stdout(self.run_command(["snapshots"]).await).await
    }

    pub async fn ls(&self, snapshot: &str) -> StreamOutput<File>
    {
        let parse_entry = {
            let snapshot: Rc<str> = Rc::from(snapshot);
            move |mut v: Value| -> Option<File> {
                let mut m = std::mem::take(v.as_object_mut()?);
                Some(File {
                    snapshot: snapshot.clone(),
                    path: Utf8PathBuf::from(m.remove("path")?.as_str()?),
                    size: m.remove("size")?.as_u64()? as usize,
                })
            }
        };

        let mut child = self.run_command(["ls", snapshot]).await;
        let stderr = FramedRead::new(child.stderr.take().unwrap(), LinesCodec::new());
        let entries = FramedRead::new(child.stdout.take().unwrap(), LinesCodec::new())
            .filter_map(move |line| {
                let x = match try { serde_json::from_str::<Value>(line?.as_str())? } {
                    Err(err) => Some(Err(err)),
                    Ok(value) => parse_entry(value).map(Ok),
                };
                async { x }
            });
        let final_status = stream::try_unfold(child, |mut child| async move {
            let status = child.wait().await?;
            if status.success() {
                Ok(None)
            } else {
                Err(Error::Exit(status))
            }
        });
        let stream = entries.chain(final_status);
        (Box::pin(stream), Box::pin(stderr))
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub version: u32,
    pub id: String,
}

////////////////////////////////////////////////////////////////////////////////
/// Panics if there is no stdout or stderr
async fn json_from_stdout<T: DeserializeOwned>(
    mut child: Child,
) -> (Result<T, Error>, Pin<Box<dyn LineStream>>)
{
    let stderr = FramedRead::new(child.stderr.take().unwrap(), LinesCodec::new());
    let res = try {
        let out = {
            let mut buf = String::new();
            child.stdout.take().unwrap().read_to_string(&mut buf).await?;
            buf
        };
        let config = serde_json::from_str(&out)?;
        let status = child.wait().await?;
        if status.success() {
            config
        } else {
            Err(Error::Exit(status))?
        }
    };
    (res, Box::pin(stderr))
}
