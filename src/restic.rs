use std::io::BufReader;
use std::process::{Child, Command, ExitStatus, Stdio};

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{Deserializer, Value};

use crate::restic::Error::ExitError;
use crate::types::{File, Snapshot};

#[derive(Debug)]
pub enum Error {
    IOError(std::io::Error),
    ExitError(ExitStatus),
    ParseError(serde_json::Error),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::IOError(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Error::ParseError(value)
    }
}

pub struct Restic {
    repo: String,
    password_command: Option<String>,
}

impl Restic {
    pub fn new(repo: &str, password_command: Option<&str>) -> Self {
        Restic { repo: String::from(repo), password_command: password_command.map(String::from) }
    }

    fn run_command<'a>(
        &self, args: impl IntoIterator<Item=&'a str>
    ) -> Result<Child, Error>
    {
        let mut cmd = Command::new("restic");
        cmd.arg("--repo").arg(&self.repo);
        for password_command in &self.password_command {
            cmd.arg("--password-command").arg(password_command);
        }
        cmd.arg("--json");
        cmd.args(args);
        Ok(cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()?)
    }

    pub fn config(&self) -> Result<Config, Error> {
        json_from_stdout(&mut self.run_command(["cat", "config"])?)
    }

    pub fn snapshots(&self) -> Result<Vec<Snapshot>, Error> {
        json_from_stdout(&mut self.run_command(["snapshots"])?)
    }

    pub fn ls<'a>(
        &self,
        snapshot: &'a str,
    ) -> Result<impl Iterator<Item = Result<File<'a>, Error>>, Error>
    {
        let parse_entry = |mut v: Value| -> Option<File> {
            let mut m = std::mem::take(v.as_object_mut()?);
            let path = if let Value::String(s) = m.remove("path")? {
                s
            } else {
                return None;
            };
            let size = m.remove("size")?.as_u64()?;
            Some(File { snapshot, path, size })
        };

        let mut child = self.run_command(["ls", snapshot])?;
        let reader = BufReader::new(child.stdout.take().unwrap());
        Ok(
            Deserializer::from_reader(reader)
                .into_iter::<Value>()
                .filter_map(move |r: Result<Value, serde_json::Error>|
                    r.map(parse_entry).transpose())
                .map(move |r: Result<File, serde_json::Error>| {
                    let v = r?;
                    if let Some(status) = child.try_wait()? {
                        if ! status.success() {
                            return Err(Error::ExitError(status))
                        }
                    }
                    Ok(v)
                })

        )
    }
}

#[derive(Deserialize)]
pub struct Config {
    pub version: u32,
    pub id: String,
}

////////////////////////////////////////////////////////////////////////////////
/// Panics if there is no stdout
fn json_from_stdout<T: DeserializeOwned>(child: &mut Child) -> Result<T, Error> {
    let reader = BufReader::new(child.stdout.take().unwrap());
    let config = serde_json::from_reader(reader)?;
    let status = child.wait()?;
    if status.success() {
        Ok(config)
    } else {
        Err(ExitError(status))
    }
}
