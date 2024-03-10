use crate::types::{File, Snapshot};
use serde_json::{Deserializer, Value};
use std::io::BufReader;
use std::process::{Command, Stdio};

#[derive(Debug)]
pub enum Error {
    IOError(std::io::Error),
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

    fn new_command(&self) -> Command {
        let mut cmd = Command::new("restic");
        cmd.arg("--repo").arg(&self.repo);
        for password_command in &self.password_command {
            cmd.arg("--password-command").arg(password_command);
        }
        cmd.arg("--json");
        cmd
    }

    pub fn snapshots(&self) -> Result<Vec<Snapshot>, Error> {
        let child_stdout = self.new_command()
            .arg("snapshots")
            .stdout(Stdio::piped())
            .spawn()?
            .stdout
            .expect("Stdout expected. This should not happen");
        let mut reader = BufReader::new(child_stdout);
        Ok(serde_json::from_reader(&mut reader)?)
    }

    pub fn ls<'a>(
        &self,
        snapshot: &'a str,
    ) -> Result<impl Iterator<Item = Result<File<'a>, serde_json::Error>>, std::io::Error>
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

        let child_stdout = self.new_command()
            .arg("ls")
            .arg(snapshot)
            .stdout(Stdio::piped())
            .spawn()?
            .stdout
            .expect("Stdout expected. This should not happen");
        let reader = BufReader::new(child_stdout);
        Ok(Deserializer::from_reader(reader)
            .into_iter::<Value>()
            .filter_map(move |r| r.map(parse_entry).transpose()))
    }
}
