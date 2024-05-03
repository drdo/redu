use camino::Utf8PathBuf;

#[derive(Clone, Debug)]
pub struct File {
    pub path: Utf8PathBuf,
    pub size: usize,
}

