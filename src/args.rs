use clap::{ArgGroup, Parser};
use log::LevelFilter;
use redu::restic::Repository;
use rpassword::read_password;

use crate::restic::Password;

#[derive(Debug)]
pub struct Args {
    pub repository: Repository,
    pub password: Password,
    pub parallelism: usize,
    pub log_level: LevelFilter,
    pub no_cache: bool,
    pub rustic: bool,
}

impl Args {
    /// Parse arguments from env::args_os(), exit on error.
    pub fn parse() -> Self {
        let cli = Cli::parse();

        Args {
            repository: if let Some(repo) = cli.repo {
                Repository::Repo(repo)
            } else if let Some(file) = cli.repository_file {
                Repository::File(file)
            } else {
                unreachable!("Error in Config: neither repo nor repository_file found. Please open an issue if you see this.")
            },
            password: if let Some(command) = cli.password_command {
                Password::Command(command)
            } else if let Some(file) = cli.password_file {
                Password::File(file)
            } else if let Some(str) = cli.restic_password {
                Password::Plain(str)
            } else {
                Password::Plain(Self::read_password_from_stdin())
            },
            parallelism: cli.parallelism,
            log_level: match cli.verbose {
                0 => LevelFilter::Info,
                1 => LevelFilter::Debug,
                _ => LevelFilter::Trace,
            },
            no_cache: cli.no_cache,
            rustic: cli.rustic,
        }
    }

    fn read_password_from_stdin() -> String {
        eprint!("enter password for repository: ");
        read_password().unwrap()
    }
}

/// This is like ncdu for a restic respository.
///
/// It computes the size for each directory/file by
/// taking the largest over all snapshots in the repository.
///
/// You can browse your repository and mark directories/files.
/// These marks are persisted across runs of redu.
///
/// When you're happy with the marks you can generate
/// a list to stdout with everything that you marked.
///   This list can be used directly as an exclude-file for restic.
///
/// Redu keeps all messages and UI in stderr,
/// only the marks list is generated to stdout.
///   This means that you can pipe redu directly to a file
/// to get the exclude-file.
///
/// NOTE: redu will never do any kind of modification to your repo.
/// It's strictly read-only.
///
/// Keybinds:
/// Arrows or hjkl: Movement
/// PgUp/PgDown or C-b/C-f: Page up / Page down
/// Enter: Details
/// Escape: Close dialog
/// m: Mark
/// u: Unmark
/// c: Clear all marks
/// g: Generate
/// q: Quit
#[derive(Parser)]
#[command(version, long_about, verbatim_doc_comment)]
#[command(group(
    ArgGroup::new("repository")
        .required(true)
        .args(["repo", "repository_file"]),
))]
struct Cli {
    #[arg(short = 'r', long, env = "RESTIC_REPOSITORY")]
    repo: Option<String>,

    #[arg(long, env = "RESTIC_REPOSITORY_FILE")]
    repository_file: Option<String>,

    #[arg(long, value_name = "COMMAND", env = "RESTIC_PASSWORD_COMMAND")]
    password_command: Option<String>,

    #[arg(long, value_name = "FILE", env = "RESTIC_PASSWORD_FILE")]
    password_file: Option<String>,

    #[arg(value_name = "RESTIC_PASSWORD", env = "RESTIC_PASSWORD")]
    restic_password: Option<String>,

    ///  How many restic subprocesses to spawn concurrently.
    ///
    /// If you get ssh-related errors or too much memory use try lowering this.
    #[arg(short = 'j', value_name = "NUMBER", default_value_t = 4)]
    parallelism: usize,

    /// Log verbosity level. You can pass it multiple times (maxes out at two).
    #[arg(
        short = 'v',
        action = clap::ArgAction::Count,
    )]
    verbose: u8,

    /// Pass the --no-cache option to restic subprocesses.
    #[arg(long)]
    no_cache: bool,

    /// use build-in rustic functionality instead of calling the restic binary
    #[arg(long)]
    rustic: bool,
}
