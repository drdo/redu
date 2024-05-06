#![feature(iter_intersperse)]
#![feature(panic_update_hook)]
#![feature(try_blocks)]
#![feature(option_get_or_insert_default)]

use std::borrow::Cow;
use std::io::stdout;
use std::panic;

use camino::Utf8Path;
use clap::{command, Parser};
use crossterm::ExecutableCommand;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use flexi_logger::{FileSpec, Logger, WriteMode};
use futures::TryStreamExt;
use log::error;
use ratatui::{CompletedFrame, Terminal};
use ratatui::backend::{Backend, CrosstermBackend};
use ratatui::widgets::WidgetRef;

use component::app::App;

use crate::cache::Cache;
use crate::component::{Action, Event};
use crate::restic::Restic;
use crate::types::Entry;

mod cache;
mod restic;
mod types;
mod component;

macro_rules! with_greedy_stderr_logging {
    ($expr:expr) => {
        {
            use futures::StreamExt;
            let (x, mut stderr_stream) = $expr;
            while let Some(line) = stderr_stream.next().await {
                error!("stderr: {}", line.unwrap())
            }
            x
        }
    };
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short = 'r', long)]
    repo: String,
    #[arg(long)]
    password_command: Option<String>,
}


fn get_files(
    cache: &Cache,
    path: Option<&Utf8Path>,
) -> Result<Vec<Entry>, rusqlite::Error>
{
    cache.get_max_file_sizes(path)
}

fn render<'a>(
    terminal: &'a mut Terminal<impl Backend>,
    app: &App,
) -> std::io::Result<CompletedFrame<'a>> {
    terminal.draw(|frame| {
        let area = frame.size();
        let buf = frame.buffer_mut();
        app.render_ref(area, buf)
    })
}

fn handle_event(
    cache: &Cache,
    app: &mut App,
    event: Event,
) -> Result<Action, rusqlite::Error>
{
    app.handle_event(|path| get_files(cache, path), event)
}

fn convert_event(event: crossterm::event::Event) -> Option<Event> {
    use crossterm::event::Event as TermEvent;
    use crossterm::event::KeyEventKind::{Press, Release};
    use Event::*;
    match event {
        TermEvent::Resize(w, h) =>
            Some(Resize(w, h)),
        TermEvent::Key(event) if [Press, Release].contains(&event.kind) =>
            Some(KeyPress(event.code)),
        _ => None,
    }
}

#[tokio::main]
async fn main() {
    let _logger = Logger::try_with_str("trace")
        .unwrap()
        .log_to_file(FileSpec::default())
        .write_mode(WriteMode::Direct)
        .start()
        .unwrap();

    unsafe {
            rusqlite::trace::config_log(Some(|code, msg| {
            error!(target: "sqlite", "({code}) {msg}");
        }));
    }

    let cli = Cli::parse();
    let restic = Restic::new(&cli.repo, cli.password_command.as_ref().map(|s| s.as_str()));
    eprintln!("Getting restic config");
    let repo_id = restic.config().await.0.unwrap().id;
    let mut cache = Cache::open(repo_id.as_str()).unwrap();
    
    eprintln!("Using cache file '{}'", cache.filename());
    
    // Figure out what snapshots we need to update
    let snapshots: Vec<Box<str>> = {
        eprintln!("Fetching restic snapshot list");
        let restic_snapshots = with_greedy_stderr_logging!(restic.snapshots().await)
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect::<Vec<Box<str>>>();

        // Delete snapshots from the DB that were deleted on Restic
        for snapshot in cache.get_snapshots().unwrap() {
            if ! restic_snapshots.contains(&snapshot) {
                eprintln!("Deleting DB Snapshot {:?} (missing from restic)", snapshot);
                cache.delete_snapshot(&snapshot).unwrap();
            }
        }
        
        let db_snapshots = cache.get_snapshots().unwrap();
        restic_snapshots.into_iter().filter(|s| ! db_snapshots.contains(s)).collect()
    };
    
    // Update snapshots
    if snapshots.len() > 0 {
        eprintln!("Need to fetch {} snapshot(s)", snapshots.len());
        for (snapshot, i) in snapshots.iter().zip(1..) {
            eprintln!("Fetching snapshot {:?} [{}/{}]", &snapshot, i, snapshots.len());
            let (mut files, _) = restic.ls(&snapshot).await;
            let handle = cache.start_snapshot(&snapshot).unwrap();
            while let Some(f) = files.try_next().await.unwrap() {
                handle.insert_file(&f.path, f.size).unwrap()
            }
            handle.finish().unwrap();
        }
    } else {
        eprintln!("Snapshots up to date");
    }
    
    // UI
    stdout().execute(EnterAlternateScreen).unwrap();
    panic::update_hook(|prev, info| {
        stdout().execute(LeaveAlternateScreen).unwrap();
        prev(info);
    });
    enable_raw_mode().unwrap();
    panic::update_hook(|prev, info| {
        disable_raw_mode().unwrap();
        prev(info);
    });
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout())).unwrap();
    terminal.clear().unwrap();
    
    let mut terminal_events = crossterm::event::EventStream::new();
    let mut app = {
        let rect = terminal.size().unwrap();
        App::new(
            (rect.width, rect.height),
            None::<Cow<Utf8Path>>,
            get_files(&cache, None).unwrap(),
        )
    };
    render(&mut terminal, &app).unwrap();
    while let Some(event) = terminal_events.try_next().await.unwrap() {
        if let Some(event) = convert_event(event) {
            match handle_event(&cache, &mut app, event).unwrap() {
                Action::Quit => break,
                Action::Render => { render(&mut terminal, &app).unwrap(); },
                Action::Nothing => {},
            }
        }
    }
    
    disable_raw_mode().unwrap();
    stdout().execute(LeaveAlternateScreen).unwrap();
}
