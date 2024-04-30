#![feature(iter_intersperse)]
#![feature(panic_update_hook)]
#![feature(try_blocks)]
#![feature(option_get_or_insert_default)]

use std::io::stdout;
use std::panic;

use camino::Utf8Path;
use clap::{command, Parser};
use crossterm::event::KeyCode;
use crossterm::ExecutableCommand;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use futures::TryStreamExt;
use ratatui::{CompletedFrame, Terminal};
use ratatui::backend::{Backend, CrosstermBackend};
use ratatui::widgets::WidgetRef;

use widget::app::App;

use crate::cache::Cache;
use crate::restic::Restic;
use crate::types::Snapshot;
use crate::widget::{Action, Event};
use crate::widget::app::FileItem;

mod cache;
mod restic;
mod types;
mod widget;

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
) -> Result<Vec<FileItem>, rusqlite::Error>
{
    Ok(cache.get_max_file_sizes(path)?
        .into_iter()
        .map(|(name, size)| FileItem { name, size })
        .collect())
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
    let get_files = |path: Option<&Utf8Path>| Ok(cache
        .get_max_file_sizes(path)?
        .into_iter()
        .map(|(name, size)| FileItem { name, size })
        .collect());
    app.handle_event(get_files, event)
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = Cli::parse();
    let restic = Restic::new(&cli.repo, cli.password_command.as_ref().map(|s| s.as_str()));
    eprintln!("Getting restic config");
    let repo_id = restic.config().await.0.unwrap().id;
    let mut cache = Cache::open(repo_id.as_str()).unwrap();
    
    eprintln!("Using cache file '{}'", cache.filename());
    
    // Figure out what snapshots we need to update
    let snapshots: Vec<Snapshot> = {
        eprintln!("Fetching restic snapshot widget");
        let restic_snapshots = restic.snapshots().await.0.unwrap();
        
        // Delete snapshots from the DB that were deleted on Restic
        for snapshot in cache.get_snapshots().unwrap() {
            if ! restic_snapshots.contains(&snapshot) {
                eprintln!("Deleting DB Snapshot {:?} (missing from restic)", snapshot.id);
                cache.delete_snapshot(&snapshot.id).unwrap();
            }
        }
        
        let db_snapshots = cache.get_snapshots().unwrap();
        restic_snapshots.into_iter().filter(|s| ! db_snapshots.contains(s)).collect()
    };
    
    // Update snapshots
    if snapshots.len() > 0 {
        eprintln!("Need to fetch {} snapshot(s)", snapshots.len());
        for (snapshot, i) in snapshots.iter().zip(1..) {
            eprintln!("Fetching snapshot {:?} [{}/{}]", &snapshot.id, i, snapshots.len());
            let (mut files, _) = restic.ls(&snapshot.id).await;
            let handle = cache.start_snapshot(&snapshot.id).unwrap();
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
            Some(Utf8Path::new("/")),
            get_files(&cache, None).unwrap(),
        )
    };
    render(&mut terminal, &app).unwrap();
    while let Some(event) = terminal_events.try_next().await.unwrap() {
        let event = match event {
            crossterm::event::Event::Key(k) => match k.code {
                KeyCode::Char('q') => Some(Event::Quit),
                KeyCode::Down => Some(Event::Down),
                KeyCode::Char('j') => Some(Event::Down),
                KeyCode::Up => Some(Event::Up),
                KeyCode::Char('k') => Some(Event::Up),
                KeyCode::Right => Some(Event::Right),
                KeyCode::Char(';') => Some(Event::Right),
                KeyCode::Enter => Some(Event::Right),
                KeyCode::Left => Some(Event::Left),
                KeyCode::Char('h') => Some(Event::Left),
                _ => None,
            }
            crossterm::event::Event::Resize(w, h) => Some(Event::Resize(w, h)),
            _ => None,
        };
        if let Some(event) = event {
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
