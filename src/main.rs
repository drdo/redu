#![feature(iter_intersperse)]
#![feature(panic_update_hook)]
#![feature(try_blocks)]

use std::{cmp, panic};
use std::io::stdout;
use camino::{Utf8Path, Utf8PathBuf};
use clap::{command, Parser};
use crossterm::event::{Event, EventStream, KeyCode};
use crossterm::ExecutableCommand;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use futures::TryStreamExt;
use ratatui::{CompletedFrame, Terminal};
use ratatui::backend::{Backend, CrosstermBackend};
use ratatui::style::Stylize;
use ratatui::widgets::{List, ListItem, Widget};

use crate::cache::Cache;
use crate::restic::Restic;
use crate::state::State;
use crate::types::Snapshot;

mod cache;
mod restic;
mod types;
mod state;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short = 'r', long)]
    repo: String,
    #[arg(long)]
    password_command: Option<String>,
}

fn render<'a>(
    terminal: &'a mut Terminal<impl Backend>,
    state: &'_ State,
) -> std::io::Result<CompletedFrame<'a>>
{
    terminal.draw(|frame| {
        let area = frame.size();
        let buf = frame.buffer_mut();
        let items = state.files()
            .iter()
            .enumerate()
            .skip(state.offset)
            .map(|(index, (name, size))| {
                let item = ListItem::new(
                    format!(
                        "{name} : {}",
                        humansize::format_size(*size, humansize::BINARY),
                    )
                );
                if state.is_selected(index) {
                    item.black().on_white()
                } else {
                    item
                }
            });
        List::new(items).render(area, buf);
    })
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
        eprintln!("Fetching restic snapshot list");
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

    let mut terminal_events = EventStream::new();
    let mut state = State::new(
        Some(Utf8Path::new("/")),
        cache.get_max_file_sizes(Some("/")).unwrap(),
    );

    render(&mut terminal, &state).unwrap();
    while let Some(event) = terminal_events.try_next().await.unwrap() {
        match event {
            Event::Key(k) => match k.code {
                KeyCode::Char('q') => break,
                KeyCode::Down => state.move_selection(1),
                KeyCode::Up => state.move_selection(-1),
                KeyCode::Enter => {
                    if let Some((name, _)) = state.selected_file() {
                        let path = state.path()
                            .map(Utf8PathBuf::from)
                            .unwrap_or_default();
                        let new_path = {
                            let mut new_path = Utf8PathBuf::from(path);
                            new_path.push(name);
                            Some(new_path)
                        };
                        let files = cache
                            .get_max_file_sizes(new_path.as_deref()).unwrap();
                        if ! files.is_empty() {
                            state.set_files(new_path.as_deref(), files)
                        }
                    }
                },
                KeyCode::Backspace => {
                    let parent = state.path().and_then(|p| p
                        .parent()
                        .map(ToOwned::to_owned)
                    );
                    state.set_files(
                        parent.clone(),
                        cache.get_max_file_sizes(parent).unwrap()
                    )
                }
                _ => {},
            }
            Event::Resize(w, h) => state.resize(w, h),
            _ => {}
        }
        render(&mut terminal, &state).unwrap();
    }

    disable_raw_mode().unwrap();
    stdout().execute(LeaveAlternateScreen).unwrap();
}
