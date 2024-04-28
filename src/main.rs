#![feature(iter_intersperse)]
#![feature(try_blocks)]

use clap::{command, Parser};
use crossterm::event::{Event, EventStream, KeyCode};
use futures::TryStreamExt;
use ratatui::CompletedFrame;
use ratatui::widgets::{List, ListItem, Widget};
use tokio::select;
use tokio::sync::mpsc;

use crate::cache::Cache;
use crate::restic::Restic;
use crate::state::{FileData, State};
use crate::tui::Tui;
use crate::types::Snapshot;

mod cache;
mod restic;
mod types;
mod tui;
mod state;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    repo: String,
    #[arg(long)]
    password_command: Option<String>,
}

enum Action {
    Render,
}

fn render<'a>(
    tui: &'a mut Tui,
    state: &'_ State,
) -> std::io::Result<CompletedFrame<'a>>
{
    tui.draw(|frame| {
        let area = frame.size();
        let buf = frame.buffer_mut();
        let items = state.files
            .iter()
            .map(|(name, FileData { snapshot, size })| {
                ListItem::new(format!("{name} : {snapshot} : {size}"))
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
    let (action_tx, mut action_rx) = mpsc::channel(512);
    let mut tui = Tui::new().unwrap();
    let mut terminal_events = EventStream::new();
    let mut state = State::new();

    loop {
        select! {
            action = action_rx.recv() => { match action {
                Some(Action::Render) => { render(&mut tui, &state).unwrap(); },
                None => break,
            }}
            event = terminal_events.try_next() => { match event.unwrap() {
                Some(event) => match event {
                    Event::Key(k) if k.code == KeyCode::Char('q') => {
                        break;
                    }
                    Event::Resize(_, _) => {
                        action_tx.send(Action::Render).await.unwrap();
                    }
                    _ => {}
                },
                None => break,
            }}
        }
    }
}
