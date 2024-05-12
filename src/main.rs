#![feature(iter_intersperse)]
#![feature(panic_update_hook)]
#![feature(try_blocks)]
#![feature(option_get_or_insert_default)]

use std::borrow::Cow;
use std::io::stderr;
use std::panic;
use std::sync::Arc;
use std::time::Duration;

use camino::Utf8Path;
use clap::{command, Parser};
use crossterm::event::KeyCode;
use crossterm::ExecutableCommand;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use flexi_logger::{FileSpec, Logger, WriteMode};
use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use ratatui::{CompletedFrame, Terminal};
use ratatui::backend::{Backend, CrosstermBackend};
use ratatui::layout::Size;
use ratatui::widgets::WidgetRef;
use scopeguard::ScopeGuard;

use ui::Action;
use ui::Event;

use crate::cache::Cache;
use crate::restic::Restic;
use crate::ui::App;

mod cache;
mod restic;
mod types;
pub mod ui;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short = 'r', long)]
    repo: String,
    #[arg(long)]
    password_command: Option<String>,
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
        })).unwrap();
    }

    let cli = Cli::parse();
    let restic = Restic::new(
        cli.repo,
        cli.password_command
    );

    let mut cache = { // Get config to determine repo id and open cache
        let pb = new_spinner("Getting restic config");
        let repo_id = restic.config().await.unwrap().id;
        pb.finish();
        Cache::open(repo_id.as_str()).unwrap()
    };
    eprintln!("Using cache file '{}'", cache.filename());

    // Figure out what snapshots we need to fetch
    let missing_snapshots: Vec<Box<str>> = {
        let pb = new_spinner("Fetching repository snapshot list");
        let repo_snapshots = restic.snapshots().await
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect::<Vec<Box<str>>>();
        pb.finish();
        { // Delete snapshots from the DB that were deleted on the repo
            let snapshots_to_delete = cache.get_snapshots()
                .unwrap()
                .into_iter()
                .filter(|snapshot| ! repo_snapshots.contains(&snapshot))
                .collect::<Vec<_>>();
            for snapshot in snapshots_to_delete {
                let pb = new_spinner(
                    format!("Deleting snapshot {}", snapshot_short_id(&snapshot))
                );
                cache.delete_snapshot(&snapshot).unwrap();
                pb.finish();
            }
        }

        let db_snapshots = cache.get_snapshots().unwrap();
        repo_snapshots.into_iter().filter(|s| ! db_snapshots.contains(s)).collect()
    };

    // Fetch missing snapshots
    if missing_snapshots.is_empty() {
        eprintln!("Snapshots up to date");
    }
    for (snapshot, i) in missing_snapshots.iter().zip(1..) {
        let pb = new_spinner(format!(
            "Fetching snapshot {}... [{}/{}]",
            snapshot_short_id(snapshot), i, missing_snapshots.len()
        ));
        let speed = {
            let pb = pb.clone();
            Speed::new(move |v| {
                let mut msg = humansize::format_size_i(v, humansize::BINARY);
                msg.push_str("/s");
                pb.set_message(format!("({msg:>12})"));
            })
        };
        let handle = cache.start_snapshot(&snapshot).unwrap();
        let mut files = restic.ls(&snapshot);
        while let Some((file, bytes_read)) = files.try_next().await.unwrap() {
            speed.inc(bytes_read).await;
            handle.insert_file(&file.path, file.size).unwrap()
        }
        handle.finish().unwrap();
        pb.finish();
    }

    // UI
    stderr().execute(EnterAlternateScreen).unwrap();
    panic::update_hook(|prev, info| {
        stderr().execute(LeaveAlternateScreen).unwrap();
        prev(info);
    });
    enable_raw_mode().unwrap();
    panic::update_hook(|prev, info| {
        disable_raw_mode().unwrap();
        prev(info);
    });
    let mut terminal = Terminal::new(CrosstermBackend::new(stderr())).unwrap();
    terminal.clear().unwrap();

    let mut app = {
        let rect = terminal.size().unwrap();
        App::new(
            rect.as_size(),
            None::<Cow<Utf8Path>>,
            cache.get_max_file_sizes(None::<&str>).unwrap(),
            cache.get_marks().unwrap(),
        )
    };

    let mut output_lines = vec![];

    render(&mut terminal, &app).unwrap();
    let mut terminal_events = crossterm::event::EventStream::new();
    'outer: while let Some(event) = terminal_events.try_next().await.unwrap() {
        let mut o_event = convert_event(event);
        while let Some(event) = o_event {
            o_event = match app.update(event) {
                Action::Nothing =>
                    None,
                Action::Render => {
                    render(&mut terminal, &app).unwrap();
                    None
                }
                Action::Quit =>
                    break 'outer,
                Action::Generate(lines) => {
                    output_lines = lines;
                    break 'outer
                }
                Action::GetEntries(path) => {
                    let children = cache.get_max_file_sizes(path.as_deref()).unwrap();
                    Some(Event::Entries {
                        parent: path,
                        children
                    })
                }
                Action::UpsertMark(path) => {
                    cache.upsert_mark(&path).unwrap();
                    Some(Event::Marks(cache.get_marks().unwrap()))
                }
                Action::DeleteMark(path) => {
                    cache.delete_mark(&path).unwrap();
                    Some(Event::Marks(cache.get_marks().unwrap()))
                }
                Action::DeleteAllMarks => {
                    cache.delete_all_marks().unwrap();
                    Some(Event::Marks(Vec::new()))
                }
            }
        }
    }

    disable_raw_mode().unwrap();
    stderr().execute(LeaveAlternateScreen).unwrap();

    for line in output_lines {
        println!("{line}");
    }
}

fn convert_event(event: crossterm::event::Event) -> Option<Event> {
    use crossterm::event::Event as TermEvent;
    use crossterm::event::KeyEventKind::{Press, Release};
    use ui::Event::*;
    match event {
        TermEvent::Resize(w, h) =>
            Some(Resize(Size::new(w, h))),
        TermEvent::Key(event) if [Press, Release].contains(&event.kind) => {
            match event.code {
                KeyCode::Left => Some(Left),
                KeyCode::Char('h') => Some(Left),

                KeyCode::Right => Some(Right),
                KeyCode::Char(';') => Some(Right),

                KeyCode::Up => Some(Up),
                KeyCode::Char('k') => Some(Up),

                KeyCode::Down => Some(Down),
                KeyCode::Char('j') => Some(Down),

                KeyCode::Char('m') => Some(Mark),
                KeyCode::Char('u') => Some(Unmark),
                KeyCode::Char('c') => Some(UnmarkAll),
                KeyCode::Char('q') => Some(Quit),
                KeyCode::Char('g') => Some(Generate),

                _ => None,
            }
        }
        _ => None,
    }
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

/// Util ///////////////////////////////////////////////////////////////////////

struct Speed {
    count: Arc<tokio::sync::Mutex<usize>>,
    _guard: ScopeGuard<(), Box<dyn FnOnce(())>>,
}

impl Speed {
    pub fn new(mut cb: impl FnMut(f64) + Send + 'static) -> Self {
        let count = Arc::new(tokio::sync::Mutex::new(0));
        let handle = {
            let count = count.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let count = {
                        let count_ref = &mut *count.lock().await;
                        let tmp = *count_ref;
                        *count_ref = 0;
                        tmp
                    };
                    cb(count as f64 / 0.5);
                }
            })
        };
        let _guard = {
            let dropfn: Box<dyn FnOnce(())> = Box::new(move |()| handle.abort());
            scopeguard::guard((), dropfn)
        };
        Speed { count, _guard }
    }

    pub async fn inc(&self, delta: usize) {
        *self.count.lock().await += delta;
    }
}

pub fn new_spinner(prefix: impl Into<Cow<'static, str>>) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{prefix} {msg} {spinner}").unwrap());
    pb.set_prefix(prefix);
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

fn snapshot_short_id(id: &str) -> String {
    id.chars().take(7).collect::<String>()
}
