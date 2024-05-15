#![feature(panic_update_hook)]

use std::{fs, panic, thread};
use std::borrow::Cow;
use std::io::stderr;
use std::sync::{Arc, mpsc, Mutex};
use std::time::Duration;

use camino::Utf8Path;
use clap::{command, Parser};
use crossterm::event::KeyCode;
use crossterm::ExecutableCommand;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use directories::ProjectDirs;
use flexi_logger::{FileSpec, Logger, LogSpecification, WriteMode};
use indicatif::{ProgressBar, ProgressStyle};
use log::error;
use ratatui::{CompletedFrame, Terminal};
use ratatui::backend::{Backend, CrosstermBackend};
use ratatui::layout::Size;
use ratatui::widgets::WidgetRef;
use dorestic::cache;

use dorestic::cache::Cache;
use dorestic::cache::filetree::FileTree;
use dorestic::restic::Restic;

use crate::ui::Action;
use crate::ui::App;
use crate::ui::Event;

mod ui;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short = 'r', long)]
    repo: String,
    #[arg(long)]
    password_command: Option<String>,
    #[arg(
        short = 'j',
        default_value = None,
        long_help = "How many restic subprocesses to spawn concurrently.\nDefaults to the available number of CPUs",
    )]
    parallelism: Option<usize>,
}

fn main() {
    let dirs = ProjectDirs::from("eu", "drdo", "dorestic")
        .expect("unable to determine project directory");
 
    let _logger = {
        let filespec = {
            let mut directory = dirs.data_local_dir().to_path_buf();
            directory.push(Utf8Path::new("logs"));
            FileSpec::default()
                .directory(directory)
                .suppress_basename()
        };
        eprintln!("Logging to {:#?}", filespec.as_pathbuf(None));
        Logger::with(LogSpecification::trace())
            .log_to_file(filespec)
            .write_mode(WriteMode::BufferAndFlush)
            .format(flexi_logger::with_thread)
            .start()
            .unwrap();
    };

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
        let repo_id = restic.config().unwrap().id;
        pb.finish();

        let cache_file = {
            let mut path = dirs.cache_dir().to_path_buf();
            path.push(format!("{repo_id}.db"));
            path
        };

        fs::create_dir_all(dirs.cache_dir())
            .expect(&format!("unable to create cache directory at {}",
                             dirs.cache_dir().to_string_lossy()));

        eprintln!("Using cache file {cache_file:#?}");
        match Cache::open(&cache_file) {
            Err(e) if cache::is_corruption_error(&e) => {
                eprintln!("### Cache file corruption detected! Deleting and recreating. ###");
                // Try to delete and reopen
                fs::remove_file(&cache_file)
                    .expect("unable to remove corrupted cache file");
                eprintln!("Corrupted cache file deleted");
                Cache::open(&cache_file)
            }
            x => x,
        }.expect("unable to open cache file")
    };

    let parallelism = cli.parallelism.unwrap_or(
        thread::available_parallelism().unwrap().get());
    update_snapshots(&restic, &mut cache, parallelism);
 
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
    'outer: loop {
        let mut o_event = convert_event(crossterm::event::read().unwrap());
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

fn update_snapshots(
    restic: &Restic,
    cache: &mut Cache,
    parallelism: usize,
) {
    // Figure out what snapshots we need to fetch
    let missing_snapshots: Vec<Box<str>> = {
        let pb = new_spinner("Fetching repository snapshot list");
        let repo_snapshots = restic.snapshots()
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
    let total_missing_snapshots = match missing_snapshots.len() {
        0 => { eprintln!("Snapshots up to date"); return; },
        n => n,
    };
    eprintln!("Fetching snaphots");
    thread::scope(|scope| {
        let snapshot_queue = Queue::new(missing_snapshots);
        let (filetree_sender, filetree_receiver) =
            mpsc::sync_channel::<(Box<str>, FileTree)>(2);

        let pb = ProgressBar::new(total_missing_snapshots as u64)
            .with_style(ProgressStyle::with_template(
                "{elapsed_precise} {wide_bar} [{pos}/{len}] {msg}"
            ).unwrap());
        let speed = {
            let pb = pb.clone();
            Speed::new(move |v| {
                let mut msg = humansize::format_size_i(v, humansize::BINARY);
                msg.push_str("/s");
                pb.set_message(format!("({msg:>12})"));
            })
        };

        // DB Thread
        scope.spawn({
            let pb = pb.clone();
            let mut speed = speed.clone();
            move || {
                while let Ok((snapshot, filetree)) = filetree_receiver.recv() {
                    cache.save_snapshot(&snapshot, &filetree).unwrap();
                    pb.inc(1);
                }
                speed.stop();
                pb.finish_with_message("Done");
            }
        });

        // Fetching threads
        for _ in 0..parallelism {
            let snapshot_queue = snapshot_queue.clone();
            let filetree_sender = filetree_sender.clone();
            let speed = speed.clone();
            scope.spawn(move || {
                while let Some(snapshot) = snapshot_queue.pop() {
                    let mut filetree = FileTree::new();
                    let files = restic.ls(&snapshot).unwrap();
                    for r in files {
                        let (file, bytes_read) = r.unwrap();
                        speed.inc(bytes_read);
                        filetree.insert(&file.path, file.size)
                            .expect("repeated entry in restic snapshot ls");
                    }
                    filetree_sender.send((snapshot, filetree)).unwrap();
                }
            });
        }
    });
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

/// Track the speed of something in units/sec
/// Periodically calls a callback with the current speed
/// Users are expected to call the `inc` method to add units
#[derive(Clone)]
struct Speed {
    state: Arc<Mutex<SpeedState>>,
}

struct SpeedState {
    should_quit: bool,
    count: usize,
    previous: f64,
}

impl Speed {
    pub fn new(mut cb: impl FnMut(f64) + Send + 'static) -> Self {
        const WINDOW_MILLIS: u64 = 300;
        const ALPHA: f64 = 0.3;

        let state = Arc::new(Mutex::new(SpeedState {
            should_quit: false,
            count: 0,
            previous: 0.0,
        }));
        thread::spawn({
            let state = Arc::downgrade(&state);
            move || {
                while let Some(state) = state.upgrade() {
                    let value = {
                        let SpeedState { should_quit, count, previous } =
                            &mut *state.lock().unwrap();
                        if *should_quit { break; }
                        let current = *count as f64 / (WINDOW_MILLIS as f64 / 1000.0);
                        *count = 0;
                        let value = (ALPHA * current) + ((1.0-ALPHA) * *previous);
                        *previous = current;
                        value
                    };
                    cb(value);
                    thread::sleep(Duration::from_millis(WINDOW_MILLIS));
                }
            }
        });
        Speed { state }
    }

    pub fn inc(&self, delta: usize) {
        self.state.lock().unwrap().count += delta;
    }
    
    pub fn stop(&mut self) {
        self.state.lock().unwrap().should_quit = true;
    }
}

pub fn new_spinner(prefix: impl Into<Cow<'static, str>>) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("{prefix} {elapsed} {msg} {spinner}").unwrap());
    pb.set_prefix(prefix);
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

fn snapshot_short_id(id: &str) -> String {
    id.chars().take(7).collect::<String>()
}

#[derive(Clone)]
struct Queue<T>(Arc<Mutex<Vec<T>>>);

impl<T> Queue<T> {
    fn new(data: Vec<T>) -> Self {
        Queue(Arc::new(Mutex::new(data)))
    }

    fn pop(&self) -> Option<T> {
        self.0.lock().unwrap().pop()
    }
}
