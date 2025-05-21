use std::{
    fs,
    io::{self, stderr},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvTimeoutError},
        Arc, Mutex,
    },
    thread::{self, ScopedJoinHandle},
    time::{Duration, Instant},
};

use anyhow::Context;
use args::Args;
use camino::{Utf8Path, Utf8PathBuf};
use chrono::Local;
use crossterm::{
    event::{KeyCode, KeyModifiers},
    terminal::{
        disable_raw_mode, enable_raw_mode, EnterAlternateScreen,
        LeaveAlternateScreen,
    },
    ExecutableCommand,
};
use directories::ProjectDirs;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error, info, trace, LevelFilter};
use rand::{seq::SliceRandom, thread_rng};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::Size,
    style::Stylize,
    widgets::WidgetRef,
    CompletedFrame, Terminal,
};
use redu::{
    cache::{self, filetree::SizeTree, Cache, Migrator},
    restic::{self, escape_for_exclude, Restic, Snapshot},
};
use scopeguard::defer;
use simplelog::{ThreadLogMode, WriteLogger};
use thiserror::Error;
use util::snapshot_short_id;

use crate::ui::{Action, App, Event};

mod args;
mod ui;
mod util;

// Print the message to stderr and log at INFO level
macro_rules! info_eprintln {
    ($($arg:expr),+) => {{
        let msg = format!($($arg),+);
        eprintln!("{msg}");
        info!("{msg}");
    }};
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let restic = Restic::new(args.repository, args.password, args.no_cache);

    let dirs = ProjectDirs::from("eu", "drdo", "redu")
        .expect("unable to determine project directory");

    // Initialize the logger
    {
        fn generate_filename() -> String {
            format!("{}.log", Local::now().format("%Y-%m-%dT%H-%M-%S%.f%z"))
        }

        let mut path = dirs.data_local_dir().to_path_buf();
        path.push(Utf8Path::new("logs"));
        fs::create_dir_all(&path)?;
        path.push(generate_filename());
        let file = loop {
            // Spin until we hit a timestamp that isn't taken yet.
            // With the level of precision that we are using this should virtually
            // never run more than once.
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                    path.set_file_name(generate_filename())
                }
                x => break x,
            }
        }?;

        eprintln!("Logging to {:#?}", path);

        let config = simplelog::ConfigBuilder::new()
            .set_target_level(LevelFilter::Error)
            .set_thread_mode(ThreadLogMode::Names)
            .build();
        WriteLogger::init(args.log_level, config, file)?;
    }

    unsafe {
        rusqlite::trace::config_log(Some(|code, msg| {
            error!(target: "sqlite", "({code}) {msg}");
        }))?;
    }

    let mut cache = {
        // Get config to determine repo id and open cache
        let pb = new_pb(" {spinner} Getting restic config");
        let repo_id = restic.config()?.id;
        pb.finish();

        let cache_file = {
            let mut path = dirs.cache_dir().to_path_buf();
            path.push(format!("{repo_id}.db"));
            path
        };

        let err_msg = format!(
            "unable to create cache directory at {}",
            dirs.cache_dir().to_string_lossy(),
        );
        fs::create_dir_all(dirs.cache_dir()).expect(&err_msg);

        info_eprintln!("Using cache file {cache_file:#?}");
        let migrator =
            Migrator::open(&cache_file).context("unable to open cache file")?;
        if let Some((old, new)) = migrator.need_to_migrate() {
            info_eprintln!(
                "Need to upgrade cache version from {old:?} to {new:?}"
            );
            let mut template =
                String::from(" {spinner} Upgrading cache version");
            if migrator.resync_necessary() {
                template.push_str(" (a resync will be necessary)");
            }
            let pb = new_pb(&template);
            let cache = migrator.migrate().context("cache migration failed")?;
            pb.finish();
            cache
        } else {
            migrator.migrate().context("there is a problem with the cache")?
        }
    };

    sync_snapshots(&restic, &mut cache, args.parallelism)?;

    let paths = ui(cache)?;
    for line in paths {
        println!("{}", escape_for_exclude(line.as_str()));
    }

    Ok(())
}

fn sync_snapshots(
    restic: &Restic,
    cache: &mut Cache,
    fetching_thread_count: usize,
) -> anyhow::Result<()> {
    let pb = new_pb(" {spinner} Fetching repository snapshot list");
    let repo_snapshots = restic.snapshots()?;
    pb.finish();

    let cache_snapshots = cache.get_snapshots()?;

    // Delete snapshots from the DB that were deleted on the repo
    let snapshots_to_delete: Vec<&Snapshot> = cache_snapshots
        .iter()
        .filter(|cache_snapshot| {
            !repo_snapshots
                .iter()
                .any(|repo_snapshot| cache_snapshot.id == repo_snapshot.id)
        })
        .collect();
    if !snapshots_to_delete.is_empty() {
        info_eprintln!(
            "Need to delete {} snapshot(s)",
            snapshots_to_delete.len()
        );
        let pb = new_pb(" {spinner} {wide_bar} [{pos}/{len}]");
        pb.set_length(snapshots_to_delete.len() as u64);
        for snapshot in snapshots_to_delete {
            cache.delete_snapshot(&snapshot.id)?;
            pb.inc(1);
        }
        pb.finish();
    }

    let mut missing_snapshots: Vec<Snapshot> = repo_snapshots
        .into_iter()
        .filter(|repo_snapshot| {
            !cache_snapshots
                .iter()
                .any(|cache_snapshot| cache_snapshot.id == repo_snapshot.id)
        })
        .collect();
    missing_snapshots.shuffle(&mut thread_rng());
    let total_missing_snapshots = match missing_snapshots.len() {
        0 => {
            info_eprintln!("Snapshots up to date");
            return Ok(());
        }
        n => n,
    };
    let missing_queue = FixedSizeQueue::new(missing_snapshots);

    // Create progress indicators
    let mpb = MultiProgress::new();
    let pb =
        mpb_insert_end(&mpb, " {spinner} {prefix} {wide_bar} [{pos}/{len}] ");
    pb.set_prefix("Fetching snapshots");
    pb.set_length(total_missing_snapshots as u64);

    const SHOULD_QUIT_POLL_PERIOD: Duration = Duration::from_millis(500);

    thread::scope(|scope| {
        macro_rules! spawn {
            ($name_fmt:literal, $scope:expr, $thunk:expr) => {
                thread::Builder::new()
                    .name(format!($name_fmt))
                    .spawn_scoped($scope, $thunk)?
            };
        }
        let mut handles: Vec<ScopedJoinHandle<anyhow::Result<()>>> = Vec::new();

        // The threads periodically poll this to see if they should
        // prematurely terminate (when other threads get unrecoverable errors).
        let should_quit: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        // Channel to funnel snapshots from the fetching threads to the db thread
        let (snapshot_sender, snapshot_receiver) =
            mpsc::sync_channel::<(Snapshot, SizeTree)>(fetching_thread_count);

        // Start fetching threads
        for i in 0..fetching_thread_count {
            let missing_queue = missing_queue.clone();
            let snapshot_sender = snapshot_sender.clone();
            let mpb = mpb.clone();
            let should_quit = should_quit.clone();
            handles.push(spawn!("fetching-{i}", &scope, move || {
                fetching_thread_body(
                    restic,
                    missing_queue,
                    mpb,
                    snapshot_sender,
                    should_quit.clone(),
                )
                .inspect_err(|_| should_quit.store(true, Ordering::SeqCst))
                .map_err(anyhow::Error::from)
            }));
        }
        // Drop the leftover channel so that the db thread
        // can properly terminate when all snapshot senders are closed
        drop(snapshot_sender);

        // Start DB thread
        handles.push({
            let should_quit = should_quit.clone();
            spawn!("db", &scope, move || {
                db_thread_body(
                    cache,
                    mpb,
                    pb,
                    snapshot_receiver,
                    should_quit.clone(),
                    SHOULD_QUIT_POLL_PERIOD,
                )
                .inspect_err(|_| should_quit.store(true, Ordering::SeqCst))
                .map_err(anyhow::Error::from)
            })
        });

        for handle in handles {
            handle.join().unwrap()?
        }
        Ok(())
    })
}

#[derive(Debug, Error)]
#[error("error in fetching thread")]
enum FetchingThreadError {
    ResticLaunch(#[from] restic::LaunchError),
    Restic(#[from] restic::Error),
    Cache(#[from] rusqlite::Error),
}

fn fetching_thread_body(
    restic: &Restic,
    missing_queue: FixedSizeQueue<Snapshot>,
    mpb: MultiProgress,
    snapshot_sender: mpsc::SyncSender<(Snapshot, SizeTree)>,
    should_quit: Arc<AtomicBool>,
) -> Result<(), FetchingThreadError> {
    defer! { trace!("terminated") }
    trace!("started");
    while let Some(snapshot) = missing_queue.pop() {
        let short_id = snapshot_short_id(&snapshot.id);
        let pb =
            mpb_insert_end(&mpb, "   {spinner} fetching {prefix}: starting up")
                .with_prefix(short_id.clone());
        let mut sizetree = SizeTree::new();
        let files = restic.ls(&snapshot.id)?;
        trace!("started fetching snapshot ({short_id})");
        let start = Instant::now();
        for r in files {
            if should_quit.load(Ordering::SeqCst) {
                return Ok(());
            }
            let file = r?;
            sizetree
                .insert(file.path.components(), file.size)
                .expect("repeated entry in restic snapshot ls");
            if pb.position() == 0 {
                pb.set_style(new_style(
                    "   {spinner} fetching {prefix}: {pos} file(s)",
                ));
            }
            pb.inc(1);
        }
        pb.finish_and_clear();
        mpb.remove(&pb);
        info!(
            "snapshot fetched in {}s ({short_id})",
            start.elapsed().as_secs_f64()
        );
        if should_quit.load(Ordering::SeqCst) {
            return Ok(());
        }
        let start = Instant::now();
        snapshot_sender.send((snapshot.clone(), sizetree)).unwrap();
        debug!(
            "waited {}s to send snapshot ({short_id})",
            start.elapsed().as_secs_f64()
        );
    }
    Ok(())
}

#[derive(Debug, Error)]
#[error("error in db thread")]
enum DBThreadError {
    CacheError(#[from] rusqlite::Error),
}

fn db_thread_body(
    cache: &mut Cache,
    mpb: MultiProgress,
    main_pb: ProgressBar,
    snapshot_receiver: mpsc::Receiver<(Snapshot, SizeTree)>,
    should_quit: Arc<AtomicBool>,
    should_quit_poll_period: Duration,
) -> Result<(), DBThreadError> {
    defer! { trace!("terminated") }
    trace!("started");
    loop {
        trace!("waiting for snapshot");
        if should_quit.load(Ordering::SeqCst) {
            return Ok(());
        }
        let start = Instant::now();
        // We wait with timeout to poll the should_quit periodically
        match snapshot_receiver.recv_timeout(should_quit_poll_period) {
            Ok((snapshot, sizetree)) => {
                debug!(
                    "waited {}s to get snapshot",
                    start.elapsed().as_secs_f64()
                );
                trace!("got snapshot, saving");
                if should_quit.load(Ordering::SeqCst) {
                    return Ok(());
                }
                let short_id = snapshot_short_id(&snapshot.id);
                let pb = mpb_insert_after(
                    &mpb,
                    &main_pb,
                    "   {spinner} saving {prefix}",
                )
                .with_prefix(short_id.clone());
                let start = Instant::now();
                let file_count = cache.save_snapshot(&snapshot, sizetree)?;
                pb.finish_and_clear();
                mpb.remove(&pb);
                main_pb.inc(1);
                info!(
                    "waited {}s to save snapshot ({} files)",
                    start.elapsed().as_secs_f64(),
                    file_count
                );
                trace!("snapshot saved");
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                trace!("loop done");
                break Ok(());
            }
        }
    }
}

fn convert_event(event: crossterm::event::Event) -> Option<Event> {
    use crossterm::event::{Event as TermEvent, KeyEventKind};
    use ui::Event::*;

    const KEYBINDINGS: &[((KeyModifiers, KeyCode), Event)] = &[
        ((KeyModifiers::empty(), KeyCode::Left), Left),
        ((KeyModifiers::empty(), KeyCode::Char('h')), Left),
        ((KeyModifiers::empty(), KeyCode::Right), Right),
        ((KeyModifiers::empty(), KeyCode::Char('l')), Right),
        ((KeyModifiers::empty(), KeyCode::Up), Up),
        ((KeyModifiers::empty(), KeyCode::Char('k')), Up),
        ((KeyModifiers::empty(), KeyCode::Down), Down),
        ((KeyModifiers::empty(), KeyCode::Char('j')), Down),
        ((KeyModifiers::empty(), KeyCode::PageUp), PageUp),
        ((KeyModifiers::CONTROL, KeyCode::Char('b')), PageUp),
        ((KeyModifiers::empty(), KeyCode::PageDown), PageDown),
        ((KeyModifiers::CONTROL, KeyCode::Char('f')), PageDown),
        ((KeyModifiers::empty(), KeyCode::Enter), Enter),
        ((KeyModifiers::empty(), KeyCode::Esc), Exit),
        ((KeyModifiers::empty(), KeyCode::Char('m')), Mark),
        ((KeyModifiers::empty(), KeyCode::Char('u')), Unmark),
        ((KeyModifiers::empty(), KeyCode::Char('c')), UnmarkAll),
        ((KeyModifiers::empty(), KeyCode::Char('q')), Quit),
        ((KeyModifiers::empty(), KeyCode::Char('g')), Generate),
    ];
    match event {
        TermEvent::Resize(w, h) => Some(Resize(Size::new(w, h))),
        TermEvent::Key(event) if event.kind == KeyEventKind::Press => {
            KEYBINDINGS.iter().find_map(|((mods, code), ui_event)| {
                if event.modifiers == *mods && event.code == *code {
                    Some(ui_event.clone())
                } else {
                    None
                }
            })
        }
        _ => None,
    }
}

fn ui(mut cache: Cache) -> anyhow::Result<Vec<Utf8PathBuf>> {
    let entries = cache.get_entries(None)?;
    if entries.is_empty() {
        info_eprintln!("The repository is empty!");
        return Ok(vec![]);
    }

    stderr().execute(EnterAlternateScreen)?;
    defer! {
        stderr().execute(LeaveAlternateScreen).unwrap();
    }
    enable_raw_mode()?;
    defer! {
        disable_raw_mode().unwrap();
    }
    let mut terminal = Terminal::new(CrosstermBackend::new(stderr()))?;
    terminal.clear()?;

    let mut app = {
        let rect = terminal.size()?;
        App::new(
            rect,
            None,
            Utf8PathBuf::new(),
            entries,
            cache.get_marks()?,
            vec![
                "Enter".bold(),
                ":Details  ".into(),
                "m".bold(),
                ":Mark  ".into(),
                "u".bold(),
                ":Unmark  ".into(),
                "c".bold(),
                ":ClearAllMarks  ".into(),
                "g".bold(),
                ":Generate  ".into(),
                "q".bold(),
                ":Quit".into(),
            ],
        )
    };

    render(&mut terminal, &app)?;
    loop {
        let mut o_event = convert_event(crossterm::event::read()?);
        while let Some(event) = o_event {
            o_event = match app.update(event) {
                Action::Nothing => None,
                Action::Render => {
                    render(&mut terminal, &app)?;
                    None
                }
                Action::Quit => return Ok(vec![]),
                Action::Generate(paths) => return Ok(paths),
                Action::GetParentEntries(path_id) => {
                    let parent_id = cache.get_parent_id(path_id)?
                        .expect("The UI requested a GetParentEntries with a path_id that does not exist");
                    let entries = cache.get_entries(parent_id)?;
                    Some(Event::Entries { path_id: parent_id, entries })
                }
                Action::GetEntries(path_id) => {
                    let entries = cache.get_entries(path_id)?;
                    Some(Event::Entries { path_id, entries })
                }
                Action::GetEntryDetails(path_id) =>
                    Some(Event::EntryDetails(cache.get_entry_details(path_id)?
                        .expect("The UI requested a GetEntryDetails with a path_id that does not exist"))),
                Action::UpsertMark(path) => {
                    cache.upsert_mark(&path)?;
                    Some(Event::Marks(cache.get_marks()?))
                }
                Action::DeleteMark(loc) => {
                    cache.delete_mark(&loc).unwrap();
                    Some(Event::Marks(cache.get_marks()?))
                }
                Action::DeleteAllMarks => {
                    cache.delete_all_marks()?;
                    Some(Event::Marks(Vec::new()))
                }
            }
        }
    }
}

fn render<'a>(
    terminal: &'a mut Terminal<impl Backend>,
    app: &App,
) -> io::Result<CompletedFrame<'a>> {
    terminal.draw(|frame| {
        let area = frame.area();
        let buf = frame.buffer_mut();
        app.render_ref(area, buf)
    })
}

/// Util ///////////////////////////////////////////////////////////////////////
fn new_style(template: &str) -> ProgressStyle {
    let frames = &[
        "(●    )",
        "( ●   )",
        "(  ●  )",
        "(   ● )",
        "(   ● )",
        "(  ●  )",
        "( ●   )",
        "(●    )",
        "(●    )",
    ];
    ProgressStyle::with_template(template).unwrap().tick_strings(frames)
}

const PB_TICK_INTERVAL: Duration = Duration::from_millis(100);

fn new_pb(template: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner().with_style(new_style(template));
    pb.enable_steady_tick(PB_TICK_INTERVAL);
    pb
}

// This is necessary to avoid some weird redraws that happen
// when enabling the tick thread before adding to the MultiProgress.
fn mpb_insert_after(
    mpb: &MultiProgress,
    other_pb: &ProgressBar,
    template: &str,
) -> ProgressBar {
    let pb = ProgressBar::new_spinner().with_style(new_style(template));
    let pb = mpb.insert_after(other_pb, pb);
    pb.enable_steady_tick(PB_TICK_INTERVAL);
    pb
}

fn mpb_insert_end(mpb: &MultiProgress, template: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner().with_style(new_style(template));
    let pb = mpb.add(pb);
    pb.enable_steady_tick(PB_TICK_INTERVAL);
    pb
}

#[derive(Clone)]
struct FixedSizeQueue<T>(Arc<Mutex<Vec<T>>>);

impl<T> FixedSizeQueue<T> {
    fn new(data: Vec<T>) -> Self {
        FixedSizeQueue(Arc::new(Mutex::new(data)))
    }

    fn pop(&self) -> Option<T> {
        self.0.lock().unwrap().pop()
    }
}
