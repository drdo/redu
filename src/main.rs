#![feature(panic_update_hook)]

use std::borrow::Cow;
use std::collections::HashSet;
use std::io::stderr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::ScopedJoinHandle;
use std::time::{Duration, Instant};
use std::{fs, panic, thread};

use camino::Utf8Path;
use clap::{command, Parser};
use crossterm::event::{KeyCode, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen,
    LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use directories::ProjectDirs;
use flexi_logger::{FileSpec, LogSpecification, Logger, WriteMode};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{error, info, trace};
use ratatui::backend::{Backend, CrosstermBackend};
use ratatui::layout::Size;
use ratatui::style::Stylize;
use ratatui::widgets::WidgetRef;
use ratatui::{CompletedFrame, Terminal};
use redu::cache::filetree::FileTree;
use redu::cache::{Cache, SnapshotGroup};
use redu::restic::Restic;
use redu::{cache, restic};
use scopeguard::defer;
use thiserror::Error;

use crate::ui::{Action, App, Event};

mod ui;

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
/// m: Mark
/// u: Unmark
/// c: Clear all marks
/// g: Generate
/// q: Quit
#[derive(Parser)]
#[command(version, long_about, verbatim_doc_comment)]
struct Cli {
    #[arg(short = 'r', long)]
    repo: Option<String>,
    #[arg(long)]
    password_command: Option<String>,
    #[arg(
        short = 'j',
        long,
        default_value_t = 4,
        long_help = "
            How many restic subprocesses to spawn concurrently.
            
            If you get ssh-related errors or too much memory use
            try lowering this."
    )]
    fetching_thread_count: usize,
    #[arg(
        long,
        default_value_t = 8,
        long_help = "
            How big to make each group of snapshots.

            A group is saved by merging the info of the snapshots in the group.
            This is primarily to save disk space but it also speeds up
            writing to the cache when doing a sync.

            The disadvantage is that if we need to delete a snapshot because
            it was removed from the repo then we must delete the entire group
            that that snapshot belongs to."
    )]
    group_size: usize,
    #[arg(
        short = 'v',
        action = clap::ArgAction::Count,
        long_help =
            "Log verbosity level. You can pass it multiple times (maxes out at two)."
    )]
    verbose: u8,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let restic = Restic::new(cli.repo, cli.password_command);

    let dirs = ProjectDirs::from("eu", "drdo", "redu")
        .expect("unable to determine project directory");

    let _logger = {
        let mut directory = dirs.data_local_dir().to_path_buf();
        directory.push(Utf8Path::new("logs"));

        eprintln!("Logging to {:#?}", directory);

        let filespec =
            { FileSpec::default().directory(directory).suppress_basename() };

        let spec = match cli.verbose {
            0 => LogSpecification::info(),
            1 => LogSpecification::debug(),
            _ => LogSpecification::trace(),
        };
        Logger::with(spec)
            .log_to_file(filespec)
            .write_mode(WriteMode::BufferAndFlush)
            .format(flexi_logger::with_thread)
            .start()?
    };

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

        fs::create_dir_all(dirs.cache_dir()).expect(&format!(
            "unable to create cache directory at {}",
            dirs.cache_dir().to_string_lossy()
        ));

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

    sync_snapshots(
        &restic,
        &mut cache,
        cli.fetching_thread_count,
        cli.group_size,
    )?;

    // UI
    stderr().execute(EnterAlternateScreen)?;
    panic::update_hook(|prev, info| {
        stderr().execute(LeaveAlternateScreen).unwrap();
        prev(info);
    });
    enable_raw_mode()?;
    panic::update_hook(|prev, info| {
        disable_raw_mode().unwrap();
        prev(info);
    });
    let mut terminal = Terminal::new(CrosstermBackend::new(stderr()))?;
    terminal.clear()?;

    let mut app = {
        let rect = terminal.size()?;
        App::new(
            rect.as_size(),
            None::<Cow<Utf8Path>>,
            cache.get_max_file_sizes(None::<&str>)?,
            cache.get_marks().unwrap(),
            vec![
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

    let mut output_lines = vec![];

    render(&mut terminal, &app)?;
    'outer: loop {
        let mut o_event = convert_event(crossterm::event::read()?);
        while let Some(event) = o_event {
            o_event = match app.update(event) {
                Action::Nothing => None,
                Action::Render => {
                    render(&mut terminal, &app)?;
                    None
                }
                Action::Quit => break 'outer,
                Action::Generate(lines) => {
                    output_lines = lines;
                    break 'outer;
                }
                Action::GetEntries(path) => {
                    let children = cache.get_max_file_sizes(path.as_deref())?;
                    Some(Event::Entries { parent: path, children })
                }
                Action::UpsertMark(path) => {
                    cache.upsert_mark(&path)?;
                    Some(Event::Marks(cache.get_marks()?))
                }
                Action::DeleteMark(path) => {
                    cache.delete_mark(&path).unwrap();
                    Some(Event::Marks(cache.get_marks()?))
                }
                Action::DeleteAllMarks => {
                    cache.delete_all_marks()?;
                    Some(Event::Marks(Vec::new()))
                }
            }
        }
    }

    disable_raw_mode()?;
    stderr().execute(LeaveAlternateScreen)?;

    for line in output_lines {
        println!("{line}");
    }
    Ok(())
}

fn sync_snapshots(
    restic: &Restic,
    cache: &mut Cache,
    fetching_thread_count: usize,
    group_size: usize,
) -> anyhow::Result<()> {
    // Figure out what snapshots we need to fetch
    let missing_snapshots: Vec<Box<str>> = {
        // Fetch snapshot list
        let pb = new_pb(" {spinner} Fetching repository snapshot list");
        let repo_snapshots = restic
            .snapshots()?
            .into_iter()
            .map(|s| s.id)
            .collect::<Vec<Box<str>>>();
        pb.finish();

        // Delete snapshots from the DB that were deleted on the repo
        let groups_to_delete = cache
            .get_snapshots()?
            .into_iter()
            .filter(|snapshot| !repo_snapshots.contains(&snapshot))
            .map(|snapshot_id| cache.get_snapshot_group(snapshot_id))
            .collect::<Result<HashSet<u64>, rusqlite::Error>>()?;
        if groups_to_delete.len() > 0 {
            eprintln!("Need to delete {} group(s)", groups_to_delete.len());
            let pb = new_pb(" {spinner} {wide_bar} [{pos}/{len}]");
            pb.set_length(groups_to_delete.len() as u64);
            for group in groups_to_delete {
                cache.delete_group(group)?;
                pb.inc(1);
            }
            pb.finish();
        }

        let db_snapshots = cache.get_snapshots()?;
        repo_snapshots
            .into_iter()
            .filter(|s| !db_snapshots.contains(s))
            .collect()
    };

    let total_missing_snapshots = match missing_snapshots.len() {
        0 => {
            eprintln!("Snapshots up to date");
            return Ok(());
        }
        n => n,
    };

    let missing_queue = Queue::new(missing_snapshots);

    // Create progress indicators
    let mpb = MultiProgress::new();
    let pb = mpb_add(&mpb, " {spinner} {prefix} {wide_bar} [{pos}/{len}] ");
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

        // Channel to funnel snapshots from the fetching threads to the grouping thread
        let (snapshot_sender, snapshot_receiver) =
            mpsc::sync_channel::<(Box<str>, FileTree)>(2);

        // Start fetching threads
        for i in 0..fetching_thread_count {
            let missing_queue = missing_queue.clone();
            let snapshot_sender = snapshot_sender.clone();
            let mpb = &mpb;
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
        // Drop the leftover channel so that the grouping thread
        // can properly terminate when all snapshot senders are closed
        drop(snapshot_sender);

        // Channel to funnel groups from the grouping thread to the db thread
        let (group_sender, group_receiver) =
            mpsc::sync_channel::<SnapshotGroup>(1);

        // Start grouping thread
        handles.push({
            let should_quit = should_quit.clone();
            spawn!("grouping", &scope, move || {
                grouping_thread_body(
                    group_size,
                    snapshot_receiver,
                    group_sender,
                    pb,
                    should_quit.clone(),
                    SHOULD_QUIT_POLL_PERIOD,
                )
                .inspect_err(|_| should_quit.store(true, Ordering::SeqCst))
                .map_err(anyhow::Error::from)
            })
        });

        // Start DB thread
        handles.push({
            let should_quit = should_quit.clone();
            spawn!("db", &scope, move || {
                db_thread_body(
                    cache,
                    group_receiver,
                    should_quit.clone(),
                    SHOULD_QUIT_POLL_PERIOD,
                )
                .inspect_err(|_| should_quit.store(true, Ordering::SeqCst))
                .map_err(anyhow::Error::from)
            })
        });

        // Drop the senders that weren't moved into threads so that
        // the receivers can detect when everyone is done

        for handle in handles {
            handle.join().unwrap()?
        }
        Ok(())
    })
}

#[derive(Debug, Error)]
#[error("error in fetching thread")]
enum FetchingThreadError {
    ResticLaunchError(#[from] restic::LaunchError),
    ResticError(#[from] restic::Error),
    CacheError(#[from] rusqlite::Error),
}

fn fetching_thread_body(
    restic: &Restic,
    missing_queue: Queue<Box<str>>,
    mpb: &MultiProgress,
    snapshot_sender: mpsc::SyncSender<(Box<str>, FileTree)>,
    should_quit: Arc<AtomicBool>,
) -> Result<(), FetchingThreadError> {
    defer! { trace!("terminated") }
    trace!("started");
    while let Some(snapshot) = missing_queue.pop() {
        let short_id = snapshot_short_id(&snapshot);
        let pb = mpb_add(mpb, "   {spinner} fetching {prefix}: starting up")
            .with_prefix(short_id.clone());
        let mut filetree = FileTree::new();
        let files = restic.ls(&snapshot)?;
        trace!("started fetching snapshot ({short_id})");
        let start = Instant::now();
        for r in files {
            if should_quit.load(Ordering::SeqCst) {
                return Ok(());
            }
            let (file, _bytes_read) = r?;
            filetree
                .insert(&file.path, file.size)
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
        trace!("got snapshot, sending ({short_id})");
        if should_quit.load(Ordering::SeqCst) {
            return Ok(());
        }
        let start = Instant::now();
        snapshot_sender.send((snapshot.clone(), filetree)).unwrap();
        info!(
            "waited {}s to send snapshot ({short_id})",
            start.elapsed().as_secs_f64()
        );
        trace!("snapshot sent ({short_id})");
    }
    Ok(())
}

#[derive(Debug, Error)]
#[error("error in grouping thread")]
enum GroupingThreadError {
    CacheError(#[from] rusqlite::Error),
}

fn grouping_thread_body(
    group_size: usize,
    snapshot_receiver: mpsc::Receiver<(Box<str>, FileTree)>,
    group_sender: mpsc::SyncSender<SnapshotGroup>,
    pb: ProgressBar,
    should_quit: Arc<AtomicBool>,
    should_quit_poll_period: Duration,
) -> Result<(), GroupingThreadError> {
    defer! { trace!("terminated") }
    trace!("started");
    let mut group = SnapshotGroup::new();
    loop {
        trace!("waiting for snapshot");
        if should_quit.load(Ordering::SeqCst) {
            return Ok(());
        }
        let start = Instant::now();
        // We wait with timeout to poll the should_quit periodically
        match snapshot_receiver.recv_timeout(should_quit_poll_period) {
            Ok((snapshot, filetree)) => {
                let short_id = snapshot_short_id(&snapshot);
                info!(
                    "waited {}s to get snapshot ({short_id})",
                    start.elapsed().as_secs_f64()
                );
                trace!("got snapshot ({short_id})");
                if should_quit.load(Ordering::SeqCst) {
                    return Ok(());
                }
                group.add_snapshot(snapshot.clone(), filetree);
                pb.inc(1);
                trace!("added snapshot ({short_id})");
                if group.count() == group_size {
                    trace!("group is full, sending");
                    if should_quit.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    let start = Instant::now();
                    group_sender.send(group).unwrap();
                    info!(
                        "waited {}s to send group",
                        start.elapsed().as_secs_f64()
                    );
                    trace!("sent group");
                    group = SnapshotGroup::new();
                }
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                trace!("loop done");
                break;
            }
        }
    }
    if group.count() > 0 {
        trace!("sending leftover group");
        if should_quit.load(Ordering::SeqCst) {
            return Ok(());
        }
        let start = Instant::now();
        group_sender.send(group).unwrap();
        info!(
            "waited {}s to send leftover group",
            start.elapsed().as_secs_f64()
        );
        trace!("sent leftover group");
    }
    pb.finish_with_message("Done");
    Ok(())
}

#[derive(Debug, Error)]
#[error("error in db thread")]
enum DBThreadError {
    CacheError(#[from] rusqlite::Error),
}

fn db_thread_body(
    cache: &mut Cache,
    group_receiver: mpsc::Receiver<SnapshotGroup>,
    should_quit: Arc<AtomicBool>,
    should_quit_poll_period: Duration,
) -> Result<(), DBThreadError> {
    defer! { trace!("terminated") }
    trace!("started");
    loop {
        trace!("waiting for group");
        if should_quit.load(Ordering::SeqCst) {
            return Ok(());
        }
        let start = Instant::now();
        // We wait with timeout to poll the should_quit periodically
        match group_receiver.recv_timeout(should_quit_poll_period) {
            Ok(group) => {
                info!("waited {}s to get group", start.elapsed().as_secs_f64());
                trace!("got group, saving");
                if should_quit.load(Ordering::SeqCst) {
                    return Ok(());
                }
                let start = Instant::now();
                cache
                    .save_snapshot_group(group)
                    .expect("unable to save snapshot group");
                info!(
                    "waited {}s to save group",
                    start.elapsed().as_secs_f64()
                );
                trace!("group saved");
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
    use crossterm::event::Event as TermEvent;
    use crossterm::event::KeyEventKind::{Press, Release};
    use ui::Event::*;

    const KEYBINDINGS: &[((KeyModifiers, KeyCode), Event)] = &[
        ((KeyModifiers::empty(), KeyCode::Left), Left),
        ((KeyModifiers::empty(), KeyCode::Char('h')), Left),
        ((KeyModifiers::empty(), KeyCode::Right), Right),
        ((KeyModifiers::empty(), KeyCode::Char(';')), Right),
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
        TermEvent::Key(event) if [Press, Release].contains(&event.kind) =>
            KEYBINDINGS.iter().find_map(|((mods, code), ui_event)| {
                if event.modifiers == *mods && event.code == *code {
                    Some(ui_event.clone())
                } else {
                    None
                }
            }),
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
pub fn new_style(template: &str) -> ProgressStyle {
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

pub fn new_pb(template: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner().with_style(new_style(template));
    pb.enable_steady_tick(PB_TICK_INTERVAL);
    pb
}

pub fn mpb_add(mpb: &MultiProgress, template: &str) -> ProgressBar {
    let pb =
        mpb.add(ProgressBar::new_spinner().with_style(new_style(template)));
    pb.enable_steady_tick(PB_TICK_INTERVAL);
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
