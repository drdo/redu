#![feature(iter_intersperse)]

use std::io::stdout;
use clap::{command, Parser};

use futures::TryStreamExt;
use crate::cache::Cache;
use crate::restic::Restic;

use crate::types::Snapshot;

mod cache;
mod restic;
mod types;
mod ncdu;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    repo: String,
    #[arg(long)]
    password_command: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = Cli::parse();
    let mut cache = Cache::open().await.unwrap();
    let restic = Restic::new(&cli.repo, cli.password_command.as_ref().map(|s| s.as_str()));

    eprintln!("Using cache directory '{}'", cache.dir());

    // Gather data from restic
    let snapshots: Vec<Snapshot> = {
        eprintln!("Fetching restic snapshot list");
        let restic_snapshots = restic.snapshots().unwrap();

        // Delete snapshots from the DB that were deleted on Restic
        for snapshot in cache.get_snapshots().await.unwrap() {
            if ! restic_snapshots.contains(&snapshot) {
                eprintln!("Deleting DB Snapshot {:?} (missing from restic)", snapshot.id.as_str());
                cache.delete_snapshot(snapshot.id.as_str()).await.unwrap();
            }
        }

        let db_snapshots = cache.get_snapshots().await.unwrap();
        restic_snapshots.into_iter().filter(|s| ! db_snapshots.contains(s)).collect()
    };
    if snapshots.len() > 0 {
        eprintln!("Need to fetch {} snapshot(s)", snapshots.len());
        for (snapshot, i) in snapshots.iter().zip(1..) {
            eprintln!("Fetching snapshot {:?} [{}/{}]", &snapshot.id, i, snapshots.len());
            for e in restic.ls(snapshot.id.as_str()).unwrap() {
                cache.add_file(&e.unwrap()).await.unwrap();
            }
            cache.finish_snapshot(snapshot.id.as_str()).await.unwrap();
        }
    } else {
        eprintln!("Snapshots up to date");
    }

    // Emit ncdu output
    eprintln!("Writing ncdu output");
    {
        let mut stream = cache.get_max_file_sizes().await.unwrap();
        let mut ncdu_writer = ncdu::Writer::new(stdout());
        ncdu_writer.header().unwrap();
        while let Some((path, size)) = stream.try_next().await.unwrap() {
            let components = path.split('/').filter(|s| !s.is_empty()).collect::<Vec<_>>();
            let (file, dir) = components.split_last().unwrap();
            ncdu_writer.change_dir(dir).unwrap();
            ncdu_writer.file(file, size).unwrap();
        }
        ncdu_writer.finish().unwrap();
    }
}
