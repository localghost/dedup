use anyhow::{ensure, Result};
use clap::Parser;
use num_cpus;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use walkdir::WalkDir;

fn hash(path: &str) -> String {
    println!("Hashing {} on {:?}...", path, rayon::current_thread_index());
    let mut f = File::open(path).unwrap();
    let mut hasher = Sha1::new();
    io::copy(&mut f, &mut hasher).unwrap();
    hex::encode(hasher.finalize())
}

#[derive(Parser)]
struct Args {
    #[clap(help = "Directory to search for duplicates in")]
    dir: PathBuf,

    #[clap(
        long,
        value_parser,
        requires = "dump-file",
        help = "Only dump duplicates"
    )]
    dump_only: bool,

    #[clap(long, value_parser, help = "Dump duplicates to a JSON file")]
    dump_file: Option<PathBuf>,

    #[clap(
        long,
        value_parser,
        help = "In place of removed files make symblic links to the one that's left"
    )]
    make_symlinks: bool,
}

fn scan_directory(path: PathBuf) -> HashMap<String, Vec<String>> {
    let cpus = num_cpus::get();
    let pool = ThreadPoolBuilder::new()
        .num_threads(std::cmp::max(cpus / 2, 1))
        .build();
    let results = pool.unwrap().scope(|_scope| {
        WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .collect::<Vec<_>>()
            .par_iter()
            .map(|entry| {
                (
                    hash(entry.path().to_str().unwrap()),
                    entry.path().to_str().unwrap().to_string(),
                )
            })
            .collect::<Vec<_>>()
    });
    println!("Merging...");
    let mut merged = HashMap::new();
    for (hash, path) in results {
        merged
            .entry(hash)
            .or_insert(Vec::<String>::new())
            .push(path);
    }
    merged
}

fn remove_duplicates<'a>(duplicates: impl Iterator<Item = &'a Vec<String>>, make_symlinks: bool) {
    for paths in duplicates {
        if paths.len() > 1 {
            let original = &paths[0];
            for path in &paths[1..] {
                std::fs::remove_file(path).unwrap();
                if make_symlinks {
                    println!("Replacing {} with link to {}", path, original);
                    std::os::unix::fs::symlink(original, path).unwrap();
                }
            }
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.dir.exists(), "Path {:?} does not exist", args.dir);
    ensure!(args.dir.is_dir(), "Path {:?} is not a directory", args.dir);

    let results = scan_directory(args.dir);
    println!("{:?}", results);

    if let Some(dump_file) = args.dump_file {
        std::fs::write(dump_file, serde_json::to_string_pretty(&results).unwrap())?;
    }

    if !args.dump_only {
        remove_duplicates(results.values(), args.make_symlinks);
    }

    Ok(())
}
