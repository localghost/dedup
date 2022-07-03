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
    no_symlinks: bool,
}

fn hash(path: &str) -> Result<String> {
    println!("Checking {}", path);
    let mut f = File::open(path)?;
    let mut hasher = Sha1::new();
    io::copy(&mut f, &mut hasher)?;
    Ok(hex::encode(hasher.finalize()))
}

fn scan_directory(path: PathBuf) -> Result<HashMap<String, Vec<String>>> {
    let cpus = num_cpus::get();
    let pool = ThreadPoolBuilder::new()
        .num_threads(std::cmp::max(cpus / 2, 1))
        .build()?;
    let results = pool.scope(|_scope| {
        WalkDir::new(path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .collect::<Vec<_>>()
            .par_iter()
            .map(|entry| -> Option<(_, _)> {
                Some((
                    hash(entry.path().to_str()?).ok()?,
                    entry.path().to_str()?.to_string(),
                ))
            })
            .collect::<Vec<_>>()
    });
    println!("Merging partial results");
    let mut merged = HashMap::new();
    for (hash, path) in results.into_iter().filter_map(|e| e) {
        merged
            .entry(hash)
            .or_insert(Vec::<String>::new())
            .push(path);
    }
    Ok(merged)
}

fn remove_duplicates<'a>(duplicates: impl Iterator<Item = &'a Vec<String>>, make_symlinks: bool) -> Result<()> {
    for paths in duplicates {
        if paths.len() > 1 {
            let original = &paths[0];
            for path in &paths[1..] {
                std::fs::remove_file(path)?;
                if make_symlinks {
                    println!("Replacing {} with link to {}", path, original);
                    std::os::unix::fs::symlink(original, path)?;
                }
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.dir.exists(), "Path {:?} does not exist", args.dir);
    ensure!(args.dir.is_dir(), "Path {:?} is not a directory", args.dir);

    let results = scan_directory(args.dir)?;

    if let Some(dump_file) = args.dump_file {
        std::fs::write(dump_file, serde_json::to_string_pretty(&results)?)?;
    }

    if !args.dump_only {
        remove_duplicates(results.values(), !args.no_symlinks)?;
    }

    Ok(())
}
