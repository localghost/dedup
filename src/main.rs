use num_cpus;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io;
use walkdir::WalkDir;

fn hash(path: &str) -> String {
    println!("Hashing {} on {:?}...", path, rayon::current_thread_index());
    let mut f = File::open(path).unwrap();
    let mut hasher = Sha1::new();
    io::copy(&mut f, &mut hasher).unwrap();
    hex::encode(hasher.finalize())
}

fn main() -> std::io::Result<()> {
    let cpus = num_cpus::get();
    let pool = ThreadPoolBuilder::new().num_threads(cpus).build();
    let results = pool.unwrap().scope(|_scope| {
        WalkDir::new("/home/kostrzewa/Downloads")
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
    println!("{:?}", merged);
    Ok(())
}
