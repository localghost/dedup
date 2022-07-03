# dedup
Find and remove duplicated files.

`dedup` walks recursively a directory, computes SHA1 of each file and removes all but one file matching the same hash and leaves symbolic links in place of the removed files.

Computing hash is done in parallel on half of the available CPU cores on the machine. I have not yet checked whether it actually improves performance - wanted to try out [rayon](https://docs.rs/rayon/latest/rayon/).
