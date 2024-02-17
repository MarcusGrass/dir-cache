#![no_main]

use dir_cache::error::Error;
use dir_cache::opts::{CacheOpenOptions, DirCacheOpts, DirOpenOpt};
use libfuzzer_sys::fuzz_target;
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;

static COUNT: AtomicU64 = AtomicU64::new(0);

fuzz_target!(|data: PathBuf| {
    let this_run = COUNT.fetch_add(1, Ordering::AcqRel);
    let dir_cache_path = TempDir::with_prefix(format!("fuzz-run-{this_run}")).unwrap();
    let value = format!("Run {this_run} value").into_bytes();
    let mut dc = DirCacheOpts::default()
        .open(
            dir_cache_path.path(),
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false),
        )
        .unwrap();
    let key = &data;
    match dc.get_or_insert(key, || Ok::<_, Infallible>(value.clone())) {
        Ok(v) => {
            assert_eq!(value.as_slice(), v.as_ref());
        }
        Err(e) => match e {
            Error::DangerousKey(_) => {}
            e => {
                panic!("Unexpected err: on key={key:?} {e}");
            }
        },
    }
});
