use std::path::{Path, PathBuf};
use tempdir::TempDir;
use dir_cache::CacheOpenOptions;
use dir_cache::error::Error;

#[test]
fn can_open_if_exists_with_create_if_missing() {
    let tmp = TempDir::new("can_create_if_create_if_exists_is_specified").unwrap();
    let _dc = dir_cache::CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::CreateIfMissing)
        .open(tmp.path().to_path_buf()).unwrap();
}

#[test]
fn will_create_if_missing_with_create_if_missing() {
    let tmp = TempDir::new("can_create_if_missing_with_create_if_missing").unwrap();
    let new = tmp.path().join("missing");
    assert_dir_exists_at(&new, false);
    let _dc = dir_cache::CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::CreateIfMissing)
        .open(new.clone()).unwrap();
    assert_dir_exists_at(&new, true);
}

#[test]
fn fails_if_no_manifest_on_only_if_exists() {
    let tmp = TempDir::new("fails_if_no_manifest_on_only_if_exists").unwrap();
    let Err(e) = dir_cache::CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::OnlyIfExists)
        .open(tmp.path().to_path_buf()) else {
        panic!("Successfully open directory with wrong mode specified");
    };
    assert!(matches!(e, Error::UnexpectedDirCacheDoesNotExist));
}

#[test]
#[cfg(unix)]
fn fails_open_on_garbage_path() {
    let bad_path = Path::new("this").join("is").join("a").join("bad").join("path").join("dontblamemeifitexistsonyourmachine");
    let Err(e) = dir_cache::CacheOptionsBuilder::new()
        // Create if missing doesn't recursively create
        .with_cache_open_options(CacheOpenOptions::CreateIfMissing)
        .open(bad_path.clone()) else {
        panic!("Managed to create at {bad_path:?}");
    };
    assert!(matches!(e, Error::BadManifestPath(_)));
    let Err(e) = dir_cache::CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::OnlyIfExists)
        .open(bad_path.clone()) else {
        panic!("Managed to create at {bad_path:?}");
    };
    assert!(matches!(e, Error::UnexpectedDirCacheDoesNotExist));
}

fn assert_dir_exists_at(path: &Path, expect: bool) {
    match std::fs::metadata(path) {
        Ok(md) => {
            if !md.is_dir() {
                panic!("Found metadata {md:?} at {path:?} but it was not a dir");
            } else {
                assert_eq!(expect, true, "Expected dir to not exists at {path:?}");
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            assert_eq!(expect, false, "Expected dir to not exist at {path:?}");
        }
        Err(e) => {
            panic!("Error checking dir exists at {path:?}: {e}");
        }
    }
}