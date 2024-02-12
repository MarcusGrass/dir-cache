use dir_cache::error::Error;
use dir_cache::opts::{
    CacheOpenOptions, CacheOptionsBuilder, ManifestWriteOpt, DirOpen, MemPullOpt, MemPushOpt,
};
use std::convert::Infallible;
use std::path::Path;
use tempdir::TempDir;

const DUMMY_KEY: &str = "dummy";
const DUMMY_CONTENT: &[u8] = b"dummy!";

#[test]
fn can_open_if_exists_with_create_if_missing() {
    let tmp = TempDir::new("can_create_if_create_if_exists_is_specified").unwrap();
    let _dc = CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::new(DirOpen::CreateIfMissing, false))
        .open(tmp.path().to_path_buf())
        .unwrap();
}

#[test]
fn will_create_if_missing_with_create_if_missing() {
    let tmp = TempDir::new("can_create_if_missing_with_create_if_missing").unwrap();
    let new = tmp.path().join("missing");
    assert_disk_object_exists(&new, false);
    let _dc = CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::new(DirOpen::CreateIfMissing, false))
        .open(new.clone())
        .unwrap();
    assert_disk_object_exists(&new, true);
}

#[test]
fn fails_if_no_manifest_on_only_if_exists() {
    let tmp = TempDir::new("fails_if_no_manifest_on_only_if_exists").unwrap();
    let Err(e) = CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::new(DirOpen::OnlyIfExists, false))
        .open(tmp.path().to_path_buf())
    else {
        panic!("Successfully open directory with wrong mode specified");
    };
    assert!(matches!(e, Error::UnexpectedDirCacheDoesNotExist));
}

#[test]
#[cfg(unix)]
fn fails_open_on_garbage_path() {
    let bad_path = Path::new("this")
        .join("is")
        .join("a")
        .join("bad")
        .join("path")
        .join("dontblamemeifitexistsonyourmachine");
    let Err(e) = CacheOptionsBuilder::new()
        // Create if missing doesn't recursively create
        .with_cache_open_options(CacheOpenOptions::new(DirOpen::CreateIfMissing, false))
        .open(bad_path.clone())
    else {
        panic!("Managed to create at {bad_path:?}");
    };
    assert!(matches!(e, Error::BadManifestPath(_)));
    let Err(e) = CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::new(DirOpen::OnlyIfExists, false))
        .open(bad_path.clone())
    else {
        panic!("Managed to create at {bad_path:?}");
    };
    assert!(matches!(e, Error::UnexpectedDirCacheDoesNotExist));
}

fn assert_disk_object_exists(path: &Path, expect: bool) {
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

#[test]
fn will_insert_with() {
    let tmp = TempDir::new("immediate_flush_on_write").unwrap();
    let mut dc = CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::new(DirOpen::CreateIfMissing, false))
        .open(tmp.path().to_path_buf())
        .unwrap();
    assert!(dc.get("any").unwrap().is_none());
    assert!(dc
        .get_opt("any", MemPullOpt::KeepInMemoryOnRead)
        .unwrap()
        .is_none());
    let val = dc
        .get_or_insert_with_opt(
            DUMMY_KEY,
            || Ok::<_, Infallible>(DUMMY_CONTENT.to_vec()),
            MemPullOpt::DontKeepInMemoryOnRead,
            MemPushOpt::DumpOnWrite,
            ManifestWriteOpt::OnWrite,
        )
        .unwrap();
    assert_eq!(DUMMY_CONTENT, val.as_ref());
    drop(dc);
    let mut dc = CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::new(DirOpen::CreateIfMissing, false))
        .open(tmp.path().to_path_buf())
        .unwrap();
    let val = dc.get(DUMMY_KEY).unwrap().unwrap();
    assert_eq!(DUMMY_CONTENT, val.as_ref());
}

#[test]
fn will_insert_with_custom_error_back() {
    let tmp = TempDir::new("immediate_flush_on_write").unwrap();
    let mut dc = CacheOptionsBuilder::new()
        .with_cache_open_options(CacheOpenOptions::new(DirOpen::CreateIfMissing, false))
        .open(tmp.path().to_path_buf())
        .unwrap();
    let Err(e) = dc.get_or_insert_with(DUMMY_KEY, || Err("Nooo!".to_string())) else {
        panic!("Expected above string err");
    };
    assert!(e.to_string().ends_with("Nooo!"));
}
