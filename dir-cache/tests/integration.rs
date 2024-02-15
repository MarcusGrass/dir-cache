use dir_cache::error::Error;
use dir_cache::opts::{CacheOpenOptions, DirCacheOpts, DirOpen};
use std::io::ErrorKind;
use std::path::Path;

#[test]
fn create_only_if_exists_fail_if_not_exists() {
    let tmp = tempdir::TempDir::new("create_only_if_exists_fail_if_not_exists").unwrap();
    let doesnt_exist = tmp.path().join("missing");
    let Err(e) = DirCacheOpts::default().open(
        &doesnt_exist,
        CacheOpenOptions::new(DirOpen::OnlyIfExists, true),
    ) else {
        panic!("Expected err on dir not existing");
    };
    assert!(matches!(e, Error::Open(_)));
}

#[test]
fn create_only_if_exists_works_if_exists() {
    let tmp = tempdir::TempDir::new("create_only_if_exists_works_if_exists").unwrap();
    let exists = tmp.path();
    DirCacheOpts::default()
        .open(&exists, CacheOpenOptions::new(DirOpen::OnlyIfExists, true))
        .unwrap();
}

#[test]
fn create_if_missing_will_create() {
    let tmp = tempdir::TempDir::new("create_if_missing_will_create").unwrap();
    let doesnt_exist = tmp.path().join("missing");
    let _ = DirCacheOpts::default()
        .open(
            &doesnt_exist,
            CacheOpenOptions::new(DirOpen::CreateIfMissing, true),
        )
        .unwrap();
    assert_dir_at(&doesnt_exist);
}

#[derive(Debug, Eq, PartialEq)]
enum ExpectedDiskObject {
    File,
    Dir,
}

fn assert_dir_at(path: &Path) {
    let p = check_path(path).expect("Expected dir, found nothing");
    assert_eq!(ExpectedDiskObject::Dir, p, "Wanted dir, found file");
}

fn check_path(path: &Path) -> Option<ExpectedDiskObject> {
    match std::fs::metadata(path) {
        Ok(m) => {
            if m.is_file() {
                return Some(ExpectedDiskObject::File);
            }
            if m.is_dir() {
                return Some(ExpectedDiskObject::Dir);
            }
            panic!("Unexpected disk object at {m:?}");
        }
        Err(e) if e.kind() == ErrorKind::NotFound => None,
        Err(e) => {
            panic!("Failed to check path: {e}");
        }
    }
}
