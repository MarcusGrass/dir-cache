use dir_cache::error::Error;
use dir_cache::opts::{
    CacheOpenOptions, DirCacheOpts, DirOpen, Encoding, ExpirationOpt, GenerationOpt, MemPullOpt,
    MemPushOpt, SyncOpt,
};
use std::collections::HashSet;
use std::convert::Infallible;
use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::path::Path;

fn dummy_key() -> &'static Path {
    Path::new("dummykey")
}

fn dummy_content() -> &'static [u8] {
    b"Dummy content!"
}

#[test]
fn map_functionality_all_opts() {
    // Make sure all bounded options permutations work as a map, without checking
    // for side effects, good smoke test, will find incompatible combinations of options
    let opts = all_opts(3);
    for opt in opts {
        for dir_open in [DirOpen::OnlyIfExists, DirOpen::CreateIfMissing] {
            for eager in [true, false] {
                let tmp = tempdir::TempDir::new("map_functionality_all_opts").unwrap();
                let mut dc = opt
                    .open(tmp.path(), CacheOpenOptions::new(dir_open, eager))
                    .unwrap();
                let my_key = dummy_key();
                let my_content = dummy_content();
                assert!(dc.get(my_key).unwrap().is_none());
                dc.insert(my_key, my_content.to_vec()).unwrap();
                assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
                assert!(dc.remove(my_key).unwrap());
                assert!(!dc.remove(my_key).unwrap());
                assert!(dc.get(my_key).unwrap().is_none());
                assert_eq!(
                    my_content,
                    dc.get_or_insert(my_key, || Ok::<_, Infallible>(my_content.to_vec()))
                        .unwrap()
                        .as_ref()
                );
                assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
                assert!(dc.remove(my_key).unwrap());
                assert!(!dc.remove(my_key).unwrap());
                assert!(dc.get(my_key).unwrap().is_none());
            }
        }
    }
}

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

#[test]
fn insert_then_get_with_defaults() {
    let tmp = tempdir::TempDir::new("insert_then_get_with_defaults").unwrap();
    let cd = tmp.path().join("cache-dir");
    let mut dc = DirCacheOpts::default()
        .open(&cd, CacheOpenOptions::new(DirOpen::CreateIfMissing, true))
        .unwrap();
    let my_key = dummy_key();
    let my_content = dummy_content();
    dc.insert(my_key, my_content.to_vec()).unwrap();
    let content = dc.get(my_key).unwrap().unwrap();
    assert_eq!(my_content, content.as_ref());
}

#[test]
fn insert_with_then_get_with_defaults() {
    let tmp = tempdir::TempDir::new("insert_with_then_get_with_defaults").unwrap();
    let cd = tmp.path().join("cache-dir");
    let mut dc = DirCacheOpts::default()
        .open(&cd, CacheOpenOptions::new(DirOpen::CreateIfMissing, true))
        .unwrap();
    let my_key = dummy_key();
    let my_content = dummy_content();
    let content = dc
        .get_or_insert(my_key, || Ok::<_, Infallible>(my_content.to_vec()))
        .unwrap();
    assert_eq!(my_content, content.as_ref());
    let content = dc.get(my_key).unwrap().unwrap();
    assert_eq!(my_content, content.as_ref());
}

#[test]
fn insert_with_then_remove_with_defaults() {
    let tmp = tempdir::TempDir::new("insert_with_then_remove_with_defaults").unwrap();
    let cd = tmp.path().join("cache-dir");
    let mut dc = DirCacheOpts::default()
        .open(&cd, CacheOpenOptions::new(DirOpen::CreateIfMissing, true))
        .unwrap();
    let my_key = dummy_key();
    let my_content = dummy_content();
    assert!(!dc.remove(my_key).unwrap());
    let content = dc
        .get_or_insert(my_key, || Ok::<_, Infallible>(my_content.to_vec()))
        .unwrap();
    assert_eq!(my_content, content.as_ref());
    let content = dc.get(my_key).unwrap().unwrap();
    assert_eq!(my_content, content.as_ref());
    assert!(dc.remove(my_key).unwrap());
    assert!(dc.get(my_key).unwrap().is_none());
}

#[test]
fn check_auto_sync_to_disk() {
    let tmp = tempdir::TempDir::new("check_auto_sync_to_disk").unwrap();
    assert_empty_dir_at(tmp.path());
    let mut dc = DirCacheOpts::default()
        .with_mem_push_opt(MemPushOpt::PassthroughWrite)
        .with_mem_pull_opt(MemPullOpt::DontKeepInMemoryOnRead)
        .with_generation_opt(GenerationOpt::new(
            NonZeroUsize::MIN,
            Encoding::Plain,
            ExpirationOpt::DoNothing,
        ))
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpen::OnlyIfExists, false),
        )
        .unwrap();
    let my_key = dummy_key();
    let my_content = dummy_content();
    dc.insert(my_key, my_content.to_vec()).unwrap();
    assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
    assert_dir_at(&tmp.path().join(my_key));
    assert_file_at(&tmp.path().join(my_key).join("manifest.txt"));
}

#[test]
fn check_manual_sync_to_disk() {
    let tmp = tempdir::TempDir::new("check_manual_sync_to_disk").unwrap();
    assert_empty_dir_at(tmp.path());
    let mut dc = DirCacheOpts::default()
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpen::OnlyIfExists, false),
        )
        .unwrap();
    let mut opts = *(dc.opts());
    opts = opts.with_mem_push_opt(MemPushOpt::MemoryOnly);
    let my_key = dummy_key();
    let my_content = dummy_content();
    dc.insert_opt(my_key, my_content.to_vec(), opts).unwrap();
    assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
    // Still nothing written
    assert_empty_dir_at(tmp.path());
    dc.sync().unwrap();
    assert_dir_at(&tmp.path().join(my_key));
    assert_file_at(&tmp.path().join(my_key).join("manifest.txt"));
}

#[test]
fn check_sync_on_drop() {
    let tmp = tempdir::TempDir::new("check_sync_on_drop").unwrap();
    assert_empty_dir_at(tmp.path());
    let mut dc = DirCacheOpts::default()
        .with_sync_opt(SyncOpt::SyncOnDrop)
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpen::OnlyIfExists, false),
        )
        .unwrap();
    let mut opts = *(dc.opts());
    opts = opts.with_mem_push_opt(MemPushOpt::MemoryOnly);
    let my_key = dummy_key();
    let my_content = dummy_content();
    dc.insert_opt(my_key, my_content.to_vec(), opts).unwrap();
    assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
    // Still nothing written
    assert_empty_dir_at(tmp.path());
    drop(dc);
    assert_dir_at(&tmp.path().join(my_key));
    assert_file_at(&tmp.path().join(my_key).join("manifest.txt"));
}

#[derive(Debug, Eq, PartialEq)]
enum ExpectedDiskObject {
    File,
    Dir,
}

fn assert_empty_dir_at(path: &Path) {
    let mut seen = HashSet::new();
    for e in std::fs::read_dir(path).unwrap() {
        let entry = e.unwrap();
        seen.insert(entry.path());
    }
    assert!(
        seen.is_empty(),
        "Expected an empty dir, found entries: {seen:?}"
    );
}

fn assert_dir_at(path: &Path) {
    let p = check_path(path).expect("Expected dir, found nothing");
    assert_eq!(ExpectedDiskObject::Dir, p, "Wanted dir, found file");
}

fn assert_file_at(path: &Path) {
    let p = check_path(path).expect("Expected file, found nothing");
    assert_eq!(ExpectedDiskObject::File, p, "Wanted file, found dir");
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

fn all_opts(genarations: usize) -> Vec<DirCacheOpts> {
    let mut v = vec![];
    for mem_pull in [
        MemPullOpt::DontKeepInMemoryOnRead,
        MemPullOpt::KeepInMemoryOnRead,
    ] {
        for mem_push in [
            MemPushOpt::MemoryOnly,
            MemPushOpt::PassthroughWrite,
            MemPushOpt::RetainAndWrite,
        ] {
            for i in 1..genarations {
                for exp in [ExpirationOpt::DoNothing, ExpirationOpt::DoNothing] {
                    let gen =
                        GenerationOpt::new(NonZeroUsize::new(i).unwrap(), Encoding::Plain, exp);
                    for sync in [SyncOpt::SyncOnDrop, SyncOpt::ManualSync] {
                        v.push(DirCacheOpts::new(mem_pull, mem_push, gen, sync));
                    }
                }
            }
        }
    }
    v
}
