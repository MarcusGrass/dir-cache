use dir_cache::error::Error;
use dir_cache::opts::{
    CacheOpenOptions, DirCacheOpts, DirOpenOpt, Encoding, ExpirationOpt, GenerationOpt, MemPullOpt,
    MemPushOpt, SyncOpt,
};
use dir_cache::DirCache;
use std::collections::HashSet;
use std::convert::Infallible;
use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn dummy_key() -> &'static Path {
    Path::new("dummykey")
}

fn dummy_content() -> &'static [u8] {
    b"Dummy content!"
}

#[test]
fn smoke_map_functionality_all_opts() {
    // Make sure all bounded options permutations work as a map, without checking
    // for side effects, good smoke test, will find incompatible combinations of options
    in_all_opts_context(
        3,
        |_opts, _open| true,
        |run_open_fn, _opts| {
            let tmp = tempfile::TempDir::with_prefix("smoke_map_functionality_all_opts").unwrap();
            let mut dc = run_open_fn(tmp.path());
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
        },
    );
}

#[test]
fn smoke_write_some_tiered_keys_all_opts_reopen() {
    in_all_opts_context(
        3,
        |_opts, _open| true,
        |run_open_fn, _opts| {
            let tmp = tempfile::TempDir::with_prefix(
                "smoke_write_a_fairly_large_amount_of_keys_all_opts",
            )
            .unwrap();
            let mut dc = run_open_fn(tmp.path());
            // Write 3 sub dirs, not that many values
            for top_level_dir in 0..2 {
                for sub_dir in 0..2 {
                    for last_sub in 0..2 {
                        let key_base = format!("key-{top_level_dir}-{sub_dir}-{last_sub}");
                        let my_key = Path::new(&key_base);
                        let my_content =
                            format!("content-{top_level_dir}-{sub_dir}-{last_sub}").into_bytes();
                        assert!(dc.get(my_key).unwrap().is_none());
                        dc.insert(my_key, my_content.clone()).unwrap();
                        assert_eq!(&my_content, dc.get(my_key).unwrap().unwrap().as_ref());
                        assert!(dc.remove(my_key).unwrap());
                        assert!(!dc.remove(my_key).unwrap());
                        assert!(dc.get(my_key).unwrap().is_none());
                        assert_eq!(
                            &my_content,
                            dc.get_or_insert(my_key, || Ok::<_, Infallible>(my_content.clone()))
                                .unwrap()
                                .as_ref()
                        );
                        assert_eq!(&my_content, dc.get(my_key).unwrap().unwrap().as_ref());
                    }
                }
            }
            dc.sync().unwrap();
            drop(dc);
            let mut dc = run_open_fn(tmp.path());
            // Make sure the keys are there
            for top_level_dir in 0..2 {
                for sub_dir in 0..2 {
                    for last_sub in 0..2 {
                        let key_base = format!("key-{top_level_dir}-{sub_dir}-{last_sub}");
                        let my_key = Path::new(&key_base);
                        let my_content =
                            format!("content-{top_level_dir}-{sub_dir}-{last_sub}").into_bytes();
                        assert_eq!(&my_content, dc.get(my_key).unwrap().unwrap().as_ref());
                        assert!(dc.remove(my_key).unwrap());
                        assert!(!dc.remove(my_key).unwrap());
                        assert!(dc.get(my_key).unwrap().is_none());
                    }
                }
            }
        },
    );
}

#[test]
fn create_only_if_exists_fail_if_not_exists() {
    let tmp = tempfile::TempDir::with_prefix("create_only_if_exists_fail_if_not_exists").unwrap();
    let doesnt_exist = tmp.path().join("missing");
    let Err(e) = DirCacheOpts::default().open(
        &doesnt_exist,
        CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, true),
    ) else {
        panic!("Expected err on dir not existing");
    };
    assert!(matches!(e, Error::Open(_)));
}

#[test]
fn create_only_if_exists_works_if_exists() {
    let tmp = tempfile::TempDir::with_prefix("create_only_if_exists_works_if_exists").unwrap();
    let exists = tmp.path();
    DirCacheOpts::default()
        .open(
            &exists,
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, true),
        )
        .unwrap();
}

#[test]
fn create_if_missing_will_create() {
    let tmp = tempfile::TempDir::with_prefix("create_if_missing_will_create").unwrap();
    let doesnt_exist = tmp.path().join("missing");
    let _ = DirCacheOpts::default()
        .open(
            &doesnt_exist,
            CacheOpenOptions::new(DirOpenOpt::CreateIfMissing, true),
        )
        .unwrap();
    assert_dir_at(&doesnt_exist);
}

#[test]
fn open_on_existing_file_fails() {
    let tmp = tempfile::TempDir::with_prefix("create_if_missing_will_create").unwrap();
    let bad_file = tmp.path().join("badfile");
    std::fs::write(&bad_file, "grenade").unwrap();
    let expect_err = DirCacheOpts::default().open(
        &bad_file,
        CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, true),
    );
    assert!(matches!(expect_err, Err(Error::Open(_))));
    let expect_err = DirCacheOpts::default().open(
        &bad_file,
        CacheOpenOptions::new(DirOpenOpt::CreateIfMissing, true),
    );
    assert!(matches!(expect_err, Err(Error::WriteContent(_, _))));
}

#[test]
fn insert_then_get_with_defaults() {
    let tmp = tempfile::TempDir::with_prefix("insert_then_get_with_defaults").unwrap();
    let cd = tmp.path().join("cache-dir");
    let mut dc = DirCacheOpts::default()
        .open(
            &cd,
            CacheOpenOptions::new(DirOpenOpt::CreateIfMissing, true),
        )
        .unwrap();
    let my_key = dummy_key();
    let my_content = dummy_content();
    dc.insert(my_key, my_content.to_vec()).unwrap();
    let content = dc.get(my_key).unwrap().unwrap();
    assert_eq!(my_content, content.as_ref());
}

#[test]
fn insert_with_then_get_with_defaults() {
    let tmp = tempfile::TempDir::with_prefix("insert_with_then_get_with_defaults").unwrap();
    let cd = tmp.path().join("cache-dir");
    let mut dc = DirCacheOpts::default()
        .open(
            &cd,
            CacheOpenOptions::new(DirOpenOpt::CreateIfMissing, true),
        )
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
    let tmp = tempfile::TempDir::with_prefix("insert_with_then_remove_with_defaults").unwrap();
    let cd = tmp.path().join("cache-dir");
    let mut dc = DirCacheOpts::default()
        .open(
            &cd,
            CacheOpenOptions::new(DirOpenOpt::CreateIfMissing, true),
        )
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
fn check_sync_on_write() {
    in_all_opts_context(
        3,
        |opts: &DirCacheOpts, _open: &CacheOpenOptions| {
            // Don't write anything automatically
            matches!(opts.mem_pull_opt, MemPullOpt::DontKeepInMemoryOnRead)
                && matches!(opts.mem_push_opt, MemPushOpt::PassthroughWrite)
        },
        |cache_create, _opts| {
            let tmp = tempfile::TempDir::with_prefix("check_sync_on_write").unwrap();
            assert_empty_dir_at(tmp.path());
            let mut dc = cache_create(tmp.path());
            let my_key = dummy_key();
            let my_content = dummy_content();
            dc.insert(my_key, my_content.to_vec()).unwrap();
            assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
            assert_dir_at(&tmp.path().join(my_key));
            assert_file_at(&tmp.path().join(my_key).join("dir-cache-manifest.txt"));
            assert_file_at(&tmp.path().join(my_key).join("dir-cache-generation-0"));
        },
    );
}

#[test]
fn check_manual_sync_to_disk() {
    in_all_opts_context(
        3,
        |opts: &DirCacheOpts, _open: &CacheOpenOptions| {
            // Don't write anything automatically
            matches!(opts.mem_pull_opt, MemPullOpt::KeepInMemoryOnRead)
                && matches!(opts.mem_push_opt, MemPushOpt::MemoryOnly)
        },
        |cache_create, _opts| {
            let tmp = tempfile::TempDir::with_prefix("check_manual_sync_to_disk").unwrap();
            assert_empty_dir_at(tmp.path());
            let mut dc = cache_create(tmp.path());
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
            assert_file_at(&tmp.path().join(my_key).join("dir-cache-manifest.txt"));
        },
    );
}

#[test]
fn check_sync_on_drop() {
    in_all_opts_context(
        3,
        |opts: &DirCacheOpts, _open: &CacheOpenOptions| {
            // Don't write anything automatically
            matches!(opts.mem_pull_opt, MemPullOpt::KeepInMemoryOnRead)
                && matches!(opts.mem_push_opt, MemPushOpt::MemoryOnly)
                && matches!(opts.sync_opt, SyncOpt::SyncOnDrop)
        },
        |cache_create, _opts| {
            let tmp = tempfile::TempDir::with_prefix("check_sync_on_drop").unwrap();
            assert_empty_dir_at(tmp.path());
            let mut dc = cache_create(tmp.path());
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
            assert_file_at(&tmp.path().join(my_key).join("dir-cache-manifest.txt"));
        },
    );
}

#[test]
fn insert_sync_drop_reopen() {
    let tmp = tempfile::TempDir::with_prefix("insert_sync_drop_reopen").unwrap();
    assert_empty_dir_at(tmp.path());
    let mut dc = DirCacheOpts::default()
        .with_sync_opt(SyncOpt::SyncOnDrop)
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false),
        )
        .unwrap();
    let my_key = dummy_key();
    let my_content = dummy_content();
    assert!(dc.get(my_key).unwrap().is_none());
    dc.insert(my_key, my_content.to_vec()).unwrap();
    assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
    drop(dc);
    let mut new_dc = DirCacheOpts::default()
        .with_sync_opt(SyncOpt::SyncOnDrop)
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false),
        )
        .unwrap();
    assert_eq!(my_content, new_dc.get(my_key).unwrap().unwrap().as_ref());
}

#[test]
#[cfg(unix)]
fn rejects_bad_paths_on_saves() {
    let tmp = tempfile::TempDir::with_prefix("rejects_bad_paths_on_saves").unwrap();
    assert_empty_dir_at(tmp.path());
    let mut dc = DirCacheOpts::default()
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false),
        )
        .unwrap();
    // Absolute path on unix, does not join properly
    let opts = *dc.opts();
    let unsafe_key = Path::new("/absolute");
    assert!(dc.get(unsafe_key).unwrap().is_none());
    assert!(dc.get_opt(unsafe_key, opts).unwrap().is_none());
    assert!(matches!(
        dc.get_or_insert(unsafe_key, || Ok::<_, Infallible>(b"".to_vec())),
        Err(Error::DangerousKey(_))
    ));
    assert!(matches!(
        dc.get_or_insert_opt(unsafe_key, || Ok::<_, Infallible>(b"".to_vec()), opts),
        Err(Error::DangerousKey(_))
    ));
    assert!(matches!(
        dc.insert(unsafe_key, b"".to_vec()),
        Err(Error::DangerousKey(_))
    ));
    assert!(matches!(
        dc.insert_opt(unsafe_key, b"".to_vec(), opts),
        Err(Error::DangerousKey(_))
    ));
    assert!(!dc.remove(unsafe_key).unwrap());
}

#[test]
fn write_generational_all_opts() {
    in_all_opts_context(
        6,
        |opts: &DirCacheOpts, _open: &CacheOpenOptions| {
            // Don't write anything automatically
            opts.generation_opt.max_generations == NonZeroUsize::new(4).unwrap()
                && !matches!(opts.mem_push_opt, MemPushOpt::MemoryOnly)
        },
        |cache_create, _opts| {
            let tmp = tempfile::TempDir::with_prefix("write_generational_all_opts").unwrap();
            assert_empty_dir_at(tmp.path());
            let mut dc = cache_create(tmp.path());
            let my_key = dummy_key();
            dc.insert(my_key, b"gen5".to_vec()).unwrap();
            dc.insert(my_key, b"gen4".to_vec()).unwrap();
            dc.insert(my_key, b"gen3".to_vec()).unwrap();
            dc.insert(my_key, b"gen2".to_vec()).unwrap();
            dc.insert(my_key, b"gen1".to_vec()).unwrap();
            dc.insert(my_key, b"gen0".to_vec()).unwrap();
            dc.sync().unwrap();
            let path = tmp.path().join(my_key);
            let mut files = all_files_in(&path);
            assert_eq!(5, files.len(), "files: {files:?}");
            let expect_manifest = path.join("dir-cache-manifest.txt");
            assert!(files.remove(&expect_manifest));
            let expect_gen0 = path.join("dir-cache-generation-0");
            assert!(files.remove(&expect_gen0));
            let content = std::fs::read(&expect_gen0).unwrap();
            assert_eq!(b"gen0".as_slice(), &content);
            let expect_gen1 = path.join("dir-cache-generation-1");
            assert!(files.remove(&expect_gen1));
            let content = std::fs::read(&expect_gen1).unwrap();
            assert_eq!(b"gen1".as_slice(), &content);
            let expect_gen2 = path.join("dir-cache-generation-2");
            assert!(files.remove(&expect_gen2));
            let content = std::fs::read(&expect_gen2).unwrap();
            assert_eq!(b"gen2".as_slice(), &content);
            let expect_gen3 = path.join("dir-cache-generation-3");
            assert!(files.remove(&expect_gen3));
            let content = std::fs::read(&expect_gen3).unwrap();
            assert_eq!(b"gen3".as_slice(), &content);
            assert!(files.is_empty());
            // Removes all generations
            assert!(dc.remove(my_key).unwrap());
            assert!(check_path(&tmp.path().join(my_key)).is_none());
        },
    );
}

#[test]
fn write_generational_not_if_in_mem_only() {
    let tmp = tempfile::TempDir::with_prefix("write_generational_not_if_in_mem_only").unwrap();
    assert_empty_dir_at(tmp.path());
    let mut dc = DirCacheOpts::default()
        .with_generation_opt(GenerationOpt::new(
            NonZeroUsize::new(4).unwrap(),
            Encoding::Plain,
            ExpirationOpt::NoExpiry,
        ))
        .with_mem_push_opt(MemPushOpt::MemoryOnly)
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false),
        )
        .unwrap();
    let my_key = dummy_key();
    dc.insert(my_key, b"gen5".to_vec()).unwrap();
    dc.insert(my_key, b"gen4".to_vec()).unwrap();
    dc.insert(my_key, b"gen3".to_vec()).unwrap();
    dc.insert(my_key, b"gen2".to_vec()).unwrap();
    dc.insert(my_key, b"gen1".to_vec()).unwrap();
    dc.insert(my_key, b"gen0".to_vec()).unwrap();
    assert_empty_dir_at(tmp.path());
    dc.sync().unwrap();
    let path = tmp.path().join(my_key);
    let mut files = all_files_in(&path);
    assert_eq!(2, files.len(), "files: {files:?}");
    let expect_manifest = path.join("dir-cache-manifest.txt");
    assert!(files.remove(&expect_manifest));
    let expect_gen0 = path.join("dir-cache-generation-0");
    assert!(files.remove(&expect_gen0));
    let content = std::fs::read(&expect_gen0).unwrap();
    assert_eq!(b"gen0".as_slice(), &content);
}

#[test]
#[cfg(feature = "lz4")]
fn write_generational_lz4() {
    let tmp = tempfile::TempDir::with_prefix("write_generational_lz4").unwrap();
    assert_empty_dir_at(tmp.path());
    let mut dc = DirCacheOpts::default()
        .with_generation_opt(GenerationOpt::new(
            NonZeroUsize::new(4).unwrap(),
            Encoding::Lz4,
            ExpirationOpt::NoExpiry,
        ))
        .with_mem_push_opt(MemPushOpt::PassthroughWrite)
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false),
        )
        .unwrap();
    let my_key = dummy_key();

    dc.insert(my_key, b"gen5".to_vec()).unwrap();
    dc.insert(my_key, b"gen4".to_vec()).unwrap();
    dc.insert(my_key, b"gen3".to_vec()).unwrap();
    dc.insert(my_key, b"gen2".to_vec()).unwrap();
    dc.insert(my_key, b"gen1".to_vec()).unwrap();
    dc.insert(my_key, b"gen0".to_vec()).unwrap();
    let path = tmp.path().join(my_key);
    let mut files = all_files_in(&path);
    assert_eq!(5, files.len(), "files: {files:?}");
    let expect_manifest = path.join("dir-cache-manifest.txt");
    assert!(files.remove(&expect_manifest));
    let expect_gen0 = path.join("dir-cache-generation-0");
    assert!(files.remove(&expect_gen0));
    let content = std::fs::read(&expect_gen0).unwrap();

    assert_eq!(b"gen0".as_slice(), &content);
    let expect_gen1 = path.join("dir-cache-generation-1");
    assert!(files.remove(&expect_gen1));
    let content = std::fs::read(&expect_gen1).unwrap();
    assert_eq!(encode(b"gen1"), content);
    let expect_gen2 = path.join("dir-cache-generation-2");
    assert!(files.remove(&expect_gen2));
    let content = std::fs::read(&expect_gen2).unwrap();
    assert_eq!(encode(b"gen2"), content);
    let expect_gen3 = path.join("dir-cache-generation-3");
    assert!(files.remove(&expect_gen3));
    let content = std::fs::read(&expect_gen3).unwrap();
    assert_eq!(encode(b"gen3"), content);
    assert!(files.is_empty());
    // Removes all generations
    assert!(dc.remove(my_key).unwrap());
    assert!(check_path(&tmp.path().join(my_key)).is_none());
}

#[test]
fn tolerates_foreign_files() {
    let tmp = tempfile::TempDir::with_prefix("tolerates_foreign_files").unwrap();
    assert_empty_dir_at(tmp.path());
    let mut dc = DirCacheOpts::default()
        .with_sync_opt(SyncOpt::SyncOnDrop)
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false),
        )
        .unwrap();
    let my_key = dummy_key();
    let my_content = dummy_content();
    dc.insert(my_key, my_content.to_vec()).unwrap();
    assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
    drop(dc);
    let files = all_files_in(&tmp.path().join(my_key));
    assert_eq!(2, files.len());
    std::fs::write(
        tmp.path().join(my_key).join("rogue_user_file"),
        b"Rogue content!".to_vec(),
    )
    .unwrap();
    let files = all_files_in(&tmp.path().join(my_key));
    assert_eq!(3, files.len());
    let mut dc = DirCacheOpts::default()
        .with_sync_opt(SyncOpt::SyncOnDrop)
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false),
        )
        .unwrap();
    assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
    assert!(dc.remove(my_key).unwrap());
    let files = all_files_in(&tmp.path().join(my_key));
    assert_eq!(1, files.len());
    let file = files.into_iter().next().unwrap();
    assert!(file.ends_with("rogue_user_file"));
}

#[test]
fn can_write_and_pick_up_subdirs() {
    let tmp = tempfile::TempDir::with_prefix("can_write_subdirs").unwrap();
    assert_empty_dir_at(tmp.path());
    let mut dc = DirCacheOpts::default()
        .with_sync_opt(SyncOpt::SyncOnDrop)
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false),
        )
        .unwrap();
    let my_key = dummy_key();
    let my_content = dummy_content();
    dc.insert(my_key, my_content.to_vec()).unwrap();
    assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
    let my_sub_key = my_key.join("sub");
    let my_sub_content = b"Good content";
    dc.insert(&my_sub_key, my_sub_content.to_vec()).unwrap();
    assert_eq!(
        my_sub_content,
        dc.get(&my_sub_key).unwrap().unwrap().as_ref()
    );
    drop(dc);
    let mut dc = DirCacheOpts::default()
        .with_sync_opt(SyncOpt::SyncOnDrop)
        .open(
            tmp.path(),
            CacheOpenOptions::new(DirOpenOpt::OnlyIfExists, false),
        )
        .unwrap();
    assert_eq!(my_content, dc.get(my_key).unwrap().unwrap().as_ref());
    assert_eq!(
        my_sub_content,
        dc.get(&my_sub_key).unwrap().unwrap().as_ref()
    );
    // Removing outer first, will leave an empty outer dir
    assert!(dc.remove(&my_key).unwrap());
    assert!(dc.get(my_key).unwrap().is_none());
    assert!(all_files_in(&tmp.path().join(my_key)).is_empty());
    assert_dir_at(&tmp.path().join(my_key));

    assert!(dc.remove(&my_sub_key).unwrap());
    assert!(dc.get(&my_sub_key).unwrap().is_none());
    assert!(check_path(&tmp.path().join(my_sub_key)).is_none());
    // Remains
    assert_dir_at(&tmp.path().join(my_key));
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

fn in_all_opts_context<
    UserFn: FnMut(Box<dyn Fn(&Path) -> DirCache>, DirCacheOpts),
    UserFilterFn: Fn(&DirCacheOpts, &CacheOpenOptions) -> bool,
>(
    num_generations: usize,
    filter: UserFilterFn,
    mut user_fn: UserFn,
) {
    for mem_pull in [
        MemPullOpt::DontKeepInMemoryOnRead,
        MemPullOpt::KeepInMemoryOnRead,
    ] {
        for mem_push in [
            MemPushOpt::MemoryOnly,
            MemPushOpt::PassthroughWrite,
            MemPushOpt::RetainAndWrite,
        ] {
            for i in 0..num_generations {
                for exp in [
                    ExpirationOpt::NoExpiry,
                    ExpirationOpt::ExpiresAfter(Duration::from_secs(1_000)),
                ] {
                    let gen =
                        GenerationOpt::new(NonZeroUsize::new(i + 1).unwrap(), Encoding::Plain, exp);
                    for sync in [SyncOpt::SyncOnDrop, SyncOpt::ManualSync] {
                        for dir_open in [DirOpenOpt::OnlyIfExists, DirOpenOpt::CreateIfMissing] {
                            for eager in [true, false] {
                                let opts = DirCacheOpts::new(mem_pull, mem_push, gen, sync);
                                let cache_open_opts = CacheOpenOptions::new(dir_open, eager);
                                if filter(&opts, &cache_open_opts) {
                                    let this_fn = Box::new(move |path: &Path| {
                                        opts.open(path, cache_open_opts).unwrap()
                                    });
                                    user_fn(this_fn, opts);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn all_files_in(path: &Path) -> HashSet<PathBuf> {
    let mut v = HashSet::new();
    for e in std::fs::read_dir(path).unwrap() {
        let entry = e.unwrap();
        let md = entry.metadata().unwrap();
        if md.is_file() {
            v.insert(entry.path());
        }
    }
    v
}

#[cfg(feature = "lz4")]
fn encode(content: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut encoder = lz4::EncoderBuilder::new().build(&mut buf).unwrap();
    std::io::Write::write(&mut encoder, &content).unwrap();
    buf
}
