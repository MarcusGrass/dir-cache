use crate::error::Error;
use crate::opts::{CacheWriteOpt, ExpirationOpt};
use crate::Result;
use crate::{
    unix_time_now, MemPushOpt, PrevSync, RamStoreValue, StoreContent, StoreValue, SyncErrorOpt,
    MANIFEST_FILE_NAME,
};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

macro_rules! bail_or_append_continue {
    ($key_ref: expr, $error_expr:expr, $error_container: expr, $sync_err_opt: expr) => {{
        match $error_expr {
            Ok(val) => val,
            Err(e) => match $sync_err_opt {
                SyncErrorOpt::BestAttempt => {
                    $error_container.push(($key_ref.to_string(), e));
                    continue;
                }
                SyncErrorOpt::FailFast => {
                    return Err(e);
                }
            },
        }
    }};
}
pub(crate) struct Manifest {
    pub(crate) version: u64,
    pub(crate) store: HashMap<String, StoreValue>,
}

impl Manifest {
    pub(crate) fn sync_to_disk(
        &mut self,
        dir: &Path,
        mem_flush_opt: MemPushOpt,
        sync_error_opt: SyncErrorOpt,
    ) -> Result<()> {
        let mut manifest_raw = String::new();
        let _ = manifest_raw.write_fmt(format_args!("{}\n", self.version));
        let mut vec_store = self.store.iter_mut().collect::<Vec<_>>();
        vec_store.sort_by(|a, b| a.0.cmp(&b.0));
        //
        let mut errors = vec![];
        for (k, v) in vec_store {
            let new_content = match &mut v.content {
                StoreContent::OnDisk(file_ext) => {
                    let res = manifest_raw
                        .write_fmt(format_args!(
                            "{},{},{},{}\n",
                            k.len() as u16,
                            k,
                            file_ext,
                            v.last_updated.as_secs()
                        ))
                        .map_err(Error::ManifestStringAppendErr);
                    bail_or_append_continue!(k, res, errors, sync_error_opt);
                    None
                }
                StoreContent::InMem(im) => {
                    if let Some(synced) = &im.prev_sync {
                        let res = manifest_raw
                            .write_fmt(format_args!(
                                "{},{},{},{}\n",
                                k.len() as u16,
                                k,
                                synced.content_file,
                                synced.synced_at.as_secs()
                            ))
                            .map_err(Error::ManifestStringAppendErr);
                        bail_or_append_continue!(k, res, errors, sync_error_opt);
                        None
                    } else {
                        let now =
                            bail_or_append_continue!(k, unix_time_now(), errors, sync_error_opt);
                        let le = now.as_nanos().to_le_bytes();
                        let ext = uuid::Builder::from_bytes_le(le);
                        let file_name = ext.as_uuid().to_string();
                        let new_file_path = dir.join(&file_name);
                        let res = std::fs::write(&new_file_path, &im.content)
                            .map_err(|e| Error::WriteContent(new_file_path, e));
                        bail_or_append_continue!(k, res, errors, sync_error_opt);
                        let res = manifest_raw
                            .write_fmt(format_args!(
                                "{},{},{},{}\n",
                                k.len() as u16,
                                k,
                                file_name,
                                now.as_secs()
                            ))
                            .map_err(Error::ManifestStringAppendErr);
                        bail_or_append_continue!(k, res, errors, sync_error_opt);
                        match mem_flush_opt {
                            MemPushOpt::RetainOnWrite => {
                                im.prev_sync = Some(PrevSync {
                                    content_file: file_name,
                                    synced_at: now,
                                });
                                None
                            }
                            MemPushOpt::DumpOnWrite => Some(StoreContent::OnDisk(file_name)),
                        }
                    }
                }
            };
            if let Some(new_content) = new_content {
                v.content = new_content;
            }
        }
        let manifest_out = dir.join(MANIFEST_FILE_NAME);
        let res = std::fs::write(&manifest_out, manifest_raw)
            .map_err(|e| Error::WriteContent(manifest_out, e));
        match sync_error_opt {
            SyncErrorOpt::FailFast => res,
            SyncErrorOpt::BestAttempt => {
                if let Err(e) = res {
                    errors.push(("Manifest".to_string(), e));
                }
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(Error::SyncErr(errors))
                }
            }
        }
    }

    pub(crate) fn deserialize(
        path: &Path,
        raw: &str,
        version: u64,
        eager_load: bool,
        expiration_opt: ExpirationOpt,
    ) -> Result<Self> {
        let mut lines = raw.lines();
        let first = lines
            .next()
            .ok_or_else(|| Error::ParseManifest("Manifest empty".to_string()))?;
        let first_v: u64 = first
            .parse()
            .map_err(|_| Error::ParseManifest("Version not a decimal number".to_string()))?;
        if first_v != version {
            return Err(Error::ParseManifest(format!(
                "Version mismatch, found={first_v}, current={version}"
            )));
        }
        let mut manifest = Self {
            version,
            store: HashMap::new(),
        };
        let now = unix_time_now()?;
        for (ind, line) in lines.enumerate() {
            let line_no = ind + 2;
            let (len, rest) = line.split_once(',').ok_or_else(|| {
                Error::ParseManifest(format!(
                    "Line {line_no} does not start with a valid key length"
                ))
            })?;
            let len_u16: u16 = len.parse().map_err(|_| {
                Error::ParseManifest(format!(
                    "Line {line_no} failed to parse key length as a valid u16 decimal"
                ))
            })?;
            // Leading comma
            let (key, rest) = rest.split_at(len_u16 as usize);
            let key = key.trim_matches(',');
            let rest = rest.trim_start_matches(',');
            let (uuid, _ts) = rest.split_once(',').ok_or_else(|| {
                Error::ParseManifest(format!(
                    "Line {line_no} failed to split into uuid and timestamp"
                ))
            })?;
            let uuid: Uuid = uuid.parse().map_err(|_| {
                Error::ParseManifest(format!("Line {line_no} failed to parse uuid"))
            })?;
            let bytes_le = uuid.to_bytes_le();
            let epoch_nanos = u128::from_le_bytes(bytes_le);
            let secs = epoch_nanos / 1_000_000_000u128;
            let nanos = epoch_nanos % 1_000_000_000u128;
            let age = Duration::new(secs as u64, nanos as u32);
            match expiration_opt {
                ExpirationOpt::DoNothing => {}
                ExpirationOpt::DeleteAfter(max_ttl) => {
                    if age.saturating_add(max_ttl) >= now {
                        let path_ext = uuid.to_string();
                        let content_path = path.join(&path_ext);
                        std::fs::remove_file(&content_path)
                            .map_err(|e| Error::DeleteContent(content_path, e))?;
                        continue;
                    }
                }
            }

            let value = if eager_load {
                let path_ext = uuid.to_string();
                let content_path = path.join(&path_ext);
                let content = std::fs::read(&content_path)
                    .map_err(|e| Error::ReadContent(content_path, e))?;
                StoreValue {
                    content: StoreContent::InMem(RamStoreValue {
                        content,
                        prev_sync: Some(PrevSync {
                            content_file: path_ext,
                            synced_at: age,
                        }),
                    }),
                    last_updated: Default::default(),
                }
            } else {
                StoreValue {
                    content: StoreContent::OnDisk(uuid.to_string()),
                    last_updated: age,
                }
            };
            manifest.store.insert(key.to_string(), value);
        }

        Ok(manifest)
    }

    #[inline]
    pub(crate) fn exists(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }

    pub(crate) fn write_new(
        &mut self,
        base: &Path,
        key: String,
        content: Vec<u8>,
        mem_flush_opt: MemPushOpt,
        cache_write_opt: CacheWriteOpt,
    ) -> Result<()> {
        match cache_write_opt {
            CacheWriteOpt::ManualOnly | CacheWriteOpt::AutoOnDrop => {
                self.store.insert(
                    key,
                    StoreValue {
                        content: StoreContent::InMem(RamStoreValue {
                            content,
                            prev_sync: None,
                        }),
                        last_updated: unix_time_now()?,
                    },
                );
            }
            CacheWriteOpt::OnWrite => {
                let now = unix_time_now()?;
                let le = now.as_nanos().to_le_bytes();
                let ext = uuid::Builder::from_bytes_le(le);
                let file_name = ext.as_uuid().to_string();
                let path = base.join(&file_name);
                std::fs::write(&path, &content)
                    .map_err(|e| Error::WriteContent(path.clone(), e))?;
                match mem_flush_opt {
                    MemPushOpt::RetainOnWrite => {
                        self.store.insert(
                            key,
                            StoreValue {
                                content: StoreContent::InMem(RamStoreValue {
                                    content,
                                    prev_sync: None,
                                }),
                                last_updated: now,
                            },
                        );
                    }
                    MemPushOpt::DumpOnWrite => {
                        self.store.insert(
                            key,
                            StoreValue {
                                content: StoreContent::OnDisk(file_name),
                                last_updated: now,
                            },
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Manifest, RamStoreValue, StoreContent, StoreValue, MANIFEST_VERSION};
    use tempdir::TempDir;

    const DUMMY_CONTENT: &[u8] = b"Hello!";

    #[test]
    fn cache_dir_flush_on_write_eager_read() {
        const DUMMY_KEY: &str = "MyKey";
        let mut manifest = Manifest {
            version: 1,
            store: Default::default(),
        };
        manifest.store.insert(
            DUMMY_KEY.to_string(),
            StoreValue {
                content: StoreContent::InMem(RamStoreValue {
                    content: DUMMY_CONTENT.to_vec(),
                    prev_sync: None,
                }),
                last_updated: Default::default(),
            },
        );
        let tmp = TempDir::new("cache_dir_eager_test").unwrap();
        let sync_to = tmp.path();
        manifest
            .sync_to_disk(sync_to, MemPushOpt::DumpOnWrite, SyncErrorOpt::FailFast)
            .unwrap();
        let val = manifest.store.get(DUMMY_KEY).unwrap();
        assert!(matches!(val.content, StoreContent::OnDisk(_)));
        let manifest = sync_to.join(MANIFEST_FILE_NAME);
        let raw_read = std::fs::read_to_string(&manifest).unwrap();
        let read =
            Manifest::deserialize(sync_to, &raw_read, 1, true, ExpirationOpt::DoNothing).unwrap();
        let has_val = read.store.get(DUMMY_KEY).unwrap();
        let StoreContent::InMem(val) = &has_val.content else {
            panic!("Content not read to mem");
        };
        assert_eq!(DUMMY_CONTENT, val.content.as_slice());
    }

    #[test]
    fn cache_dir_retain_on_write_lazy_read() {
        const DUMMY_KEY: &str = "MyKey";
        let mut manifest = Manifest {
            version: MANIFEST_VERSION,
            store: Default::default(),
        };
        manifest.store.insert(
            DUMMY_KEY.to_string(),
            StoreValue {
                content: StoreContent::InMem(RamStoreValue {
                    content: DUMMY_CONTENT.to_vec(),
                    prev_sync: None,
                }),
                last_updated: Default::default(),
            },
        );
        let tmp = TempDir::new("cache_dir_eager_test").unwrap();
        let sync_to = tmp.path();
        manifest
            .sync_to_disk(sync_to, MemPushOpt::RetainOnWrite, SyncErrorOpt::FailFast)
            .unwrap();
        let val = manifest.store.get(DUMMY_KEY).unwrap();
        let StoreContent::InMem(in_mem) = &val.content else {
            panic!("Expected retained in mem value");
        };
        let Some(ps) = &in_mem.prev_sync else {
            panic!("No prev sync found after retain");
        };
        let manifest = sync_to.join(MANIFEST_FILE_NAME);
        let raw_read = std::fs::read_to_string(&manifest).unwrap();
        let read = Manifest::deserialize(
            sync_to,
            &raw_read,
            MANIFEST_VERSION,
            false,
            ExpirationOpt::DoNothing,
        )
        .unwrap();
        let has_val = read.store.get(DUMMY_KEY).unwrap();
        let StoreContent::OnDisk(ext) = &has_val.content else {
            panic!("Content not read to mem");
        };
        assert_eq!(&ps.content_file, ext);
        assert_eq!(ps.synced_at, has_val.last_updated);
    }

    #[test]
    fn fail_fast_to_disk() {
        let mut manifest = Manifest {
            version: MANIFEST_VERSION,
            store: Default::default(),
        };
        let tmp = TempDir::new("fail_fast_to_disk").unwrap();
        let not_found = tmp.path().join("hello");
        let Err(e) =
            manifest.sync_to_disk(&not_found, MemPushOpt::DumpOnWrite, SyncErrorOpt::FailFast)
        else {
            panic!("Expected err on bad dest write");
        };
        assert!(matches!(e, Error::WriteContent(_, _)));
    }

    #[test]
    fn best_attempt_to_disk() {
        let mut manifest = Manifest {
            version: MANIFEST_VERSION,
            store: Default::default(),
        };
        let tmp = TempDir::new("best_attempt_to_disk").unwrap();
        let not_found = tmp.path().join("hello");
        let Err(e) = manifest.sync_to_disk(
            &not_found,
            MemPushOpt::DumpOnWrite,
            SyncErrorOpt::BestAttempt,
        ) else {
            panic!("Expected err on bad dest write");
        };
        assert!(matches!(e, Error::SyncErr(_)));
    }
}
