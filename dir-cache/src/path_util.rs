use crate::error::{Error, Result};
use std::path::{Component, Path, PathBuf};

pub(crate) trait SafePathJoin {
    /// The path needs to be safe, there will be a lot of path joining.
    /// Paths are a nightmare, this is just a best attempt at protecting the user from themselves.
    /// Joining on an absolute path replaces the path, which is the danger.
    /// File overwrites and removals are structured with set names, which makes the
    /// danger of a user getting a path replaced to some absolute not as bad, but better safe(r)
    /// than sorry (sorrier).
    /// This is not a catch-all, the user will have to take care with the paths provided as keys.
    fn safe_join<P: AsRef<Path>>(&self, other: P) -> Result<PathBuf>;
}

impl<'a> SafePathJoin for &'a Path {
    #[allow(clippy::disallowed_methods)]
    fn safe_join<P: AsRef<Path>>(&self, other: P) -> Result<PathBuf> {
        let other_ref = other.as_ref();
        // Rather not allow dots on created keys, need to allow the one exception, the manifest file
        if other_ref.is_absolute() {
            return Err(Error::DangerousKey(format!(
                "Got an absolute path when trying to join {self:?} and {other_ref:?}"
            )));
        }
        if other_ref
            .as_os_str()
            .as_encoded_bytes()
            .iter()
            .any(|b| b == &b'\0')
        {
            return Err(Error::DangerousKey(format!(
                "Raw path os str {other_ref:?} no null bytes allowed"
            )));
        }
        let len = other_ref.as_os_str().len();
        let mut cumulative_len = 0;
        let mut num_components = 0;
        for component in other_ref.components() {
            let Component::Normal(os) = component else {
                return Err(Error::DangerousKey(format!(
                    "Found key with an unexpected path component {component:?} when trying to join {self:?} and {other_ref:?}"
                )));
            };
            cumulative_len += os.len();
            num_components += 1;
        }
        if cumulative_len == 0 || cumulative_len + num_components - 1 != len {
            return Err(Error::DangerousKey(format!(
                "Found key that contains a component that is something other than just a normal alphanumeric utf8 string when trying to join {self:?} and {other_ref:?}"
            )));
        }
        let res = self.join(other_ref);
        Ok(res)
    }
}

impl SafePathJoin for PathBuf {
    #[inline]
    fn safe_join<P: AsRef<Path>>(&self, other: P) -> Result<PathBuf> {
        let p: &Path = self.as_ref();
        p.safe_join(other)
    }
}

pub(crate) fn relativize(base: &Path, ext: &Path) -> Result<PathBuf> {
    let mut base_components = base.components();
    let mut ext_components = ext.components();
    loop {
        match (base_components.next(), ext_components.next()) {
            (Some(a), Some(b)) => {
                if a != b {
                    return Err(Error::PathRelativize(format!(
                        "Failed to relativize {base:?} and {ext:?} component mismatch"
                    )));
                }
            }
            (Some(_), None) => {
                return Err(Error::PathRelativize(format!(
                    "Failed to relativize {base:?} and {ext:?} base longer than ext"
                )));
            }
            (None, None) => {
                return Err(Error::PathRelativize(format!(
                    "Failed to relativize {base:?} and {ext:?} same path"
                )));
            }
            (None, Some(ext_first)) => {
                return Ok(std::iter::once(ext_first)
                    .chain(ext_components)
                    .collect::<PathBuf>())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relativize_happy() {
        let base = Path::new("a").join("b").join("c");
        let ext = base.join("d");
        let relative = relativize(&base, &ext).unwrap();
        assert_eq!(Path::new("d"), &relative);
        let longer_ext = base.join("e").join("f").join("g");
        let relative = relativize(&base, &longer_ext).unwrap();
        assert_eq!(Path::new("e").join("f").join("g"), relative);
    }

    #[test]
    fn relativize_sad() {
        let base = Path::new("a").join("b").join("c");
        let ext_too_short = Path::new("a").join("b");
        // Ext shorter than base not allowed
        assert!(relativize(&base, &ext_too_short).is_err());
        // Identical not allowed
        assert!(relativize(&base, &base).is_err());
        let ext_different = Path::new("a").join("c").join("c");
        // Ext not an extension of base
        assert!(relativize(&base, &ext_different).is_err());
    }

    #[test]
    fn safe_join_happy() {
        let base = Path::new("base");
        base.safe_join("some_other_path").unwrap();
        base.safe_join("some/other/path").unwrap();
        base.safe_join("some\\other\\path").unwrap();
    }

    #[test]
    fn safe_join_sad() {
        let base = Path::new("/tmp/fuzz-run-166924lGJEQ/");
        assert!(base.safe_join(Path::new("/root")).is_err());
        assert!(base.safe_join(Path::new(".")).is_err());
        assert!(base.safe_join(Path::new("..")).is_err());
        assert!(base
            .safe_join(Path::new("hello/../../../etc/shadow"))
            .is_err());
        assert!(base
            .safe_join(Path::new("fuzz-run-389boHa9s/s/./\""))
            .is_err());
        assert!(base
            .safe_join(Path::new("dir-cache-manifest.txt/."))
            .is_err());
        assert!(base.safe_join(Path::new("nullterm\0")).is_err());
    }
}
