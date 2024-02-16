use crate::error::{Error, Result};
use std::path::Path;

// The path needs to be safe, there will be a lot of path joining.
// Paths are a nightmare, this is just a best attempt at protecting the user from themselves.
// Joining on an absolute path replaces the path, which is the danger.
// This is not a catch-all, the user will have to take care with the paths provided as keys.
#[inline]
pub(crate) fn reject_demonstrably_unsafe_key(path: &Path) -> Result<()> {
    if path.is_absolute() {
        Err(Error::DangerousKey(format!(
            "Path {path:?} is absolute, should not be used as key"
        )))
    } else {
        Ok(())
    }
}
