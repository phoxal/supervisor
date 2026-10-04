//! The supervisor lock: one supervisor per bundle owner, for its whole life.
//!
//! The lock - not the socket's existence - is what "an execution is live"
//! means. A socket file survives a killed supervisor; an advisory lock does not,
//! because the kernel releases it when the holder's descriptor closes however
//! the process ended.
//!
//! It is acquired before the bundle is even read and held until the process
//! exits, which is exactly the window in which a bundle-mutating command must
//! refuse to replace files underneath a running graph.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// An exclusively held supervisor lock. Dropping it (or exiting) releases it.
#[derive(Debug)]
pub(crate) struct SupervisorLock {
    _file: File,
    path: PathBuf,
}

impl SupervisorLock {
    /// Take the lock at `path`, failing if another supervisor already owns it.
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        let directory = path
            .parent()
            .context("the supervisor lock path has no parent directory")?;
        std::fs::create_dir_all(directory).with_context(|| {
            format!(
                "failed to create the runtime directory {}",
                directory.display()
            )
        })?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .write(true)
            .open(path)
            .with_context(|| format!("failed to open the supervisor lock {}", path.display()))?;
        if crate::rendezvous::try_advisory_lock(&file, true).is_err() {
            bail!(
                "another phoxal-supervisor already owns this bundle's execution (the supervisor \
                 lock {} is held); attach to it or stop it rather than starting a second \
                 supervisor",
                path.display()
            );
        }
        // The pid is diagnostic only - the lock itself is the authority, so a
        // failure to record it never fails the acquisition.
        let _ = file.set_len(0);
        let _ = writeln!(file, "{}", std::process::id());
        Ok(Self {
            _file: file,
            path: path.to_path_buf(),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_second_holder_is_refused_and_a_released_lock_is_free_again() {
        let dir = tempfile::tempdir().expect("temp dir");
        // Deliberately below a directory that does not exist yet: the supervisor
        // creates its own run directory.
        let path = dir.path().join("run").join("supervisor.lock");

        let held = SupervisorLock::acquire(&path).expect("the first supervisor takes the lock");
        assert_eq!(held.path(), path);
        let error = SupervisorLock::acquire(&path).expect_err("a second supervisor is refused");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("already owns"), "{rendered}");
        assert!(rendered.contains("attach to it or stop it"), "{rendered}");

        drop(held);
        SupervisorLock::acquire(&path).expect("a released lock is free again");
    }
}
