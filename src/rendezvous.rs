//! Host rendezvous shared by a supervisor and its clients.
//!
//! These are process-boundary facts, not installation policy. The supervisor
//! names one explicit execution state directory; the framework owns how that
//! directory maps to the socket and lifetime lock. The CLI continues to own
//! its build lock, installation roots, releases, systemd unit, and installed
//! binary policy.

use std::fs;
use std::path::{Path, PathBuf};

use thiserror::Error;

/// The paths through which one execution supervisor and its clients meet.
///
/// Every path is derived literally from the explicit state directory the
/// launcher named; the supervisor never infers a project, release, or
/// installation location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRendezvous {
    state_dir: PathBuf,
}

impl RuntimeRendezvous {
    /// Resolve the shared rendezvous paths for one explicit state directory.
    #[must_use]
    pub fn for_state_dir(state_dir: &Path) -> Self {
        Self {
            state_dir: state_dir.to_path_buf(),
        }
    }

    /// The lifetime lock held exclusively by one supervisor from before it
    /// reads the bundle until the process exits.
    ///
    /// An exclusive holder, not the file's existence, means an execution is
    /// live: the kernel releases the advisory lock when the holder exits even
    /// if a stale file or socket remains.
    #[must_use]
    pub fn supervisor_lock(&self) -> PathBuf {
        self.state_dir.join("supervisor.lock")
    }

    /// The supervisor's Unix-domain socket path without platform validation.
    #[must_use]
    pub fn supervisor_socket(&self) -> PathBuf {
        self.state_dir.join("supervisor.sock")
    }

    /// Return the supervisor socket after validating the platform path limit.
    ///
    /// # Errors
    ///
    /// Returns [`SupervisorSocketPathError`] when the encoded Unix path is too
    /// long to bind, naming the actual and maximum lengths and the path.
    pub fn checked_supervisor_socket(&self) -> Result<PathBuf, SupervisorSocketPathError> {
        let path = self.supervisor_socket();
        let actual = std::os::unix::ffi::OsStrExt::as_bytes(path.as_os_str()).len();
        let maximum = supervisor_socket_path_maximum();
        if actual > maximum {
            return Err(SupervisorSocketPathError {
                path,
                actual,
                maximum,
            });
        }
        Ok(path)
    }
}

/// A supervisor socket path that cannot fit in this platform's `sockaddr_un`.
#[derive(Debug, Error)]
#[error(
    "the supervisor socket path is {actual} bytes but this platform supports at most {maximum}: {path}; choose a shorter --state-dir",
    path = .path.display()
)]
pub struct SupervisorSocketPathError {
    path: PathBuf,
    actual: usize,
    maximum: usize,
}

/// Try to acquire a non-blocking advisory lock on `file`.
///
/// `exclusive` selects an exclusive rather than shared lock. The file remains
/// locked while its descriptor remains open, unless explicitly unlocked.
pub fn try_advisory_lock(file: &fs::File, exclusive: bool) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let operation = if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    } | libc::LOCK_NB;
    // SAFETY: `file` owns a live descriptor for the duration of this call and
    // `operation` is one of flock's documented lock-mode combinations.
    if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Release the advisory lock held through `file`.
#[cfg(test)]
fn unlock_advisory(file: &fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: `file` owns a live descriptor for the duration of this call.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

const fn supervisor_socket_path_maximum() -> usize {
    std::mem::size_of::<libc::sockaddr_un>() - std::mem::offset_of!(libc::sockaddr_un, sun_path) - 1
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use super::*;

    #[test]
    fn the_explicit_state_directory_is_used_literally() {
        let literal = RuntimeRendezvous::for_state_dir(Path::new("/work/robot/run"));
        assert_eq!(literal.state_dir.as_path(), Path::new("/work/robot/run"));
        assert_eq!(
            literal.supervisor_lock(),
            Path::new("/work/robot/run/supervisor.lock")
        );
        assert_eq!(
            literal.supervisor_socket(),
            Path::new("/work/robot/run/supervisor.sock")
        );

        // Historical installed-release and active-runtime roots no longer
        // remap: the launcher's explicit directory is the only rendezvous.
        for explicit in [
            Path::new("/var/phoxal"),
            Path::new("/var/lib/phoxal/releases/20260814T010203.000Z-deadbeef"),
            Path::new("/run/phoxal"),
        ] {
            let paths = RuntimeRendezvous::for_state_dir(explicit);
            assert_eq!(paths.supervisor_lock(), explicit.join("supervisor.lock"));
            assert_eq!(paths.supervisor_socket(), explicit.join("supervisor.sock"));
        }
    }

    #[test]
    fn socket_precheck_is_a_deterministic_platform_boundary() {
        let maximum = supervisor_socket_path_maximum();
        let suffix = "/supervisor.sock";
        let accepted_root = PathBuf::from("x".repeat(maximum - suffix.len()));
        let accepted = RuntimeRendezvous::for_state_dir(&accepted_root);
        assert_eq!(
            std::os::unix::ffi::OsStrExt::as_bytes(
                accepted
                    .checked_supervisor_socket()
                    .expect("the exact boundary is accepted")
                    .as_os_str()
            )
            .len(),
            maximum
        );

        let rejected = RuntimeRendezvous::for_state_dir(&PathBuf::from(
            "x".repeat(maximum - suffix.len() + 1),
        ));
        let error = rejected
            .checked_supervisor_socket()
            .expect_err("one byte beyond the boundary is rejected");
        assert!(error.to_string().contains("shorter --state-dir"), "{error}");
    }

    #[test]
    fn advisory_lock_modes_and_explicit_unlock_share_one_kernel_contract() {
        let directory = tempfile::tempdir().expect("temporary lock directory");
        let path = directory.path().join("supervisor.lock");
        let open = || {
            OpenOptions::new()
                .create(true)
                .read(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .expect("open lock file")
        };
        let first = open();
        let second = open();
        let third = open();

        try_advisory_lock(&first, true).expect("the first exclusive holder acquires the lock");
        assert!(
            try_advisory_lock(&second, true).is_err(),
            "a second exclusive holder must be refused"
        );
        unlock_advisory(&first).expect("explicit unlock releases the exclusive lock");
        try_advisory_lock(&second, true).expect("the lock can be reacquired after explicit unlock");
        unlock_advisory(&second).expect("release the second exclusive holder");

        try_advisory_lock(&first, false).expect("the first shared holder acquires the lock");
        try_advisory_lock(&second, false).expect("a second shared holder is admitted");
        assert!(
            try_advisory_lock(&third, true).is_err(),
            "an exclusive holder must be refused while shared holders remain"
        );
        unlock_advisory(&first).expect("release the first shared holder");
        unlock_advisory(&second).expect("release the second shared holder");
        try_advisory_lock(&third, true)
            .expect("exclusive acquisition succeeds after shared unlock");
        unlock_advisory(&third).expect("release the final holder");
    }
}
