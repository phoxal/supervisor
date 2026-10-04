//! Minimal, dependency-free `sd_notify` for the headless `phoxal-supervisor`
//! process under systemd.
//!
//! A systemd unit running `phoxal-supervisor` with `Type=notify` leaves
//! readiness and watchdog signalling in the foreground supervisor process.
//! Rather than add an `sd_notify` crate for two one-line datagrams, this hand-rolls the protocol: a
//! `AF_UNIX`/`SOCK_DGRAM` socket sending `READY=1` once the supervised graph is
//! ready and `WATCHDOG=1` on a timer while it runs. It supports both address
//! forms systemd hands out in `NOTIFY_SOCKET` - a filesystem path and the Linux
//! abstract-namespace form (`@name`) - and reads the watchdog interval from
//! `WATCHDOG_USEC`/`WATCHDOG_PID`. It owns only the notification wire the
//! supervisor needs.

use std::ffi::OsStr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::time::Duration;

use anyhow::{Context, Result, bail};

/// A connected notify channel to `$NOTIFY_SOCKET`. Owns an unbound datagram
/// socket and the resolved destination address; `Send` so the supervisor can move
/// it into the background readiness/watchdog task.
#[derive(Debug)]
pub(crate) struct SdNotify {
    fd: OwnedFd,
    addr: libc::sockaddr_un,
    addr_len: libc::socklen_t,
    watchdog: Option<Duration>,
}

impl SdNotify {
    /// Build a notify channel from the systemd environment, or `None` when
    /// `NOTIFY_SOCKET` is unset (an interactive, non-systemd invocation). The
    /// watchdog interval is read from `WATCHDOG_USEC`/`WATCHDOG_PID`.
    pub(crate) fn from_env() -> Result<Option<Self>> {
        let Some(socket) = std::env::var_os("NOTIFY_SOCKET") else {
            return Ok(None);
        };
        if socket.is_empty() {
            return Ok(None);
        }
        let mut notify = Self::connect(&socket)?;
        notify.watchdog = watchdog_ping_interval_from_env();
        Ok(Some(notify))
    }

    /// Resolve `address` (a filesystem path or a Linux `@abstract` name) into a
    /// connected datagram channel with no watchdog. Split from [`Self::from_env`]
    /// so the datagram wire can be exercised without touching process env.
    fn connect(address: &OsStr) -> Result<Self> {
        let bytes = address.as_bytes();
        // SAFETY: `sockaddr_un` is a plain C struct; zeroing is a valid init.
        let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let offset = std::mem::offset_of!(libc::sockaddr_un, sun_path);
        let capacity = std::mem::size_of_val(&addr.sun_path);

        // systemd's `@name` selects the Linux abstract namespace: the leading
        // byte of the address is a NUL, and the remaining bytes are the name.
        // A pathname address is copied verbatim and NUL-terminated.
        let (name, abstract_namespace) = match bytes.split_first() {
            Some((b'@', rest)) => (rest, true),
            _ => (bytes, false),
        };
        // Both forms occupy the same span: the abstract form is one leading NUL
        // plus the name; the pathname form is the name plus one trailing NUL.
        let needed = name.len() + 1;
        if needed > capacity {
            bail!(
                "NOTIFY_SOCKET address is {needed} bytes but the platform supports at most {capacity}"
            );
        }
        let start = usize::from(abstract_namespace);
        for (slot, byte) in addr.sun_path[start..start + name.len()]
            .iter_mut()
            .zip(name)
        {
            *slot = *byte as libc::c_char;
        }
        let addr_len = (offset + needed) as libc::socklen_t;

        // SAFETY: a standard unbound datagram socket; the returned fd transfers
        // once into the OwnedFd. `set_cloexec` below applies the same portable
        // close-on-exec path on every supported Unix host.
        let raw = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM, 0) };
        if raw < 0 {
            return Err(std::io::Error::last_os_error()).context("create sd_notify socket");
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        set_cloexec(&fd).context("set FD_CLOEXEC on the sd_notify socket")?;
        Ok(Self {
            fd,
            addr,
            addr_len,
            watchdog: None,
        })
    }

    /// The watchdog ping interval (half of `WATCHDOG_USEC`), or `None` when no
    /// watchdog applies to this process.
    pub(crate) fn watchdog_interval(&self) -> Option<Duration> {
        self.watchdog
    }

    /// Announce the supervised graph is up (`READY=1`).
    pub(crate) fn notify_ready(&self) -> Result<()> {
        self.send(b"READY=1\n")
    }

    /// Keep the systemd watchdog satisfied (`WATCHDOG=1`).
    pub(crate) fn notify_watchdog(&self) -> Result<()> {
        self.send(b"WATCHDOG=1\n")
    }

    fn send(&self, message: &[u8]) -> Result<()> {
        // SAFETY: `addr`/`addr_len` describe a valid, fully-initialized
        // `sockaddr_un`; `message` is a live slice for the duration of the call.
        let sent = unsafe {
            libc::sendto(
                self.fd.as_raw_fd(),
                message.as_ptr().cast::<libc::c_void>(),
                message.len(),
                0,
                std::ptr::addr_of!(self.addr).cast::<libc::sockaddr>(),
                self.addr_len,
            )
        };
        if sent < 0 {
            return Err(std::io::Error::last_os_error()).context("send sd_notify datagram");
        }
        Ok(())
    }
}

/// Set `FD_CLOEXEC` on `fd` so it is closed on every `exec`. This is the
/// portable path (it works on macOS, which cannot request `SOCK_CLOEXEC` on
/// `socket`); on Linux the socket already carries the flag from creation and
/// this is a cheap idempotent confirmation. Without it the notify datagram
/// socket would leak into every participant the supervisor spawns.
fn set_cloexec(fd: &OwnedFd) -> Result<()> {
    let raw = fd.as_raw_fd();
    // SAFETY: `raw` is a live fd owned by `fd` for the duration of the call.
    let flags = unsafe { libc::fcntl(raw, libc::F_GETFD) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error()).context("read fd flags");
    }
    if flags & libc::FD_CLOEXEC == libc::FD_CLOEXEC {
        return Ok(());
    }
    // SAFETY: same live fd; setting the close-on-exec bit is side-effect free.
    if unsafe { libc::fcntl(raw, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error()).context("set fd flags");
    }
    Ok(())
}

/// The watchdog ping interval from the current environment: half of
/// `WATCHDOG_USEC`, but only when `WATCHDOG_PID` is unset or names this process.
fn watchdog_ping_interval_from_env() -> Option<Duration> {
    if let Some(pid) = std::env::var("WATCHDOG_PID")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        && pid != std::process::id() as i32
    {
        return None;
    }
    let usec = std::env::var("WATCHDOG_USEC")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())?;
    watchdog_ping_interval_from_usec(usec)
}

/// Half the watchdog timeout - the systemd convention for how often to ping - or
/// `None` when the timeout is zero (watchdog disabled).
fn watchdog_ping_interval_from_usec(usec: u64) -> Option<Duration> {
    (usec > 0).then(|| Duration::from_micros(usec / 2))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watchdog_interval_is_half_the_timeout_and_zero_disables_it() {
        assert_eq!(
            watchdog_ping_interval_from_usec(10_000_000),
            Some(Duration::from_micros(5_000_000))
        );
        assert_eq!(
            watchdog_ping_interval_from_usec(3),
            Some(Duration::from_micros(1))
        );
        assert_eq!(watchdog_ping_interval_from_usec(0), None);
    }
}
