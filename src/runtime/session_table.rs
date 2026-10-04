//! Bounded supervisor-side logical-session admission state.
//!
//! This owner is deliberately independent of Zenoh and async execution. The
//! public transport adapter derives the asserted principal from its protected
//! trusted-ingress route, then applies this table before admitting every
//! operation. The route is not a credential by itself: the configured router
//! must enforce the principal-to-namespace association.

use std::collections::BTreeMap;

use phoxal::communication::SESSION_PROTOCOL;
use phoxal::communication::validation::valid_identifier;

/// Baseline public-session lease accepted by the server.
pub const DEFAULT_LEASE_MS: u32 = 30_000;
/// Default finite active-session capacity for one supervisor.
pub const DEFAULT_MAX_SESSIONS: usize = 1_024;
/// Byte length of a collision-resistant opaque session identifier.
pub const SESSION_ID_BYTES: usize = 32;

/// Opaque identifier generated for one successful logical-session open.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SessionId([u8; SESSION_ID_BYTES]);

impl SessionId {
    /// Parse the exact wire representation.
    ///
    /// # Errors
    ///
    /// Returns [`SessionTableError::InvalidSessionId`] unless the value is
    /// exactly 32 bytes.
    pub fn from_bytes(value: &[u8]) -> Result<Self, SessionTableError> {
        let bytes = value
            .try_into()
            .map_err(|_| SessionTableError::InvalidSessionId)?;
        Ok(Self(bytes))
    }

    /// Return the opaque wire bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; SESSION_ID_BYTES] {
        &self.0
    }
}

impl std::fmt::Debug for SessionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SessionId(<redacted>)")
    }
}

/// One active logical session bound to an authenticated routed principal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalSession {
    id: SessionId,
    principal: String,
    protocol: &'static str,
    expires_at_ms: u64,
}

impl LogicalSession {
    /// Opaque session identifier.
    #[must_use]
    pub const fn id(&self) -> SessionId {
        self.id
    }

    /// Principal asserted through the trusted ingress route.
    #[must_use]
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Exact confirmed session protocol.
    #[must_use]
    pub const fn protocol(&self) -> &'static str {
        self.protocol
    }

    /// Server host-monotonic lease deadline in milliseconds.
    #[must_use]
    pub const fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }
}

/// Bounded in-memory session table owned by one supervisor process.
///
/// Constructing a fresh table after restart intentionally restores no session.
#[derive(Debug)]
pub struct SessionTable {
    lease_ms: u32,
    max_sessions: usize,
    sessions: BTreeMap<SessionId, LogicalSession>,
}

impl Default for SessionTable {
    fn default() -> Self {
        Self {
            lease_ms: DEFAULT_LEASE_MS,
            max_sessions: DEFAULT_MAX_SESSIONS,
            sessions: BTreeMap::new(),
        }
    }
}

impl SessionTable {
    /// Create an empty table with finite caller-selected bounds.
    ///
    /// # Errors
    ///
    /// Returns [`SessionTableError::InvalidLimits`] when either bound is zero.
    pub const fn with_limits(
        lease_ms: u32,
        max_sessions: usize,
    ) -> Result<Self, SessionTableError> {
        if lease_ms == 0 || max_sessions == 0 {
            return Err(SessionTableError::InvalidLimits);
        }
        Ok(Self {
            lease_ms,
            max_sessions,
            sessions: BTreeMap::new(),
        })
    }

    /// Number of currently retained sessions, including entries not yet swept.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Whether no sessions are retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// The fixed lease duration applied to newly opened or renewed sessions.
    #[must_use]
    pub const fn lease_ms(&self) -> u32 {
        self.lease_ms
    }

    /// Return the principal for an active session without extending its lease.
    #[must_use]
    pub fn principal(&self, id: SessionId) -> Option<String> {
        self.sessions
            .get(&id)
            .map(|session| session.principal.clone())
    }

    /// Admit a new session for the exact baseline protocol and routed principal.
    ///
    /// Expired entries are swept before the finite capacity check. A new random
    /// identifier is generated for every successful open.
    ///
    /// # Errors
    ///
    /// Returns a typed protocol, principal, capacity, time, entropy, or
    /// collision error.
    pub fn open(
        &mut self,
        protocol: &str,
        principal: impl Into<String>,
        now_ms: u64,
    ) -> Result<LogicalSession, SessionTableError> {
        if protocol != SESSION_PROTOCOL {
            return Err(SessionTableError::UnsupportedProtocol);
        }
        let principal = principal.into();
        if !valid_identifier(&principal) {
            return Err(SessionTableError::InvalidPrincipal);
        }
        self.expire(now_ms);
        if self.sessions.len() >= self.max_sessions {
            return Err(SessionTableError::CapacityExhausted);
        }
        let expires_at_ms = now_ms
            .checked_add(u64::from(self.lease_ms))
            .ok_or(SessionTableError::ClockOverflow)?;
        let mut bytes = [0_u8; SESSION_ID_BYTES];
        getrandom::fill(&mut bytes).map_err(|_| SessionTableError::EntropyUnavailable)?;
        let id = SessionId(bytes);
        if self.sessions.contains_key(&id) {
            return Err(SessionTableError::IdentifierCollision);
        }
        let session = LogicalSession {
            id,
            principal,
            protocol: SESSION_PROTOCOL,
            expires_at_ms,
        };
        self.sessions.insert(id, session.clone());
        Ok(session)
    }

    /// Validate a non-open request against session, principal, and lease.
    ///
    /// # Errors
    ///
    /// Returns distinct unknown, expired, or principal-mismatch failures.
    pub fn authorize(
        &mut self,
        id: SessionId,
        principal: &str,
        now_ms: u64,
    ) -> Result<&LogicalSession, SessionTableError> {
        let Some(session) = self.sessions.get(&id) else {
            return Err(SessionTableError::UnknownSession);
        };
        if now_ms >= session.expires_at_ms {
            self.sessions.remove(&id);
            return Err(SessionTableError::ExpiredSession);
        }
        if session.principal != principal {
            return Err(SessionTableError::PrincipalMismatch);
        }
        self.sessions
            .get(&id)
            .ok_or(SessionTableError::UnknownSession)
    }

    /// Renew one active matching session from the acceptance instant.
    ///
    /// # Errors
    ///
    /// Returns the same authorization errors as [`Self::authorize`] or a clock
    /// overflow.
    pub fn renew(
        &mut self,
        id: SessionId,
        principal: &str,
        now_ms: u64,
    ) -> Result<LogicalSession, SessionTableError> {
        self.authorize(id, principal, now_ms)?;
        let expires_at_ms = now_ms
            .checked_add(u64::from(self.lease_ms))
            .ok_or(SessionTableError::ClockOverflow)?;
        let session = self
            .sessions
            .get_mut(&id)
            .ok_or(SessionTableError::UnknownSession)?;
        session.expires_at_ms = expires_at_ms;
        Ok(session.clone())
    }

    /// Close one active matching session without affecting any other session.
    ///
    /// # Errors
    ///
    /// Returns the same authorization errors as [`Self::authorize`].
    pub fn close(
        &mut self,
        id: SessionId,
        principal: &str,
        now_ms: u64,
    ) -> Result<LogicalSession, SessionTableError> {
        self.authorize(id, principal, now_ms)?;
        self.sessions
            .remove(&id)
            .ok_or(SessionTableError::UnknownSession)
    }

    /// Remove every lease whose deadline has elapsed.
    pub fn expire(&mut self, now_ms: u64) {
        self.sessions
            .retain(|_, session| now_ms < session.expires_at_ms);
    }
}

/// Logical-session admission failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SessionTableError {
    /// Lease or table capacity was zero.
    #[error("session lease and capacity must both be nonzero")]
    InvalidLimits,
    /// The requested protocol is not implemented.
    #[error("requested session protocol is not supported")]
    UnsupportedProtocol,
    /// The asserted principal is not a valid protected route segment.
    #[error("session principal must match [a-z0-9][a-z0-9_-]{{0,63}}")]
    InvalidPrincipal,
    /// The finite active-session table is full.
    #[error("active session capacity is exhausted")]
    CapacityExhausted,
    /// Host-monotonic lease arithmetic overflowed.
    #[error("session lease deadline overflows host time")]
    ClockOverflow,
    /// Secure random bytes could not be obtained.
    #[error("cannot obtain entropy for a session identifier")]
    EntropyUnavailable,
    /// A random identifier collided with a live session.
    #[error("generated session identifier collides with a live session")]
    IdentifierCollision,
    /// The identifier does not have the fixed wire size.
    #[error("session identifier must be exactly 32 bytes")]
    InvalidSessionId,
    /// No active entry has the supplied identifier.
    #[error("logical session is unknown")]
    UnknownSession,
    /// The logical session's finite lease elapsed.
    #[error("logical session lease expired")]
    ExpiredSession,
    /// The authenticated routed principal does not own the session.
    #[error("logical session belongs to another principal")]
    PrincipalMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sessions_are_random_principal_bound_and_independent() {
        let mut table = SessionTable::with_limits(30_000, 2).expect("limits");
        let first = table
            .open(SESSION_PROTOCOL, "operator-a", 10)
            .expect("open");
        let second = table
            .open(SESSION_PROTOCOL, "operator-b", 10)
            .expect("open");
        assert_ne!(first.id(), second.id());
        assert_eq!(
            table.authorize(first.id(), "operator-b", 11),
            Err(SessionTableError::PrincipalMismatch)
        );
        table.close(first.id(), "operator-a", 11).expect("close");
        assert!(table.authorize(second.id(), "operator-b", 11).is_ok());
    }

    #[test]
    fn expiry_cannot_be_revived_by_renewal() {
        let mut table = SessionTable::with_limits(30, 1).expect("limits");
        let session = table.open(SESSION_PROTOCOL, "operator", 100).expect("open");
        assert_eq!(
            table.renew(session.id(), "operator", 130),
            Err(SessionTableError::ExpiredSession)
        );
        assert_eq!(
            table.authorize(session.id(), "operator", 131),
            Err(SessionTableError::UnknownSession)
        );
    }

    #[test]
    fn expired_capacity_is_reclaimed_without_reusing_identity() {
        let mut table = SessionTable::with_limits(10, 1).expect("limits");
        let first = table.open(SESSION_PROTOCOL, "operator", 0).expect("open");
        assert_eq!(
            table.open(SESSION_PROTOCOL, "operator", 1),
            Err(SessionTableError::CapacityExhausted)
        );
        let replacement = table
            .open(SESSION_PROTOCOL, "operator", 10)
            .expect("replace");
        assert_ne!(first.id(), replacement.id());
    }

    #[test]
    fn invalid_protocol_principal_and_wire_id_are_refused() {
        let mut table = SessionTable::default();
        assert_eq!(
            table.open("phoxal.session.v2", "operator", 0),
            Err(SessionTableError::UnsupportedProtocol)
        );
        assert_eq!(
            table.open(SESSION_PROTOCOL, "Operator", 0),
            Err(SessionTableError::InvalidPrincipal)
        );
        assert_eq!(
            SessionId::from_bytes(&[0_u8; 31]),
            Err(SessionTableError::InvalidSessionId)
        );
    }
}
