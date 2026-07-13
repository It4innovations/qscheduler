use crate::machine::MachineId;
use crate::project::ProjectId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::num::NonZeroU64;
use std::time::Duration;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct SessionId(NonZeroU64);

impl Default for SessionId {
    fn default() -> Self {
        Self(NonZeroU64::MIN)
    }
}

impl Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl SessionId {
    pub fn bump(&mut self) -> SessionId {
        let result = SessionId(self.0);
        self.0 = NonZeroU64::new(self.0.get() + 1).unwrap();
        result
    }

    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.0.get()
    }

    #[inline]
    pub fn as_i64(&self) -> i64 {
        self.0.get() as i64
    }
}

impl TryFrom<u64> for SessionId {
    type Error = ();
    fn try_from(v: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(v).map(SessionId).ok_or(())
    }
}

impl TryFrom<i64> for SessionId {
    type Error = ();
    fn try_from(v: i64) -> Result<Self, Self::Error> {
        let v = u64::try_from(v).map_err(|_| ())?;
        NonZeroU64::new(v).map(SessionId).ok_or(())
    }
}

impl From<NonZeroU64> for SessionId {
    fn from(v: NonZeroU64) -> Self {
        SessionId(v)
    }
}

#[derive(Clone, Copy)]
pub enum SessionState {
    Waiting,
    Open { opened_at: DateTime<Utc> },
    Closed { opened_at: Option<DateTime<Utc>>, closed_at: DateTime<Utc> },
}

impl SessionState {
    pub fn close(&mut self, closed_at: DateTime<Utc>) {
        let opened_at = match self {
            SessionState::Waiting => None,
            SessionState::Open { opened_at } => Some(*opened_at),
            SessionState::Closed { .. } => unreachable!(),
        };
        *self = SessionState::Closed { opened_at, closed_at };
    }
}

/// The current state of a session, as a plain string (no embedded data).
#[derive(Clone, Copy, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SessionStateKind {
    /// Session is queued and waiting for the machine to become available.
    Waiting,
    /// Session is open and ready to accept tasks.
    Open,
    /// Session has ended and no longer accepts tasks.
    Closed,
}

impl From<&SessionState> for SessionStateKind {
    fn from(s: &SessionState) -> Self {
        match s {
            SessionState::Waiting => Self::Waiting,
            SessionState::Open { .. } => Self::Open,
            SessionState::Closed { .. } => Self::Closed,
        }
    }
}

pub struct SessionConfig {
    pub machine_id: MachineId,
    pub project_id: ProjectId,
    pub time_limit: Duration,
}

pub struct Session {
    pub id: SessionId,
    pub state: SessionState,
    pub consumed: Duration,
    pub created_at: DateTime<Utc>,
    pub config: SessionConfig,
}

impl Session {
    pub fn new(id: SessionId, created_at: DateTime<Utc>, config: SessionConfig) -> Self {
        Self {
            id,
            state: SessionState::Waiting,
            consumed: Duration::ZERO,
            created_at,
            config,
        }
    }

    pub fn id(&self) -> SessionId {
        self.id
    }
}

/// A snapshot of a session's public-facing data, resolved names and all — the shape returned
/// by `GET /sessions/{id}` and embedded in the session open/close callback.
#[derive(Clone, Serialize, utoipa::ToSchema)]
pub struct SessionInfo {
    pub id: u64,
    pub state: SessionStateKind,
    /// Name of the reserved machine.
    pub machine: String,
    /// Name of the project the session's time is charged to.
    pub project: String,
    pub time_limit_ms: i64,
    pub created_at: DateTime<Utc>,
    /// Absent if the session never opened (still waiting, or closed while still queued).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub opened_at: Option<DateTime<Utc>>,
    /// Absent unless the session has closed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at: Option<DateTime<Utc>>,
    /// Milliseconds of execution time consumed by tasks run during this session, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exectime_ms: Option<i64>,
}

impl SessionInfo {
    pub(crate) fn build(session: &Session, machine: String, project: String) -> SessionInfo {
        let (opened_at, closed_at) = match session.state {
            SessionState::Waiting => (None, None),
            SessionState::Open { opened_at } => (Some(opened_at), None),
            SessionState::Closed {
                opened_at,
                closed_at,
            } => (opened_at, Some(closed_at)),
        };
        SessionInfo {
            id: session.id().as_u64(),
            state: SessionStateKind::from(&session.state),
            machine,
            project,
            time_limit_ms: session.config.time_limit.as_millis() as i64,
            created_at: session.created_at,
            opened_at,
            closed_at,
            exectime_ms: crate::task::duration_ms(session.consumed),
        }
    }
}

#[derive(Default)]
pub(crate) struct SessionMap(HashMap<SessionId, Session>);

impl SessionMap {
    // #[inline]
    // pub fn get_session(&self, session_id: SessionId) -> &Session {
    //     self.0.get(&session_id).unwrap()
    // }

    #[inline]
    pub fn get_session_mut(&mut self, session_id: SessionId) -> &mut Session {
        self.0.get_mut(&session_id).unwrap()
    }

    #[inline]
    pub fn find_session(&self, session_id: SessionId) -> Option<&Session> {
        self.0.get(&session_id)
    }

    #[inline]
    pub fn find_session_mut(&mut self, session_id: SessionId) -> Option<&mut Session> {
        self.0.get_mut(&session_id)
    }

    #[inline]
    pub fn insert(&mut self, session: Session) {
        let session_id = session.id;
        self.0.insert(session_id, session);
    }
}
