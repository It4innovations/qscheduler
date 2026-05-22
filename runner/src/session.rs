use crate::machine::MachineId;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Display;
use std::num::NonZeroU64;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Deserialize)]
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
}

impl TryFrom<u64> for SessionId {
    type Error = ();
    fn try_from(v: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(v).map(SessionId).ok_or(())
    }
}

impl From<NonZeroU64> for SessionId {
    fn from(v: NonZeroU64) -> Self {
        SessionId(v)
    }
}

#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionState {
    Waiting,
    Open,
    Closed,
}

pub struct SessionConfig {
    pub machine_id: MachineId,
    pub time_limit: Duration,
}

pub struct Session {
    pub id: SessionId,
    pub state: SessionState,
    pub config: SessionConfig,
}

impl Session {
    pub fn new(id: SessionId, config: SessionConfig) -> Self {
        Self {
            id,
            state: SessionState::Waiting,
            config,
        }
    }

    pub fn id(&self) -> SessionId {
        self.id
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
