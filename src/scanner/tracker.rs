use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io, path::PathBuf};
/// A session identity includes its harness; identifiers are not globally unique.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SessionKey {
    pub harness: String,
    pub session_id: String,
}
impl SessionKey {
    pub fn new(harness: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self {
            harness: harness.into(),
            session_id: session_id.into(),
        }
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TrackedSession {
    pub session_id: String,
    pub harness: String,
    pub pid: i32,
    pub cwd: PathBuf,
    pub harness_version: String,
    pub started_at: i64,
    pub is_active: bool,
}
#[derive(Debug)]
pub struct SessionTracker {
    path: PathBuf,
    sessions: BTreeMap<SessionKey, TrackedSession>,
    dirty: bool,
}
impl SessionTracker {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let records: Vec<TrackedSession> = crate::persistence::load(&path)?;
        let mut sessions = BTreeMap::new();
        for session in records {
            let key = SessionKey::new(&session.harness, &session.session_id);
            if sessions.insert(key, session).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate harness/session identity in tracker",
                ));
            }
        }
        Ok(Self {
            path,
            sessions,
            dirty: false,
        })
    }
    pub fn get_all(&self) -> BTreeMap<SessionKey, TrackedSession> {
        self.sessions.clone()
    }
    pub fn set(&mut self, session: TrackedSession) {
        let key = SessionKey::new(&session.harness, &session.session_id);
        if self.sessions.get(&key) != Some(&session) {
            self.sessions.insert(key, session);
            self.dirty = true;
        }
    }
    pub fn delete(&mut self, harness: &str, id: &str) {
        if self
            .sessions
            .remove(&SessionKey::new(harness, id))
            .is_some()
        {
            self.dirty = true;
        }
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
    pub fn save(&mut self) -> io::Result<()> {
        if self.dirty {
            crate::persistence::save(&self.path, &self.sessions.values().collect::<Vec<_>>())?;
            self.dirty = false;
        }
        Ok(())
    }
}
