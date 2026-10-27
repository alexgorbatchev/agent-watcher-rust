//! Durable OpenCode revisions; pending delivery never implies persistence.
use crate::{Acknowledgement, persistence};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Clone, Default, Deserialize, Serialize)]
struct Checkpoint {
    fingerprint: String,
    sequence: u64,
}
#[derive(Default)]
struct State {
    committed: BTreeMap<String, Checkpoint>,
    pending: BTreeMap<String, BTreeMap<String, u64>>,
    sequence: u64,
    dirty: bool,
}
pub struct CheckpointStore {
    path: PathBuf,
    state: Mutex<State>,
}
pub struct Reservation {
    pub acknowledgement: Acknowledgement,
    reject: Acknowledgement,
}
impl Reservation {
    pub fn reject(&self) -> io::Result<()> {
        self.reject.acknowledge()
    }
}
impl CheckpointStore {
    /// Unacknowledged database events must drain before a session exits.
    pub fn has_pending_session(&self, session_id: &str) -> io::Result<bool> {
        let state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let prefix = format!("{session_id}:");
        Ok(state.pending.keys().any(|key| key.starts_with(&prefix)))
    }
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let committed: BTreeMap<String, Checkpoint> = persistence::load(path.as_ref())?;
        let sequence = committed
            .values()
            .map(|entry| entry.sequence)
            .max()
            .unwrap_or(0);
        Ok(Self {
            path: path.as_ref().into(),
            state: Mutex::new(State {
                committed,
                sequence,
                ..State::default()
            }),
        })
    }
    pub fn reserve(
        self: &Arc<Self>,
        key: &str,
        fingerprint: &str,
        immutable: bool,
    ) -> io::Result<Option<Reservation>> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if state
            .committed
            .get(key)
            .is_some_and(|entry| immutable || entry.fingerprint == fingerprint)
            || state.pending.get(key).is_some_and(|pending| {
                if immutable {
                    !pending.is_empty()
                } else {
                    pending.contains_key(fingerprint)
                }
            })
        {
            return Ok(None);
        }
        state.sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("checkpoint sequence overflow"))?;
        let sequence = state.sequence;
        state
            .pending
            .entry(key.into())
            .or_default()
            .insert(fingerprint.into(), sequence);
        let store = self.clone();
        let ack_key = key.to_owned();
        let ack_fingerprint = fingerprint.to_owned();
        let acknowledgement =
            Acknowledgement::new(move || store.finish(&ack_key, &ack_fingerprint, sequence, true));
        let store = self.clone();
        let key = key.to_owned();
        let fingerprint = fingerprint.to_owned();
        Ok(Some(Reservation {
            acknowledgement,
            reject: Acknowledgement::new(move || store.finish(&key, &fingerprint, sequence, false)),
        }))
    }
    fn finish(&self, key: &str, fingerprint: &str, sequence: u64, commit: bool) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        // A rejected delivery may already have a new reservation for identical bytes.
        if state
            .pending
            .get(key)
            .and_then(|pending| pending.get(fingerprint))
            != Some(&sequence)
        {
            return Ok(());
        }
        if commit
            && state
                .committed
                .get(key)
                .is_none_or(|entry| entry.sequence < sequence)
        {
            state.committed.insert(
                key.into(),
                Checkpoint {
                    fingerprint: fingerprint.into(),
                    sequence,
                },
            );
            state.dirty = true;
        }
        if let Some(pending) = state.pending.get_mut(key) {
            pending.remove(fingerprint);
            if pending.is_empty() {
                state.pending.remove(key);
            }
        }
        Ok(())
    }
    pub fn flush(&self) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if state.dirty {
            persistence::save(&self.path, &state.committed)?;
            state.dirty = false;
        }
        Ok(())
    }
}
