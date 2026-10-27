//! Read-only transactional database streaming with revision checkpoints.
use crate::{
    checkpoint::CheckpointStore,
    watcher::{Event, Handler},
};
use agent_parser::{
    OpencodeDb, OpencodeMessage, OpencodePart, OpencodeSession, OpencodeSessionSnapshot,
    ParsedEvent, reconstruct_session_events,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct DatabaseEvent {
    pub parsed: ParsedEvent,
    pub key: String,
    pub immutable: bool,
}
pub fn opencode_message_events(
    record: &OpencodeSession,
    message: &OpencodeMessage,
    parts: &[OpencodePart],
) -> Vec<DatabaseEvent> {
    let mut events = Vec::new();
    let mut append = |key: String, immutable, parsed: ParsedEvent| {
        if parsed.event_type != "run_started" {
            events.push(DatabaseEvent {
                key: format!("{key}:{}", parsed.event_type),
                immutable,
                parsed,
            });
        }
    };
    if message.data.role == "user" {
        for parsed in reconstruct_session_events(record, std::slice::from_ref(message), parts) {
            append(format!("message:{}", message.id), false, parsed);
        }
        return events;
    }
    let mut streaming = message.clone();
    streaming.data.tokens = None;
    streaming.data.cost = None;
    streaming.data.finish = None;
    streaming.data.error = None;
    let mut part_errors = Vec::new();
    for part in parts {
        for parsed in reconstruct_session_events(
            record,
            std::slice::from_ref(&streaming),
            std::slice::from_ref(part),
        ) {
            if parsed.event_type == "error" {
                part_errors.push(parsed.clone());
            }
            append(format!("part:{}", part.id), false, parsed);
        }
    }
    let completed = message
        .data
        .time
        .as_ref()
        .and_then(|time| time.completed)
        .is_some_and(|time| time > 0)
        || message
            .data
            .finish
            .as_ref()
            .is_some_and(|finish| !finish.is_empty());
    for parsed in reconstruct_session_events(record, std::slice::from_ref(message), parts) {
        if parsed.event_type == "turn_end" && completed {
            append(format!("message:{}", message.id), true, parsed);
        } else if parsed.event_type == "error" {
            // Remove errors already emitted from retry parts. The remaining error is
            // terminal even when another part sorts after its completion timestamp.
            if let Some(index) = part_errors
                .iter()
                .position(|part_error| part_error == &parsed)
            {
                part_errors.remove(index);
            } else {
                append(format!("message:{}", message.id), false, parsed);
            }
        }
    }
    events
}
pub struct OpencodeReader {
    path: PathBuf,
    checkpoints: Arc<CheckpointStore>,
}
impl OpencodeReader {
    pub fn new(path: impl AsRef<Path>, checkpoints: Arc<CheckpointStore>) -> Self {
        Self {
            path: path.as_ref().into(),
            checkpoints,
        }
    }
    pub async fn poll(
        &self,
        cancel: &CancellationToken,
        session: &Event,
        handler: &Handler,
    ) -> io::Result<()> {
        let snapshot = self.snapshot(cancel, &session.session_id).await?;
        self.deliver_snapshot(cancel, session, snapshot, handler)
            .await
    }
    async fn snapshot(
        &self,
        cancel: &CancellationToken,
        session_id: &str,
    ) -> io::Result<OpencodeSessionSnapshot> {
        if cancel.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "database read cancelled",
            ));
        }
        let path = self.path.clone();
        let id = session_id.to_owned();
        let token = cancel.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut worker = tokio::task::spawn_blocking(move || {
            std::fs::metadata(&path)?;
            let db = OpencodeDb::open(path).map_err(io::Error::other)?;
            let _ = sender.send(db.interrupt_handle());
            if token.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "database read cancelled",
                ));
            }
            db.session_snapshot(&id).map_err(|error| match error {
                agent_parser::Error::Sqlite(rusqlite::Error::QueryReturnedNoRows) => {
                    io::Error::new(io::ErrorKind::NotFound, "OpenCode session was deleted")
                }
                error => io::Error::other(error),
            })
        });
        let interrupt = receiver.await.ok();
        tokio::select! { biased;
            _ = cancel.cancelled() => {
                // SQLite ignores interrupts before a statement starts. Continue
                // interrupting until the blocking worker has actually joined.
                loop {
                    if let Some(interrupt) = &interrupt { interrupt.interrupt(); }
                    tokio::select! { _ = &mut worker => break, _ = tokio::time::sleep(std::time::Duration::from_millis(2)) => {} }
                }
                Err(io::Error::new(io::ErrorKind::Interrupted, "database read cancelled"))
            },
            result = &mut worker => result.map_err(io::Error::other)?,
        }
    }
    pub async fn deliver_snapshot(
        &self,
        cancel: &CancellationToken,
        session: &Event,
        mut snapshot: OpencodeSessionSnapshot,
        handler: &Handler,
    ) -> io::Result<()> {
        snapshot
            .messages
            .sort_by(|a, b| (a.time_created, &a.id).cmp(&(b.time_created, &b.id)));
        snapshot
            .parts
            .sort_by(|a, b| (a.time_created, &a.id).cmp(&(b.time_created, &b.id)));
        let mut grouped: BTreeMap<&str, Vec<OpencodePart>> = BTreeMap::new();
        for part in &snapshot.parts {
            grouped
                .entry(&part.message_id)
                .or_default()
                .push(part.clone());
        }
        for message in &snapshot.messages {
            let parts = grouped
                .get(message.id.as_str())
                .map(Vec::as_slice)
                .unwrap_or_default();
            for parsed in opencode_message_events(&snapshot.session, message, parts) {
                if cancel.is_cancelled() {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "database delivery cancelled",
                    ));
                }
                let fingerprint =
                    format!("{:x}", Sha256::digest(serde_json::to_vec(&parsed.parsed)?));
                let Some(reservation) = self.checkpoints.reserve(
                    &format!("{}:{}", session.session_id, parsed.key),
                    &fingerprint,
                    parsed.immutable,
                )?
                else {
                    continue;
                };
                let mut event = session.clone();
                event.parsed = parsed.parsed;
                if let Err(error) =
                    handler(cancel, event, Some(reservation.acknowledgement.clone())).await
                {
                    reservation.reject()?;
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}
