//! Lifecycle timestamp selection shared by harness discovery.
pub fn start_timestamp(started_at: i64, last_active: i64, now: i64) -> i64 {
    if last_active > 0 && last_active < now {
        last_active
    } else if started_at > 0 {
        started_at
    } else {
        now
    }
}

use crate::{
    scanner::*,
    watcher::{Event, Harness, Runner, Session},
};
use agent_parser::{EventData, ParsedEvent};
use std::{collections::BTreeMap, io};
use tokio_util::sync::CancellationToken;
impl Runner {
    pub async fn scan(&mut self, cancel: &CancellationToken) {
        let snapshot = match self.cfg.process_snapshot_provider.snapshot(cancel) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(action="process_snapshot_failed",%error);
                return;
            }
        };
        for harness in self.cfg.harnesses.clone() {
            if cancel.is_cancelled() {
                break;
            }
            match self.discover(cancel, harness, &snapshot) {
                Ok(sessions) => self.reconcile(cancel, harness, sessions).await,
                Err(error) => tracing::warn!(action="session_scan_failed",%harness,%error),
            }
        }
        if let Err(error) = self.tracker.save() {
            tracing::warn!(action="session_tracker_save_failed",%error);
        }
    }
    fn discover(
        &mut self,
        cancel: &CancellationToken,
        harness: Harness,
        snapshot: &ProcessSnapshot,
    ) -> io::Result<Vec<Session>> {
        let descriptors = match harness {
            Harness::ClaudeCode => self.claude.scan(cancel, snapshot)?,
            Harness::Pi => self.pi.scan(cancel, snapshot)?,
            Harness::Codex => self.codex.scan(cancel, snapshot)?,
            Harness::Opencode => self.opencode.scan(cancel, snapshot)?,
        };
        let now = chrono::Utc::now().timestamp_millis();
        Ok(descriptors
            .into_iter()
            .map(|descriptor| {
                let mut last = descriptor.last_active;
                let mut data = EventData::default();
                let mut subagents = Vec::new();
                if harness == Harness::ClaudeCode {
                    last =
                        extract_last_active_timestamp(&descriptor.transcript_path, 0).unwrap_or(0);
                    if descriptor.name_source == "custom" && !descriptor.name.is_empty() {
                        data.title = Some(descriptor.name.clone());
                    }
                    match self.claude.discover_subagents(&descriptor) {
                        Ok(found) => subagents = found,
                        Err(error) => tracing::warn!(action="subagent_scan_failed",%error),
                    }
                }
                if harness == Harness::Opencode {
                    last = 0;
                    if !descriptor.title.is_empty() {
                        data.title = Some(descriptor.title.clone());
                    }
                }
                let subagent_id = (descriptor.is_subagent
                    && ((harness == Harness::Codex && !descriptor.parent_thread_id.is_empty())
                        || (harness == Harness::Opencode && !descriptor.parent_id.is_empty())))
                .then(|| descriptor.session_id.clone());
                Session {
                    event: Event {
                        parsed: ParsedEvent {
                            timestamp: start_timestamp(descriptor.started_at, last, now),
                            cwd: descriptor.cwd.to_string_lossy().into(),
                            data,
                            ..Default::default()
                        },
                        harness,
                        harness_version: descriptor.harness_version,
                        session_id: descriptor.session_id,
                        subagent_id,
                        pid: descriptor.pid,
                    },
                    path: descriptor.transcript_path,
                    started_at: descriptor.started_at,
                    active: descriptor.is_active,
                    subagents,
                }
            })
            .collect())
    }
    async fn reconcile(
        &mut self,
        cancel: &CancellationToken,
        harness: Harness,
        sessions: Vec<Session>,
    ) {
        let previous = self.active.remove(&harness).unwrap_or_default();
        let mut next = BTreeMap::new();
        for session in sessions.into_iter().filter(|session| session.active) {
            let id = session.event.session_id.clone();
            if !previous.contains_key(&id)
                && let Err(error) = self
                    .lifecycle(cancel, session.event.clone(), "run_started")
                    .await
            {
                tracing::warn!(action="lifecycle_emit_failed",%error);
                continue;
            }
            self.tracker.set(TrackedSession {
                session_id: id.clone(),
                harness: harness.to_string(),
                pid: session.event.pid,
                cwd: session.event.parsed.cwd.clone().into(),
                harness_version: session.event.harness_version.clone(),
                started_at: session.started_at,
                is_active: true,
            });
            self.attach(cancel, &session);
            self.attach_subagents(cancel, &session).await;
            if harness == Harness::Opencode
                && let Err(error) = self.poll(cancel, &session).await
            {
                tracing::warn!(action="database_poll_failed",%error);
            }
            next.insert(id, session);
        }
        for (id, old) in previous {
            if next.contains_key(&id) {
                continue;
            }
            // Drain both inactive descriptors and vanished sessions before exit.
            if harness == Harness::Opencode
                && let Err(error) = self.poll(cancel, &old).await
                && error.kind() != io::ErrorKind::NotFound
            {
                tracing::warn!(action="database_final_drain_failed",%error);
                next.insert(id, old);
                continue;
            }
            if harness == Harness::Opencode {
                match self.checkpoints.has_pending_session(&id) {
                    Ok(false) => {}
                    Ok(true) => {
                        next.insert(id, old);
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(action="database_pending_read_failed",%error);
                        next.insert(id, old);
                        continue;
                    }
                }
            }
            let mut event = old.event.clone();
            event.parsed.timestamp = chrono::Utc::now().timestamp_millis();
            event.parsed.data = EventData::default();
            if let Err(error) = self.lifecycle(cancel, event, "run_exited").await {
                tracing::warn!(action="lifecycle_emit_failed",%error);
                next.insert(id, old);
                continue;
            }
            self.detach(harness, &id);
            self.tracker.delete(&harness.to_string(), &id);
        }
        self.active.insert(harness, next);
    }
}
