`agent-watcher` discovers Claude Code, Pi, Codex, and OpenCode sessions and delivers lifecycle and parsed activity events to Rust applications. Consumers choose their storage, display, and delivery policy, then acknowledge events after delivery succeeds.

# What It Does

- **Session discovery**: Match session files and database records to running harness processes, including child sessions and Claude Code subagent transcripts.
- **Incremental activity**: Follow JSONL appends and replacements, and read OpenCode message and part revisions without modifying harness storage.
- **Durable acknowledgements**: Persist transcript byte offsets and database revisions so unacknowledged activity replays after restart.
- **Owned shutdown**: Stop and join transcript workers when the caller cancels observation.

# How It Works

1. Pass a cancellation token, `Config`, and asynchronous event handler to `run`.
2. Receive session lifecycle events and parsed activity with harness, session, subagent, and process identity.
3. Deliver each event and call its supplied acknowledgement after delivery is durable.
4. Cancel the token and await `run` so workers stop and checkpoints flush.

# How it Really Works

1. `run` observes until cancellation. Handlers for different transcripts can execute concurrently and must honor their cancellation token. Normal cancellation returns `Ok(())`; a token cancelled before initialization returns `Interrupted` without accessing storage.
2. Discovery and database polling default to two seconds. Filesystem notifications follow JSONL appends, with a ten-second polling fallback. Harness storage is read-only; only observer checkpoint and session-state files are written. The library sends no network requests and performs no work at import time.
3. Only the last event from a transcript line carries an acknowledgement, which confirms the entire line. Byte checkpoints advance through a contiguous acknowledged prefix. Database events carry individual revision acknowledgements; lifecycle events have none. OpenCode completion usage requires a completion marker and is emitted once per acknowledged assistant message.
4. A handler error preserves pending transcript parsing state or rejects the database reservation for retry. Accepted, unacknowledged events remain pending during the run. After restart, transcripts replay when their session is active again; pending final OpenCode activity drains before the session's exit event. Crashes before a checkpoint flush can cause duplicate delivery. File replacement and truncation start a new generation; callbacks from previous generations cannot commit offsets into rewritten content.
5. Byte and database checkpoints flush every two seconds and during shutdown; session state saves after discovery and at shutdown. Initial storage errors return to the caller; runtime read, handler, and persistence errors emit structured tracing diagnostics and retain retriable progress. Shutdown persistence failures remain diagnostics, so a successful return does not prove every acknowledgement reached disk. Each simultaneous observer requires separate state paths because files have no inter-process locking.

# Prerequisites

- Accessible local harness transcripts or an OpenCode SQLite database, and permission to inspect the current user's processes.
- A [Tokio](https://docs.rs/tokio/latest/tokio/) runtime for asynchronous observation.

# Installation

From a consuming Rust project beside this checkout:

```sh
cargo add agent-watcher --path ../agent-watcher
cargo add tokio --features macros,rt-multi-thread,time
cargo add tokio-util
```

This checkout has no published crate or configured Git remote.

# Quick Start

This observer prints activity for five seconds. A durable consumer acknowledges after its storage transaction or downstream delivery succeeds.

```rust,no_run
use agent_watcher::{Config, Handler, run};
use std::{io, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> io::Result<()> {
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        stop.cancel();
    });
    let handler: Handler = Arc::new(|cancel, event, acknowledge| Box::pin(async move {
        if cancel.is_cancelled() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "delivery cancelled"));
        }
        use std::io::Write;
        writeln!(io::stdout().lock(), "{} {} {}", event.harness, event.session_id, event.parsed.event_type)?;
        if let Some(acknowledge) = acknowledge {
            acknowledge.acknowledge()?;
        }
        Ok(())
    }));
    run(&cancel, Config::default(), handler).await
}
```

# Configuration

Explicit paths override environment defaults. Empty harness selection means all four harnesses; zero durations select their defaults.

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `harnesses` | `Vec<Harness>` | All four | Select `ClaudeCode`, `Pi`, `Codex`, or `Opencode`. |
| `scan_interval` | `Duration` | `2s` | Discovery and database polling period. |
| `claude_sessions_dirs` | `Vec<PathBuf>` | Harness paths | `$CLAUDE_SESSIONS_DIR`, ai-registry sessions under the data directory, and `~/.claude/sessions`. |
| `claude_projects_dir` | `PathBuf` | Harness paths | `$CLAUDE_PROJECTS_DIR`, ai-registry projects under the data directory, and `~/.claude/projects`. |
| `pi_sessions_dir` | `PathBuf` | Harness path | `$PI_SESSIONS_DIR`, `$PI_CODING_AGENT_DIR/sessions`, or `~/.pi/agent/sessions`. |
| `codex_sessions_dir` | `PathBuf` | Harness path | `$CODEX_SESSIONS_DIR`, `$CODEX_HOME/sessions`, or `~/.codex/sessions`. |
| `opencode_db_path` | `PathBuf` | Harness path | `$OPENCODE_DB_PATH`, or `opencode/opencode.db` beneath the data directory. |
| `cursor_path` | `PathBuf` | Observer cache | `observer-cursors.json` beneath `$XDG_CACHE_HOME/agent-watcher` or `~/.cache/agent-watcher`. |
| `opencode_checkpoint_path` | `PathBuf` | Cursor path + `.opencode` | Acknowledged database revisions. |
| `session_state_path` | `PathBuf` | Observer cache | `observer-sessions.json` for exit detection across restarts. |
| `cursor_retention` | `Duration` | `24h` | Retention for inaccessible transcripts; matching existing files retain progress regardless of age. |
| `cursor_prune_interval` | `Duration` | `1h` | Prune deleted or replaced transcript cursors. |
| `cursor_flush_interval` | `Duration` | `2s` | Flush byte cursors and database revisions. |
| `process_snapshot_provider` | `Arc<dyn SnapshotProvider>` | Live processes | Supply one immutable process snapshot per discovery tick. |

Custom providers can iterate captured `ProcessSnapshot::processes()` and construct a filtered snapshot with `from_processes`. `declare_candidate(harness, ProcessInfo)` associates Pi, Codex, or OpenCode with an explicitly selected alias/interpreter process. It requires a captured live PID, its exact captured start time, and a working directory; repeated declarations replace that PID's candidate without altering actual process metadata. Claude Code uses PID descriptors instead. Cancellation and process-session filtering remain the provider's responsibility.

The data directory is `$XDG_DATA_HOME` or `~/.local/share`. The ai-registry paths are `ai-registry/claude-code/sessions` and `ai-registry/claude-code/projects` beneath it. Logging uses the application's tracing subscriber.

# API

| Export | Purpose |
| :--- | :--- |
| `run(&CancellationToken, Config, Handler)` | Observe sessions asynchronously and return `io::Result<()>`. |
| `Event` | Flatten `agent-parser` activity fields into serialized events with harness and session identity. |
| `Acknowledgement::acknowledge()` | Confirm delivery; cloned callbacks share checkpoint state. |
| `scanner` | Access native process snapshots, harness discovery, timestamp extraction, and persisted session tracking. |
| `tailer` | Access byte cursors, filesystem subscriptions, and explicit or automatic line acknowledgement. |
| `transcript::TranscriptReader` | Parse lines with backpressure and whole-line acknowledgements. |
| `checkpoint::CheckpointStore` | Reserve and acknowledge database revisions. |
| `opencode::OpencodeReader` | Deliver transactional read-only database snapshots. |

# Compatibility

Native file identities use Unix device and inode numbers; other platforms fall back to paths and prefix/size checks. Replacement detection is consequently weaker outside Unix. Paths persisted as JSON must be representable as Unicode. Native process and file notification behavior is verified on Linux; live harness installations and other platforms require separate verification.

Malformed checkpoint and session-state JSON returns an error. Inferred Claude transcript and subagent paths reject session identifiers containing path separators. Timestamp scanning rejects non-finite numbers and resumes after oversized records only at a newline boundary. Cursor timestamp refreshes are persisted even when the offset stays unchanged.

Session lifecycle state is a JSON array of tracked sessions, ordered by harness and session ID. Those two fields form the identity: equal session IDs in different harnesses remain separate across restarts and exits. Duplicate identities are rejected. The Rust session-state format is independent of the Go tracker's map format.

Native watch registration and removal release subscription state before waiting for the notifier thread, so incoming file events can finish while watches change.

# License

[MIT](LICENSE), copyright 2026 Alex Gorbatchev.
