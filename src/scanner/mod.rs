//! Harness session discovery using one immutable process snapshot per scan cycle.
mod claude;
mod opencode;
mod paths;
mod process;
mod timestamps;
mod tracker;
mod transcripts;
pub use claude::*;
pub use opencode::*;
pub use paths::*;
pub use process::*;
use serde::{Deserialize, Serialize};
use std::{io, path::PathBuf};
pub use timestamps::{extract_first_timestamp, extract_last_active_timestamp};
use tokio_util::sync::CancellationToken;
pub use tracker::*;
pub use transcripts::*;

/// Shared session information; timestamps are Unix milliseconds.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SessionDescriptor {
    pub pid: i32,
    pub session_id: String,
    pub cwd: PathBuf,
    pub transcript_path: PathBuf,
    pub started_at: i64,
    pub last_active: i64,
    pub updated_at: i64,
    pub harness_version: String,
    pub is_active: bool,
    pub is_subagent: bool,
    pub parent_id: String,
    pub parent_thread_id: String,
    pub agent_nickname: String,
    pub agent_role: String,
    pub title: String,
    pub name: String,
    pub name_source: String,
    pub status: String,
}

fn check_cancel(token: &CancellationToken) -> io::Result<()> {
    if token.is_cancelled() {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "session scan cancelled",
        ))
    } else {
        Ok(())
    }
}
