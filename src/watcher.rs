//! Lifecycle runtime. Every spawned transcript worker is joined at shutdown.
use crate::{
    Acknowledgement,
    checkpoint::CheckpointStore,
    opencode::OpencodeReader,
    scanner::*,
    tailer::{CursorStore, DeliveryMode, Tailer, WatcherRegistry, default_cursor_path},
    transcript::TranscriptReader,
};
use agent_parser::{EventData, ParsedEvent};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Harness {
    #[serde(rename = "claude-code")]
    ClaudeCode,
    #[serde(rename = "pi")]
    Pi,
    #[serde(rename = "codex")]
    Codex,
    #[serde(rename = "opencode")]
    Opencode,
}
impl std::fmt::Display for Harness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ClaudeCode => "claude-code",
            Self::Pi => "pi",
            Self::Codex => "codex",
            Self::Opencode => "opencode",
        })
    }
}
impl std::str::FromStr for Harness {
    type Err = io::Error;
    fn from_str(value: &str) -> io::Result<Self> {
        match value {
            "claude-code" => Ok(Self::ClaudeCode),
            "pi" => Ok(Self::Pi),
            "codex" => Ok(Self::Codex),
            "opencode" => Ok(Self::Opencode),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported harness: {value}"),
            )),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(flatten)]
    pub parsed: ParsedEvent,
    pub harness: Harness,
    pub harness_version: String,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subagent_id: Option<String>,
    pub pid: i32,
}
pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
pub type Handler = Arc<
    dyn for<'a> Fn(&'a CancellationToken, Event, Option<Acknowledgement>) -> HandlerFuture<'a>
        + Send
        + Sync,
>;
#[derive(Clone)]
pub struct Config {
    pub harnesses: Vec<Harness>,
    pub scan_interval: Duration,
    pub claude_sessions_dirs: Vec<PathBuf>,
    pub claude_projects_dir: PathBuf,
    pub pi_sessions_dir: PathBuf,
    pub codex_sessions_dir: PathBuf,
    pub opencode_db_path: PathBuf,
    pub cursor_path: PathBuf,
    pub opencode_checkpoint_path: PathBuf,
    pub session_state_path: PathBuf,
    pub cursor_retention: Duration,
    pub cursor_prune_interval: Duration,
    pub cursor_flush_interval: Duration,
    pub process_snapshot_provider: Arc<dyn SnapshotProvider>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            harnesses: vec![
                Harness::ClaudeCode,
                Harness::Pi,
                Harness::Codex,
                Harness::Opencode,
            ],
            scan_interval: Duration::from_secs(2),
            claude_sessions_dirs: Vec::new(),
            claude_projects_dir: PathBuf::new(),
            pi_sessions_dir: PathBuf::new(),
            codex_sessions_dir: PathBuf::new(),
            opencode_db_path: PathBuf::new(),
            cursor_path: PathBuf::new(),
            opencode_checkpoint_path: PathBuf::new(),
            session_state_path: PathBuf::new(),
            cursor_retention: Duration::from_secs(86400),
            cursor_prune_interval: Duration::from_secs(3600),
            cursor_flush_interval: Duration::from_secs(2),
            process_snapshot_provider: Arc::new(LiveProcessProvider),
        }
    }
}
#[derive(Clone)]
pub(crate) struct Session {
    pub event: Event,
    pub path: PathBuf,
    pub started_at: i64,
    pub active: bool,
    pub subagents: Vec<SubagentTranscript>,
}
struct ActiveTail {
    tailer: Arc<Tailer>,
    cancel: CancellationToken,
    session_id: String,
    harness: Harness,
}
pub(crate) struct Runner {
    pub cfg: Config,
    pub handler: Handler,
    pub cursors: Arc<CursorStore>,
    pub checkpoints: Arc<CheckpointStore>,
    pub tracker: SessionTracker,
    pub registry: Arc<WatcherRegistry>,
    pub claude: ClaudeCodeScanner,
    pub pi: PiScanner,
    pub codex: CodexScanner,
    pub opencode: OpencodeScanner,
    pub active: BTreeMap<Harness, BTreeMap<String, Session>>,
    tails: BTreeMap<PathBuf, ActiveTail>,
    subagents: BTreeSet<(String, String)>,
    workers: JoinSet<()>,
}
fn positive(value: Duration, fallback: Duration) -> Duration {
    if value.is_zero() { fallback } else { value }
}
/// Observe until cancellation. A pre-cancelled token returns `Interrupted` without I/O.
pub async fn run(cancel: &CancellationToken, mut cfg: Config, handler: Handler) -> io::Result<()> {
    if cancel.is_cancelled() {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "watcher cancelled before initialization",
        ));
    }
    let defaults = Config::default();
    if cfg.harnesses.is_empty() {
        cfg.harnesses = defaults.harnesses;
    }
    cfg.scan_interval = positive(cfg.scan_interval, defaults.scan_interval);
    cfg.cursor_retention = positive(cfg.cursor_retention, defaults.cursor_retention);
    cfg.cursor_prune_interval = positive(cfg.cursor_prune_interval, defaults.cursor_prune_interval);
    cfg.cursor_flush_interval = positive(cfg.cursor_flush_interval, defaults.cursor_flush_interval);
    if cfg.cursor_path.as_os_str().is_empty() {
        cfg.cursor_path = default_cursor_path();
    }
    if cfg.opencode_checkpoint_path.as_os_str().is_empty() {
        let mut path = cfg.cursor_path.as_os_str().to_owned();
        path.push(".opencode");
        cfg.opencode_checkpoint_path = path.into();
    }
    if cfg.session_state_path.as_os_str().is_empty() {
        cfg.session_state_path = default_session_tracker_path();
    }
    if cfg.opencode_db_path.as_os_str().is_empty() {
        cfg.opencode_db_path = default_opencode_db_path();
    }
    let cursors = Arc::new(CursorStore::open(&cfg.cursor_path)?);
    let checkpoints = Arc::new(CheckpointStore::open(&cfg.opencode_checkpoint_path)?);
    let tracker = SessionTracker::open(cfg.session_state_path.clone())?;
    let registry = WatcherRegistry::new()?;
    let mut claude = ClaudeCodeScanner::new(cfg.claude_sessions_dirs.clone());
    if !cfg.claude_projects_dir.as_os_str().is_empty() {
        claude.set_projects_dirs(vec![cfg.claude_projects_dir.clone()]);
    }
    let mut runner = Runner {
        claude,
        pi: PiScanner::new(cfg.pi_sessions_dir.clone()),
        codex: CodexScanner::new(cfg.codex_sessions_dir.clone()),
        opencode: OpencodeScanner::new(cfg.opencode_db_path.clone()),
        cfg,
        handler,
        cursors,
        checkpoints,
        tracker,
        registry,
        active: BTreeMap::new(),
        tails: BTreeMap::new(),
        subagents: BTreeSet::new(),
        workers: JoinSet::new(),
    };
    for tracked in runner
        .tracker
        .get_all()
        .into_values()
        .filter(|session| session.is_active)
    {
        let Ok(harness) = tracked.harness.parse::<Harness>() else {
            continue;
        };
        runner.active.entry(harness).or_default().insert(
            tracked.session_id.clone(),
            Session {
                event: Event {
                    parsed: ParsedEvent {
                        cwd: tracked.cwd.to_string_lossy().into(),
                        ..Default::default()
                    },
                    harness,
                    harness_version: tracked.harness_version,
                    session_id: tracked.session_id,
                    subagent_id: None,
                    pid: tracked.pid,
                },
                path: PathBuf::new(),
                started_at: tracked.started_at,
                active: true,
                subagents: Vec::new(),
            },
        );
    }
    let owned = cancel.child_token();
    runner.prune();
    let cursors = runner.cursors.clone();
    let checkpoints = runner.checkpoints.clone();
    let token = owned.clone();
    let flush_interval = runner.cfg.cursor_flush_interval;
    let prune_interval = runner.cfg.cursor_prune_interval;
    let retention = runner.cfg.cursor_retention;
    runner.workers.spawn(async move {
        let mut flush = tokio::time::interval(flush_interval);
        let mut prune = tokio::time::interval(prune_interval);
        loop { tokio::select! { biased;
            _ = token.cancelled() => break,
            _ = flush.tick() => for result in [cursors.flush(), checkpoints.flush()] { if let Err(error) = result { tracing::warn!(action="checkpoint_save_failed",%error); } },
            _ = prune.tick() => if let Err(error) = cursors.prune(retention).and_then(|_|cursors.flush()) { tracing::warn!(action="cursor_prune_failed",%error); },
        } }
    });
    runner.scan(&owned).await;
    let mut scans = tokio::time::interval(runner.cfg.scan_interval);
    scans.tick().await;
    loop {
        tokio::select! { biased;
            _ = owned.cancelled() => break,
            _ = scans.tick() => runner.scan(&owned).await,
            result = runner.workers.join_next(), if !runner.workers.is_empty() => if let Some(Err(error)) = result { tracing::warn!(action="transcript_worker_failed",%error); },
        }
    }
    owned.cancel();
    for tail in runner.tails.values() {
        tail.cancel.cancel();
        tail.tailer.stop();
    }
    while let Some(result) = runner.workers.join_next().await {
        if let Err(error) = result {
            tracing::warn!(action="transcript_worker_failed",%error);
        }
    }
    runner.registry.close();
    runner.flush();
    Ok(())
}
impl Runner {
    pub fn prune(&self) {
        if let Err(error) = self.cursors.prune(self.cfg.cursor_retention) {
            tracing::warn!(action="cursor_prune_failed",%error);
        }
    }
    fn flush(&mut self) {
        for result in [
            self.cursors.flush(),
            self.checkpoints.flush(),
            self.tracker.save(),
        ] {
            if let Err(error) = result {
                tracing::warn!(action="checkpoint_save_failed",%error);
            }
        }
    }
    pub async fn lifecycle(
        &self,
        cancel: &CancellationToken,
        mut event: Event,
        kind: &str,
    ) -> io::Result<()> {
        event.parsed.event_type = kind.into();
        (self.handler)(cancel, event, None).await
    }
    pub fn attach(&mut self, cancel: &CancellationToken, session: &Session) {
        if session.path.as_os_str().is_empty() || self.tails.contains_key(&session.path) {
            return;
        }
        let tailer = match Tailer::new(
            &session.path,
            self.cursors.clone(),
            self.registry.clone(),
            DeliveryMode::Explicit,
        ) {
            Ok(tailer) => Arc::new(tailer),
            Err(error) => {
                tracing::warn!(action="tailer_attach_failed",%error);
                return;
            }
        };
        let reader = Arc::new(tokio::sync::Mutex::new(TranscriptReader::new(
            session.event.clone(),
        )));
        let token = cancel.child_token();
        let worker_token = token.clone();
        let handler = self.handler.clone();
        let worker = tailer.clone();
        self.workers.spawn(async move {
            let result = worker
                .run(worker_token.clone(), move |line| {
                    let reader = reader.clone();
                    let handler = handler.clone();
                    let token = worker_token.clone();
                    async move { reader.lock().await.process(&token, line, &handler).await }
                })
                .await;
            if let Err(error) = result {
                tracing::warn!(action="tailer_failed",%error);
            }
        });
        self.tails.insert(
            session.path.clone(),
            ActiveTail {
                tailer,
                cancel: token,
                session_id: session.event.session_id.clone(),
                harness: session.event.harness,
            },
        );
    }
    pub fn detach(&mut self, harness: Harness, session_id: &str) {
        self.tails.retain(|_, tail| {
            let remove = tail.harness == harness && tail.session_id == session_id;
            if remove {
                tail.cancel.cancel();
                tail.tailer.stop();
            }
            !remove
        });
        self.subagents.retain(|(parent, _)| parent != session_id);
    }
    pub async fn attach_subagents(&mut self, cancel: &CancellationToken, parent: &Session) {
        for sub in &parent.subagents {
            let mut session = parent.clone();
            session.path = sub.path.clone();
            session.event.subagent_id = Some(sub.subagent_id.clone());
            session.event.parsed.data = EventData::default();
            let key = (parent.event.session_id.clone(), sub.subagent_id.clone());
            if !self.subagents.contains(&key) {
                let now = chrono::Utc::now().timestamp_millis();
                session.event.parsed.timestamp = extract_first_timestamp(&sub.path)
                    .ok()
                    .filter(|time| *time > 0 && *time < now)
                    .unwrap_or(now)
                    .max(parent.started_at);
                if let Err(error) = self
                    .lifecycle(cancel, session.event.clone(), "run_started")
                    .await
                {
                    tracing::warn!(action="subagent_lifecycle_failed",%error);
                    continue;
                }
                self.subagents.insert(key);
            }
            self.attach(cancel, &session);
        }
    }
    pub async fn poll(&self, cancel: &CancellationToken, session: &Session) -> io::Result<()> {
        OpencodeReader::new(&self.cfg.opencode_db_path, self.checkpoints.clone())
            .poll(cancel, &session.event, &self.handler)
            .await
    }
}
