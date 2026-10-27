use agent_watcher::{
    Acknowledgement, Config, Event, Handler, Harness, run,
    scanner::{ProcessMeta, ProcessSnapshot, SessionTracker, TrackedSession},
};
use serde_json::json;
use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
fn write(path: &Path, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}
fn config(root: &Path, harness: Harness, active: Arc<AtomicBool>) -> Config {
    let cwd = root.to_owned();
    let name = harness.to_string();
    Config {
        harnesses: vec![harness],
        scan_interval: Duration::from_millis(10),
        cursor_flush_interval: Duration::from_millis(5),
        cursor_prune_interval: Duration::from_millis(5),
        claude_sessions_dirs: vec![root.join("claude")],
        claude_projects_dir: root.join("projects"),
        pi_sessions_dir: root.join("pi"),
        codex_sessions_dir: root.join("codex"),
        opencode_db_path: root.join("opencode.db"),
        cursor_path: root.join("cursors.json"),
        session_state_path: root.join("sessions.json"),
        process_snapshot_provider: Arc::new(move |_: &CancellationToken| {
            Ok(ProcessSnapshot::from_processes([ProcessMeta {
                pid: 42,
                name: name.clone(),
                alive: active.load(Ordering::SeqCst),
                cwd: cwd.clone(),
                ..Default::default()
            }]))
        }),
        ..Default::default()
    }
}
fn fixture(cfg: &Config, harness: Harness) -> PathBuf {
    let cwd = cfg.cursor_path.parent().unwrap();
    let stamp = chrono::Utc::now().to_rfc3339();
    let path = match harness {
        Harness::ClaudeCode => cwd.join("transcript.jsonl"),
        Harness::Pi => cfg.pi_sessions_dir.join("project/session.jsonl"),
        Harness::Codex => cfg.codex_sessions_dir.join("2026/session.jsonl"),
        Harness::Opencode => unreachable!(),
    };
    let records = match harness {
        Harness::ClaudeCode => {
            write(&cfg.claude_sessions_dirs[0].join("session.json"),&json!({"pid":42,"sessionId":"session","cwd":cwd,"transcriptPath":path,"version":"1.2","name":"Task","nameSource":"custom"}).to_string());
            vec![
                json!({"type":"user","timestamp":stamp,"message":{"role":"user","content":"hello"}}),
                json!({"type":"assistant","timestamp":stamp,"message":{"id":"answer","content":[{"type":"text","text":"done"}],"usage":{"input_tokens":10}}}),
            ]
        }
        Harness::Pi => vec![
            json!({"type":"session","id":"session","version":3,"cwd":cwd,"timestamp":stamp}),
            json!({"type":"message","timestamp":stamp,"message":{"role":"assistant","content":[{"type":"text","text":"done"}],"stopReason":"stop","usage":{"input":10,"output":2,"totalTokens":12}}}),
        ],
        Harness::Codex => vec![
            json!({"type":"session_meta","timestamp":stamp,"payload":{"id":"session","cwd":cwd,"cli_version":"1.2","timestamp":stamp}}),
            json!({"type":"event_msg","timestamp":stamp,"payload":{"type":"task_complete","last_agent_message":"done"}}),
        ],
        Harness::Opencode => unreachable!(),
    };
    write(
        &path,
        &records
            .into_iter()
            .map(|record| format!("{record}\n"))
            .collect::<String>(),
    );
    path
}
fn collect(sender: mpsc::UnboundedSender<Event>, acknowledge: bool) -> Handler {
    Arc::new(move |_, event, ack| {
        let sender = sender.clone();
        Box::pin(async move {
            sender.send(event).map_err(io::Error::other)?;
            if acknowledge && let Some(ack) = ack {
                ack.acknowledge()?;
            }
            Ok(())
        })
    })
}
fn start(cfg: Config, handler: Handler) -> (CancellationToken, JoinHandle<io::Result<()>>) {
    let token = CancellationToken::new();
    let worker_token = token.clone();
    let worker = tokio::spawn(async move { run(&worker_token, cfg, handler).await });
    (token, worker)
}
async fn stop(token: CancellationToken, worker: JoinHandle<io::Result<()>>) {
    token.cancel();
    tokio::time::timeout(Duration::from_secs(3), worker)
        .await
        .expect("worker did not join")
        .unwrap()
        .unwrap();
}
async fn event(receiver: &mut mpsc::UnboundedReceiver<Event>, kind: &str) -> Event {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let event = receiver.recv().await.expect("watcher stopped");
            if event.parsed.event_type == kind {
                return event;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("missing {kind}"))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_transcript_harness_streams_lifecycle_and_persists_cursors() {
    for harness in [Harness::ClaudeCode, Harness::Pi, Harness::Codex] {
        let temp = tempfile::tempdir().unwrap();
        let active = Arc::new(AtomicBool::new(true));
        let cfg = config(temp.path(), harness, active.clone());
        let path = fixture(&cfg, harness);
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let (token, worker) = start(cfg.clone(), collect(sender, true));
        let started = event(&mut receiver, "run_started").await;
        assert_eq!(started.session_id, "session");
        assert_eq!(started.pid, 42);
        assert_eq!(started.parsed.cwd, temp.path().to_str().unwrap());
        if harness == Harness::ClaudeCode {
            assert_eq!(started.parsed.data.title.as_deref(), Some("Task"));
        }
        event(&mut receiver, "turn_end").await;
        active.store(false, Ordering::SeqCst);
        event(&mut receiver, "run_exited").await;
        stop(token, worker).await;
        let cursors: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&cfg.cursor_path).unwrap()).unwrap();
        assert!(
            cursors
                .as_object()
                .unwrap()
                .values()
                .any(|cursor| cursor["path"] == path.to_str().unwrap()
                    && cursor["offset"].as_u64().unwrap() > 0)
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_unacknowledged_transcripts_replay_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let cfg = config(temp.path(), Harness::Pi, Arc::new(AtomicBool::new(true)));
    fixture(&cfg, Harness::Pi);
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (token, worker) = start(cfg.clone(), collect(sender, false));
    event(&mut receiver, "turn_end").await;
    stop(token, worker).await;
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (token, worker) = start(cfg, collect(sender, true));
    event(&mut receiver, "turn_end").await;
    stop(token, worker).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claude_subagents_keep_parent_identity_and_workers_join_on_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let cfg = config(
        temp.path(),
        Harness::ClaudeCode,
        Arc::new(AtomicBool::new(true)),
    );
    fixture(&cfg, Harness::ClaudeCode);
    write(
        &temp.path().join("subagents/worker.jsonl"),
        &format!(
            "{}\n",
            json!({"type":"user","timestamp":chrono::Utc::now().to_rfc3339(),"message":{"role":"user","content":"subtask"}})
        ),
    );
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (token, worker) = start(cfg, collect(sender, true));
    let child = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let event = receiver.recv().await.unwrap();
            if event.subagent_id.is_some() && event.parsed.event_type == "turn_start" {
                break event;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(child.session_id, "session");
    assert_eq!(child.subagent_id.as_deref(), Some("worker"));
    stop(token, worker).await;
}
#[tokio::test]
async fn initialization_errors_and_pre_cancel_perform_no_writes() {
    let temp = tempfile::tempdir().unwrap();
    let cfg = config(temp.path(), Harness::Pi, Arc::new(AtomicBool::new(false)));
    let (sender, _) = mpsc::unbounded_channel();
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        run(&token, cfg.clone(), collect(sender.clone(), true))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    assert!(!cfg.cursor_path.exists());
    for (path, mut bad) in [
        (cfg.cursor_path.clone(), cfg.clone()),
        (cfg.session_state_path.clone(), cfg.clone()),
        (temp.path().join("db-checkpoint"), cfg.clone()),
    ] {
        if path.ends_with("db-checkpoint") {
            bad.opencode_checkpoint_path = path.clone();
        }
        write(&path, "invalid");
        assert!(
            run(
                &CancellationToken::new(),
                bad,
                collect(sender.clone(), true)
            )
            .await
            .is_err()
        );
        std::fs::remove_file(path).unwrap();
    }
}
const SCHEMA: &str = "CREATE TABLE session(id TEXT,project_id TEXT,parent_id TEXT,slug TEXT,directory TEXT,title TEXT,version TEXT,agent TEXT,model TEXT,cost REAL,tokens_input INTEGER DEFAULT 0,tokens_output INTEGER DEFAULT 0,tokens_reasoning INTEGER DEFAULT 0,tokens_cache_read INTEGER DEFAULT 0,tokens_cache_write INTEGER DEFAULT 0,time_created INTEGER,time_updated INTEGER,time_archived INTEGER,metadata TEXT);CREATE TABLE message(id TEXT,session_id TEXT,time_created INTEGER,data TEXT);CREATE TABLE part(id TEXT,message_id TEXT,session_id TEXT,time_created INTEGER,data TEXT);";
fn database(cfg: &Config) -> rusqlite::Connection {
    let db = rusqlite::Connection::open(&cfg.opencode_db_path).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute("INSERT INTO session(id,directory,title,version,model,time_created,time_updated)VALUES('session',?,'Task','1.2','model',1,2)",[cfg.cursor_path.parent().unwrap().to_str().unwrap()]).unwrap();
    db
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn database_revisions_and_final_completion_drain_precede_exit() {
    let temp = tempfile::tempdir().unwrap();
    let active = Arc::new(AtomicBool::new(true));
    let cfg = config(temp.path(), Harness::Opencode, active.clone());
    let db = database(&cfg);
    db.execute_batch("INSERT INTO message VALUES('assistant','session',3,'{\"role\":\"assistant\",\"tokens\":{\"input\":0},\"cost\":0}');INSERT INTO part VALUES('thought','assistant','session',3,'{\"type\":\"reasoning\",\"text\":\"working\"}');").unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let rejects = Arc::new(AtomicUsize::new(0));
    let handler: Handler = {
        let rejects = rejects.clone();
        Arc::new(move |_, event, ack: Option<Acknowledgement>| {
            let sender = sender.clone();
            let rejects = rejects.clone();
            Box::pin(async move {
                if event.parsed.event_type == "turn_end"
                    && rejects.fetch_add(1, Ordering::SeqCst) == 0
                {
                    return Err(io::Error::other("sink busy"));
                }
                sender.send(event).map_err(io::Error::other)?;
                if let Some(ack) = ack {
                    ack.acknowledge()?;
                }
                Ok(())
            })
        })
    };
    let (token, worker) = start(cfg, handler);
    assert_eq!(
        event(&mut receiver, "run_started")
            .await
            .parsed
            .data
            .title
            .as_deref(),
        Some("Task")
    );
    event(&mut receiver, "thinking").await;
    db.execute_batch("UPDATE part SET data='{\"type\":\"reasoning\",\"text\":\"harder\"}';")
        .unwrap();
    assert_eq!(
        event(&mut receiver, "thinking")
            .await
            .parsed
            .data
            .content
            .as_deref(),
        Some("harder")
    );
    db.execute_batch("UPDATE message SET data='{\"role\":\"assistant\",\"finish\":\"stop\",\"tokens\":{\"input\":10,\"output\":2,\"total\":12}}';").unwrap();
    active.store(false, Ordering::SeqCst);
    assert_eq!(
        event(&mut receiver, "turn_end")
            .await
            .parsed
            .data
            .usage
            .unwrap()
            .total,
        12
    );
    event(&mut receiver, "run_exited").await;
    assert!(rejects.load(Ordering::SeqCst) >= 2);
    stop(token, worker).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restored_shared_identifiers_exit_independently_when_database_disappears() {
    let temp = tempfile::tempdir().unwrap();
    let active = Arc::new(AtomicBool::new(false));
    let mut cfg = config(temp.path(), Harness::Pi, active);
    cfg.harnesses = vec![
        Harness::ClaudeCode,
        Harness::Pi,
        Harness::Codex,
        Harness::Opencode,
    ];
    let mut tracker = SessionTracker::open(cfg.session_state_path.clone()).unwrap();
    for harness in &cfg.harnesses {
        tracker.set(TrackedSession {
            session_id: "shared".into(),
            harness: harness.to_string(),
            is_active: true,
            ..Default::default()
        });
    }
    tracker.save().unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (token, worker) = start(cfg.clone(), collect(sender, true));
    let mut exited = std::collections::BTreeSet::new();
    for _ in 0..4 {
        let event = event(&mut receiver, "run_exited").await;
        assert_eq!(event.session_id, "shared");
        exited.insert(event.harness);
    }
    assert_eq!(exited, cfg.harnesses.iter().copied().collect());
    stop(token, worker).await;
    assert!(
        SessionTracker::open(cfg.session_state_path)
            .unwrap()
            .get_all()
            .is_empty()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_database_events_replay_after_restart_until_acknowledged() {
    let temp = tempfile::tempdir().unwrap();
    let active = Arc::new(AtomicBool::new(true));
    let cfg = config(temp.path(), Harness::Opencode, active.clone());
    let db = database(&cfg);
    db.execute_batch("INSERT INTO message VALUES('m','session',3,'{\"role\":\"assistant\"}');INSERT INTO part VALUES('p','m','session',3,'{\"type\":\"text\",\"text\":\"draft\"}');").unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (token, worker) = start(cfg.clone(), collect(sender, false));
    event(&mut receiver, "text").await;
    db.execute_batch("UPDATE message SET data='{\"role\":\"assistant\",\"finish\":\"stop\",\"tokens\":{\"input\":7}}';").unwrap();
    active.store(false, Ordering::SeqCst);
    event(&mut receiver, "turn_end").await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(60),
            event(&mut receiver, "run_exited")
        )
        .await
        .is_err(),
        "exit must wait for pending database ACKs"
    );
    stop(token, worker).await;
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (token, worker) = start(cfg.clone(), collect(sender, true));
    assert_eq!(
        event(&mut receiver, "turn_end")
            .await
            .parsed
            .data
            .usage
            .unwrap()
            .input,
        7
    );
    event(&mut receiver, "run_exited").await;
    stop(token, worker).await;
    assert!(
        SessionTracker::open(cfg.session_state_path)
            .unwrap()
            .get_all()
            .is_empty()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_rejection_and_snapshot_failure_preserve_session_state() {
    let temp = tempfile::tempdir().unwrap();
    let active = Arc::new(AtomicBool::new(true));
    let mut cfg = config(temp.path(), Harness::ClaudeCode, active.clone());
    fixture(&cfg, Harness::ClaudeCode);
    let fail_snapshot = Arc::new(AtomicBool::new(false));
    let snapshots = Arc::new(AtomicUsize::new(0));
    let cwd = temp.path().to_owned();
    cfg.process_snapshot_provider = {
        let fail = fail_snapshot.clone();
        let active = active.clone();
        let snapshots = snapshots.clone();
        Arc::new(move |_: &CancellationToken| {
            snapshots.fetch_add(1, Ordering::SeqCst);
            if fail.load(Ordering::SeqCst) {
                return Err(io::Error::other("process query failed"));
            }
            Ok(ProcessSnapshot::from_processes([ProcessMeta {
                pid: 42,
                name: "claude".into(),
                alive: active.load(Ordering::SeqCst),
                cwd: cwd.clone(),
                ..Default::default()
            }]))
        })
    };
    let starts = Arc::new(AtomicUsize::new(0));
    let exits = Arc::new(AtomicUsize::new(0));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let handler: Handler = {
        let starts = starts.clone();
        let exits = exits.clone();
        Arc::new(move |_, event, ack| {
            let starts = starts.clone();
            let exits = exits.clone();
            let sender = sender.clone();
            Box::pin(async move {
                if event.parsed.event_type == "run_started"
                    && starts.fetch_add(1, Ordering::SeqCst) == 0
                {
                    return Err(io::Error::other("start rejected"));
                }
                if event.parsed.event_type == "run_exited"
                    && exits.fetch_add(1, Ordering::SeqCst) == 0
                {
                    return Err(io::Error::other("exit rejected"));
                }
                sender.send(event).map_err(io::Error::other)?;
                if let Some(ack) = ack {
                    ack.acknowledge()?;
                }
                Ok(())
            })
        })
    };
    let (token, worker) = start(cfg.clone(), handler);
    event(&mut receiver, "turn_end").await;
    fail_snapshot.store(true, Ordering::SeqCst);
    active.store(false, Ordering::SeqCst);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            event(&mut receiver, "run_exited")
        )
        .await
        .is_err()
    );
    fail_snapshot.store(false, Ordering::SeqCst);
    event(&mut receiver, "run_exited").await;
    assert!(starts.load(Ordering::SeqCst) >= 2);
    assert!(exits.load(Ordering::SeqCst) >= 2);
    assert!(snapshots.load(Ordering::SeqCst) > 3);
    stop(token, worker).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_transcript_callbacks_are_joined() {
    let temp = tempfile::tempdir().unwrap();
    let cfg = config(
        temp.path(),
        Harness::ClaudeCode,
        Arc::new(AtomicBool::new(true)),
    );
    fixture(&cfg, Harness::ClaudeCode);
    let entered = Arc::new(tokio::sync::Notify::new());
    let finished = Arc::new(AtomicBool::new(false));
    let handler: Handler = {
        let entered = entered.clone();
        let finished = finished.clone();
        Arc::new(move |cancel, event, _| {
            let entered = entered.clone();
            let finished = finished.clone();
            Box::pin(async move {
                if event.parsed.event_type == "turn_end" {
                    entered.notify_one();
                    cancel.cancelled().await;
                    finished.store(true, Ordering::SeqCst);
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"));
                }
                Ok(())
            })
        })
    };
    let (token, worker) = start(cfg, handler);
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    stop(token, worker).await;
    assert!(finished.load(Ordering::SeqCst));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleted_database_rows_exit_without_retrying_forever() {
    let temp = tempfile::tempdir().unwrap();
    let cfg = config(
        temp.path(),
        Harness::Opencode,
        Arc::new(AtomicBool::new(true)),
    );
    let db = database(&cfg);
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (token, worker) = start(cfg, collect(sender, true));
    event(&mut receiver, "run_started").await;
    db.execute("DELETE FROM session", []).unwrap();
    event(&mut receiver, "run_exited").await;
    stop(token, worker).await;
}
#[test]
fn harness_parsing_drives_serialized_event_identity() {
    let parsed = serde_json::json!({"eventType":"thinking","timestamp":123,"cwd":"/project","gitBranch":"branch","data":{"content":"reasoning"},"harness":"codex","harnessVersion":"1","sessionId":"id","pid":42});
    let event: Event = serde_json::from_value(parsed.clone()).unwrap();
    assert_eq!(event.harness, "codex".parse().unwrap());
    assert_eq!(serde_json::to_value(event).unwrap(), parsed);
    assert!("unsupported".parse::<Harness>().is_err());
}

#[test]
fn runtime_defaults_resolve_paths_in_an_isolated_process() {
    let temp = tempfile::tempdir().unwrap();
    let cfg = config(
        temp.path(),
        Harness::ClaudeCode,
        Arc::new(AtomicBool::new(true)),
    );
    fixture(&cfg, Harness::ClaudeCode);
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "isolated_default_runtime",
            "--ignored",
            "--nocapture",
        ])
        .env("AGENT_WATCHER_RUNTIME_TEST_ROOT", temp.path())
        .env("HOME", temp.path())
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .env("CLAUDE_SESSIONS_DIR", &cfg.claude_sessions_dirs[0])
        .env("CLAUDE_PROJECTS_DIR", &cfg.claude_projects_dir)
        .env("PI_SESSIONS_DIR", &cfg.pi_sessions_dir)
        .env("CODEX_SESSIONS_DIR", &cfg.codex_sessions_dir)
        .env("OPENCODE_DB_PATH", &cfg.opencode_db_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let cursor: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            temp.path()
                .join("cache/agent-watcher/observer-cursors.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(
        cursor
            .as_object()
            .unwrap()
            .values()
            .any(|entry| entry["offset"].as_u64().unwrap() > 0)
    );
}
#[tokio::test]
#[ignore = "launched by runtime_defaults_resolve_paths_in_an_isolated_process"]
async fn isolated_default_runtime() {
    let cwd = PathBuf::from(
        std::env::var_os("AGENT_WATCHER_RUNTIME_TEST_ROOT").expect("isolated child only"),
    );
    let token = CancellationToken::new();
    let stop = token.clone();
    let handler: Handler = Arc::new(move |_, event, ack| {
        let stop = stop.clone();
        Box::pin(async move {
            if let Some(ack) = ack {
                ack.acknowledge()?;
            }
            if event.parsed.event_type == "turn_end" {
                stop.cancel();
            }
            Ok(())
        })
    });
    let cfg = Config {
        harnesses: Vec::new(),
        scan_interval: Duration::ZERO,
        cursor_retention: Duration::ZERO,
        cursor_prune_interval: Duration::ZERO,
        cursor_flush_interval: Duration::ZERO,
        process_snapshot_provider: Arc::new(move |_: &CancellationToken| {
            Ok(ProcessSnapshot::from_processes([ProcessMeta {
                pid: 42,
                name: "claude".into(),
                alive: true,
                cwd: cwd.clone(),
                ..Default::default()
            }]))
        }),
        ..Default::default()
    };
    tokio::time::timeout(Duration::from_secs(3), run(&token, cfg, handler))
        .await
        .unwrap()
        .unwrap();
}
