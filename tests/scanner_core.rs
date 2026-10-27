use agent_watcher::scanner::*;
use std::{fs, path::PathBuf};
use tokio_util::sync::CancellationToken;

fn fixture() -> tempfile::TempDir {
    fs::create_dir_all(".tmp").unwrap();
    tempfile::tempdir_in(".tmp").unwrap()
}
fn process(pid: i32, name: &str, cwd: &str, time: i64) -> ProcessMeta {
    ProcessMeta {
        pid,
        name: name.into(),
        cwd: cwd.into(),
        create_time: time,
        alive: true,
        cmdline: vec![],
    }
}
#[test]
fn snapshot_classifies_native_and_interpreter_processes_without_pid_reuse() {
    let mut wrapped = process(2, "node", "/repo/./child/..", 1_700_000_000_000);
    wrapped.cmdline = vec!["node".into(), "/opt/bin/codex".into()];
    let snap = ProcessSnapshot::from_processes([
        process(1, "pi", "/repo", 1_700_000_000_000),
        wrapped,
        process(3, "python", "/repo", 0),
    ]);
    assert_eq!(snap.pi_processes()[0].pid, 1);
    assert_eq!(snap.codex_processes()[0].cwd, PathBuf::from("/repo"));
    assert!(snap.verify_process_match(1, 1_700_000_059_999));
    assert!(!snap.verify_process_match(1, 1_700_000_060_000));
    assert!(!snap.is_process_alive(0));
    assert!(snap.opencode_processes().is_empty());
}
#[test]
fn pi_and_codex_prioritize_recent_sessions_and_resumes() {
    let dir = fixture();
    let project = dir.path().join("project");
    fs::create_dir(&project).unwrap();
    fs::write(
        project.join("old.jsonl"),
        "{\"type\":\"session\",\"id\":\"old\",\"cwd\":\"/repo\",\"timestamp\":1700000000000}\n",
    )
    .unwrap();
    fs::write(project.join("new.jsonl"),"{\"type\":\"session\",\"id\":\"new\",\"cwd\":\"/repo\",\"timestamp\":1700000000000}\n{\"type\":\"message\",\"timestamp\":1700000600000}\n{\"type\":\"last-prompt\",\"timestamp\":1900000000000}\n").unwrap();
    let snap = ProcessSnapshot::from_processes([process(10, "pi", "/repo", 1_700_000_600_000)]);
    let mut scanner = PiScanner::new(dir.path().into());
    let token = CancellationToken::new();
    let sessions = scanner.scan(&token, &snap).unwrap();
    let active = sessions.iter().find(|s| s.is_active).unwrap();
    assert_eq!(active.session_id, "new");
    assert_eq!(active.last_active, 1_700_000_600_000);
    assert_eq!(active.pid, 10);
    fs::remove_file(project.join("new.jsonl")).unwrap();
    assert_eq!(scanner.scan(&token, &snap).unwrap().len(), 1);
    fs::write(project.join("old.jsonl"), "invalid\n").unwrap();
    assert!(scanner.scan(&token, &snap).unwrap().is_empty());
    fs::write(project.join("codex.jsonl"),"{\"type\":\"session_meta\",\"timestamp\":1700000000000,\"payload\":{\"id\":\"child\",\"cwd\":\"/repo\",\"parent_thread_id\":\"parent\",\"cli_version\":\"1\"}}\n").unwrap();
    let sessions = CodexScanner::new(dir.path().into())
        .scan(&token, &ProcessSnapshot::default())
        .unwrap();
    assert_eq!(sessions[0].parent_thread_id, "parent");
    assert!(sessions[0].is_subagent);
    token.cancel();
    assert_eq!(
        scanner.scan(&token, &snap).unwrap_err().kind(),
        std::io::ErrorKind::Interrupted
    );
}
#[test]
fn tracker_round_trips_and_refuses_corrupt_persistence() {
    let dir = fixture();
    let path = dir.path().join("nested/sessions.json");
    let mut tracker = SessionTracker::open(path.clone()).unwrap();
    assert!(!tracker.is_dirty());
    let session = TrackedSession {
        session_id: "a".into(),
        pid: 42,
        ..Default::default()
    };
    tracker.set(session.clone());
    assert!(tracker.is_dirty());
    tracker.save().unwrap();
    tracker.set(session);
    assert!(!tracker.is_dirty());
    assert_eq!(
        SessionTracker::open(path.clone()).unwrap().get_all()[&SessionKey::new("", "a")].pid,
        42
    );
    tracker.delete("", "a");
    tracker.save().unwrap();
    assert!(
        SessionTracker::open(path.clone())
            .unwrap()
            .get_all()
            .is_empty()
    );
    fs::write(&path, "{broken").unwrap();
    assert_eq!(
        SessionTracker::open(path).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}
#[test]
fn timestamps_ignore_markers_accept_nested_units_and_bounded_large_records() {
    let dir = fixture();
    let path = dir.path().join("session.jsonl");
    fs::write(&path,"{\"type\":\"session\",\"startedAt\":1700000000}\n{\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"time\":{\"completed\":1700000010000000}}}\n{\"type\":\"event_msg\",\"timestamp\":1900000000000,\"payload\":{\"type\":\"token_count\"}}\ninvalid\n").unwrap();
    assert_eq!(extract_first_timestamp(&path).unwrap(), 1_700_000_000_000);
    // Nested timestamp containers are one level deep, matching the harness contract.
    assert_eq!(extract_last_active_timestamp(&path, 123).unwrap(), 123);
    fs::write(
        &path,
        "{\"type\":\"message\",\"data\":{\"completed\":1700000010000000}}\n",
    )
    .unwrap();
    assert_eq!(
        extract_last_active_timestamp(&path, 123).unwrap(),
        1_700_000_010_000
    );
    let mut content = b"{\"type\":\"user\",\"timestamp\":1700000000000}\n".to_vec();
    content.extend(vec![b'x'; 9 * 1024 * 1024]);
    content.extend_from_slice(b"\n{\"type\":\"last-prompt\",\"timestamp\":1900000000000}\n");
    fs::write(&path, content).unwrap();
    assert_eq!(
        extract_last_active_timestamp(&path, 123).unwrap(),
        1_700_000_000_000
    );
}

#[test]
fn failed_tracker_save_retains_dirty_state_and_all_scanners_cancel() {
    let dir = fixture();
    let blocked = dir.path().join("blocked");
    fs::create_dir(&blocked).unwrap();
    let mut tracker = SessionTracker::open(blocked.join("sessions.json")).unwrap();
    fs::remove_dir(&blocked).unwrap();
    fs::write(&blocked, "file").unwrap();
    tracker.set(TrackedSession {
        session_id: "retry".into(),
        ..Default::default()
    });
    assert!(tracker.save().is_err());
    assert!(tracker.is_dirty());
    fs::remove_file(&blocked).unwrap();
    tracker.save().unwrap();
    assert!(!tracker.is_dirty());
    tracker.delete("", "missing");
    assert!(!tracker.is_dirty());
    tracker.save().unwrap();
    let token = CancellationToken::new();
    token.cancel();
    let snapshot = ProcessSnapshot::default();
    assert!(
        ClaudeCodeScanner::new(vec![dir.path().into()])
            .scan(&token, &snapshot)
            .is_err()
    );
    assert!(
        CodexScanner::new(dir.path().into())
            .scan(&token, &snapshot)
            .is_err()
    );
    assert!(
        OpencodeScanner::new(dir.path().join("missing"))
            .scan(&token, &snapshot)
            .is_err()
    );
}
