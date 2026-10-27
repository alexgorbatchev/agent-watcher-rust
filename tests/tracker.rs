use agent_watcher::scanner::{SessionKey, SessionTracker, TrackedSession};

#[test]
fn shared_session_ids_remain_independent_across_harnesses_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sessions.json");
    let mut tracker = SessionTracker::open(path.clone()).unwrap();
    for (harness, pid) in [("pi", 42), ("codex", 43)] {
        tracker.set(TrackedSession {
            session_id: "shared".into(),
            harness: harness.into(),
            pid,
            is_active: true,
            ..Default::default()
        });
    }
    tracker.save().unwrap();
    let mut restored = SessionTracker::open(path.clone()).unwrap();
    assert_eq!(restored.get_all().len(), 2, "one harness overwrote another");
    assert_eq!(restored.get_all()[&SessionKey::new("pi", "shared")].pid, 42);
    assert_eq!(
        restored.get_all()[&SessionKey::new("codex", "shared")].pid,
        43
    );
    restored.delete("pi", "shared");
    restored.save().unwrap();
    let records = SessionTracker::open(path).unwrap().get_all();
    assert_eq!(records.len(), 1);
    assert_eq!(records[&SessionKey::new("codex", "shared")].pid, 43);
}

#[test]
fn duplicate_composite_identities_are_rejected_instead_of_silently_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sessions.json");
    let record = TrackedSession {
        session_id: "a".into(),
        harness: "pi".into(),
        ..Default::default()
    };
    std::fs::write(
        &path,
        serde_json::to_vec(&[record.clone(), record]).unwrap(),
    )
    .unwrap();
    assert_eq!(
        SessionTracker::open(path).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}
