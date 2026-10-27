use agent_watcher::{
    Harness,
    scanner::{ProcessInfo, ProcessMeta, ProcessSnapshot},
};

fn alias() -> ProcessMeta {
    ProcessMeta {
        pid: 42,
        name: "custom-agent".into(),
        create_time: 123_000,
        alive: true,
        cwd: "/repo".into(),
        cmdline: vec!["custom-agent".into()],
    }
}

#[test]
fn explicit_candidates_preserve_metadata_and_replace_duplicate_associations() {
    let original = alias();
    let mut snapshot = ProcessSnapshot::from_processes([original.clone()]);
    assert_eq!(snapshot.processes().collect::<Vec<_>>(), vec![&original]);
    assert!(snapshot.pi_processes().is_empty());
    for harness in [Harness::Pi, Harness::Codex, Harness::Opencode] {
        snapshot
            .declare_candidate(
                harness,
                ProcessInfo {
                    pid: 42,
                    cwd: "/repo/./child/..".into(),
                    create_time: 123_000,
                },
            )
            .unwrap();
        snapshot
            .declare_candidate(
                harness,
                ProcessInfo {
                    pid: 42,
                    cwd: "/repo".into(),
                    create_time: 123_000,
                },
            )
            .unwrap();
    }
    assert_eq!(snapshot.pi_processes().len(), 1);
    assert_eq!(snapshot.codex_processes(), snapshot.pi_processes());
    assert_eq!(snapshot.opencode_processes(), snapshot.pi_processes());
    assert_eq!(
        snapshot.pi_processes()[0].cwd,
        std::path::Path::new("/repo")
    );
    assert_eq!(snapshot.process(42), Some(&original));
}

#[test]
fn declarations_reject_missing_dead_reused_and_invalid_processes() {
    let mut dead = alias();
    dead.pid = 43;
    dead.alive = false;
    let mut snapshot = ProcessSnapshot::from_processes([alias(), dead]);
    for (harness, pid, cwd, start) in [
        (Harness::Pi, 99, "/repo", 123_000),
        (Harness::Pi, 43, "/repo", 123_000),
        (Harness::Pi, 42, "/repo", 124_000),
        (Harness::Pi, 42, "", 123_000),
        (Harness::Pi, 0, "/repo", 123_000),
        (Harness::ClaudeCode, 42, "/repo", 123_000),
    ] {
        assert!(
            snapshot
                .declare_candidate(
                    harness,
                    ProcessInfo {
                        pid,
                        cwd: cwd.into(),
                        create_time: start
                    }
                )
                .is_err()
        );
    }
    assert!(snapshot.pi_processes().is_empty());
}
