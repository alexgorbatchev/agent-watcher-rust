use agent_watcher::scanner::*;
use rusqlite::{Connection, params};
use serde_json::json;
use std::{
    fs, io,
    process::{Command, Stdio},
    time::Duration,
};
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
        ..Default::default()
    }
}

#[test]
fn claude_deduplicates_valid_pids_locates_project_transcripts_and_subagents() {
    let dir = fixture();
    let sessions = dir.path().join("sessions");
    let projects = dir.path().join("projects");
    fs::create_dir_all(&sessions).unwrap();
    fs::create_dir_all(projects.join("-repo-a-b/valid/subagents")).unwrap();
    let transcript = projects.join("-repo-a-b/valid.jsonl");
    fs::write(&transcript, "{}\n").unwrap();
    fs::write(
        projects.join("-repo-a-b/valid/subagents/worker.jsonl"),
        "{}\n",
    )
    .unwrap();
    fs::write(
        sessions.join("a-stale.json"),
        json!({"pid":42,"sessionId":"stale","startedAt":1700000900000i64}).to_string(),
    )
    .unwrap();
    fs::write(sessions.join("b-valid.json"),json!({"pid":42,"sessionId":"valid","cwd":"/repo/a.b","startedAt":1700000000000i64,"version":"2.1","name":"custom","nameSource":"custom"}).to_string()).unwrap();
    fs::write(
        sessions.join("c-duplicate.json"),
        json!({"pid":42,"sessionId":"duplicate"}).to_string(),
    )
    .unwrap();
    fs::write(
        sessions.join("dead.json"),
        json!({"pid":90,"sessionId":"dead"}).to_string(),
    )
    .unwrap();
    fs::write(sessions.join("invalid.json"), "broken").unwrap();
    fs::write(sessions.join("ignored.key"), "key").unwrap();
    let snapshot = ProcessSnapshot::from_processes([process(42, "claude", "", 1700000000000)]);
    let mut scanner = ClaudeCodeScanner::new(vec![sessions]);
    scanner.set_projects_dirs(vec![projects]);
    let found = scanner.scan(&CancellationToken::new(), &snapshot).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].session_id, "valid");
    assert_eq!(found[0].harness_version, "2.1");
    assert_eq!(found[0].name, "custom");
    assert_eq!(
        found[0].transcript_path,
        fs::canonicalize(&transcript).unwrap()
    );
    let children = scanner.discover_subagents(&found[0]).unwrap();
    assert_eq!(children[0].subagent_id, "worker");
    assert!(
        scanner
            .discover_subagents(&SessionDescriptor::default())
            .unwrap()
            .is_empty()
    );
}
#[test]
fn claude_project_fallback_and_unsafe_identifier_do_not_escape_roots() {
    let dir = fixture();
    let projects = dir.path().join("projects");
    fs::create_dir_all(projects.join("unrelated")).unwrap();
    fs::write(projects.join("unrelated/lookup.jsonl"), "{}").unwrap();
    fs::write(
        dir.path().join("a.json"),
        json!({"pid":1,"sessionId":"lookup","cwd":"/else"}).to_string(),
    )
    .unwrap();
    fs::write(
        dir.path().join("b.json"),
        json!({"pid":2,"sessionId":"../secret","cwd":"/else"}).to_string(),
    )
    .unwrap();
    let mut scanner = ClaudeCodeScanner::new(vec![dir.path().into()]);
    scanner.set_projects_dirs(vec![projects]);
    let sessions = scanner
        .scan(
            &CancellationToken::new(),
            &ProcessSnapshot::from_processes([
                process(1, "claude", "", 0),
                process(2, "claude", "", 0),
            ]),
        )
        .unwrap();
    assert!(!sessions[0].transcript_path.as_os_str().is_empty());
    assert!(sessions[1].transcript_path.as_os_str().is_empty());
}
#[test]
fn sqlite_sessions_pair_only_top_level_and_descendants_inherit_without_cycles() {
    let dir = fixture();
    let path = dir.path().join("opencode.db");
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TABLE session(id TEXT PRIMARY KEY,project_id TEXT,parent_id TEXT,slug TEXT,directory TEXT,title TEXT,version TEXT,agent TEXT,model TEXT,cost REAL,tokens_input INTEGER,tokens_output INTEGER,tokens_reasoning INTEGER,tokens_cache_read INTEGER,tokens_cache_write INTEGER,time_created INTEGER,time_updated INTEGER,time_archived INTEGER,metadata TEXT);").unwrap();
    for (id, parent, created, updated, archived) in [
        ("root", None, 1700000000000i64, 1700000600000i64, None),
        ("old", None, 1699990000000, 1699990000000, None),
        ("child", Some("root"), 1700000001000, 1700000700000, None),
        (
            "grandchild",
            Some("child"),
            1700000002000,
            1700000800000,
            None,
        ),
        (
            "archived",
            Some("root"),
            1700000002000,
            1700000900000,
            Some(1700000900000i64),
        ),
        (
            "orphan",
            Some("missing"),
            1700000002000,
            1700000800000,
            None,
        ),
        ("cycle1", Some("cycle2"), 1700000002000, 1700000800000, None),
        ("cycle2", Some("cycle1"), 1700000002000, 1700000800000, None),
    ] {
        connection.execute("INSERT INTO session VALUES (?1,'project',?2,?1,'/repo',?1,'1','default','{}',0,0,0,0,0,0,?3,?4,?5,'{}')",params![id,parent,created,updated,archived]).unwrap();
    }
    let snapshot =
        ProcessSnapshot::from_processes([process(20, "opencode", "/repo", 1700000600000)]);
    let scanner = OpencodeScanner::new(path);
    let found = scanner.scan(&CancellationToken::new(), &snapshot).unwrap();
    for id in ["root", "child", "grandchild"] {
        let session = found.iter().find(|s| s.session_id == id).unwrap();
        assert!(session.is_active, "{id}");
        assert_eq!(session.pid, 20);
    }
    for id in ["old", "archived", "orphan", "cycle1", "cycle2"] {
        assert!(
            !found.iter().find(|s| s.session_id == id).unwrap().is_active,
            "{id}"
        );
    }
    assert!(
        OpencodeScanner::new(dir.path().join("missing"))
            .scan(&CancellationToken::new(), &snapshot)
            .unwrap()
            .is_empty()
    );
    fs::write(dir.path().join("invalid.db"), "broken").unwrap();
    assert!(
        OpencodeScanner::new(dir.path().join("invalid.db"))
            .scan(&CancellationToken::new(), &snapshot)
            .is_err()
    );
}
#[test]
fn cache_reuses_unchanged_headers_and_invalidates_inode_replacement() {
    let dir = fixture();
    let project = dir.path().join("project");
    fs::create_dir(&project).unwrap();
    let path = project.join("session.jsonl");
    let original=json!({"type":"session","id":"original","cwd":"/repo","version":3,"timestamp":"2026-01-01T00:00:00Z"}).to_string()+"\n";
    let changed = original.replace("original", "modified");
    fs::write(&path, &original).unwrap();
    let stamp = fs::metadata(&path).unwrap().modified().unwrap();
    let snapshot = ProcessSnapshot::default();
    let token = CancellationToken::new();
    let mut scanner = PiScanner::new(dir.path().into());
    assert_eq!(
        scanner.scan(&token, &snapshot).unwrap()[0].session_id,
        "original"
    );
    fs::write(&path, &changed).unwrap();
    fs::File::open(&path).unwrap().set_modified(stamp).unwrap();
    assert_eq!(
        scanner.scan(&token, &snapshot).unwrap()[0].session_id,
        "original"
    );
    let replacement = project.join("replacement");
    fs::write(&replacement, &changed).unwrap();
    fs::File::open(&replacement)
        .unwrap()
        .set_modified(stamp)
        .unwrap();
    fs::rename(&replacement, &path).unwrap();
    assert_eq!(
        scanner.scan(&token, &snapshot).unwrap()[0].session_id,
        "modified"
    );
    fs::remove_dir_all(&project).unwrap();
    assert!(scanner.scan(&token, &snapshot).unwrap().is_empty());
    fs::write(&project, "file").unwrap();
    assert!(PiScanner::new(project).scan(&token, &snapshot).is_err());
}
#[test]
fn live_snapshot_observes_real_named_process_and_cancellation() {
    let dir = fixture();
    let source = dir.path().join("fixture.rs");
    let executable = dir.path().join("pi");
    fs::write(
        &source,
        "fn main(){std::thread::sleep(std::time::Duration::from_secs(30));}",
    )
    .unwrap();
    assert!(
        Command::new("rustc")
            .arg(&source)
            .arg("-o")
            .arg(&executable)
            .status()
            .unwrap()
            .success()
    );
    let mut child = Command::new(fs::canonicalize(executable).unwrap())
        .current_dir(dir.path())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let token = CancellationToken::new();
    let result = (|| -> io::Result<()> {
        for _ in 0..20 {
            let snapshot = ProcessSnapshot::capture(&token)?;
            if let Some(p) = snapshot
                .pi_processes()
                .iter()
                .find(|p| p.pid == child.id() as i32)
            {
                assert_eq!(p.cwd, fs::canonicalize(dir.path())?);
                assert!(p.create_time > 0);
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(io::Error::other("named fixture missing from real snapshot"))
    })();
    let _ = child.kill();
    let _ = child.wait();
    result.unwrap();
    token.cancel();
    assert_eq!(
        ProcessSnapshot::capture(&token).unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    let provider = LiveProcessProvider;
    assert!(provider.snapshot(&token).is_err());
    let provider = |_: &CancellationToken| Ok(ProcessSnapshot::default());
    assert!(
        provider
            .snapshot(&CancellationToken::new())
            .unwrap()
            .pi_processes()
            .is_empty()
    );
}

#[test]
fn default_paths_resolve_environment_in_isolated_processes() {
    for mode in ["custom", "fallback"] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--ignored", "--exact", "default_path_fixture"])
            .env("SCANNER_PATH_FIXTURE", mode)
            .env("HOME", "/fixture/home")
            .env_remove("CLAUDE_SESSIONS_DIR")
            .env_remove("CLAUDE_PROJECTS_DIR")
            .env_remove("PI_SESSIONS_DIR")
            .env_remove("PI_CODING_AGENT_DIR")
            .env_remove("CODEX_SESSIONS_DIR")
            .env_remove("CODEX_HOME")
            .env_remove("OPENCODE_DB_PATH")
            .env_remove("XDG_DATA_HOME")
            .env_remove("XDG_CACHE_HOME");
        if mode == "custom" {
            command
                .env("CLAUDE_SESSIONS_DIR", "/custom/claude")
                .env("CLAUDE_PROJECTS_DIR", "/custom/projects")
                .env("PI_CODING_AGENT_DIR", "/custom/pi")
                .env("CODEX_HOME", "/custom/codex")
                .env("XDG_DATA_HOME", "/custom/data")
                .env("XDG_CACHE_HOME", "/custom/cache");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    let dir = fixture();
    let project = dir.path().join("project");
    fs::create_dir(&project).unwrap();
    fs::write(
        project.join("pi.jsonl"),
        json!({"type":"session","id":"from-default"}).to_string(),
    )
    .unwrap();
    fs::write(
        dir.path().join("codex.jsonl"),
        json!({"type":"session_meta","payload":{"id":"from-default"}}).to_string(),
    )
    .unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "default_path_fixture"])
        .env("SCANNER_PATH_FIXTURE", "constructor")
        .env("PI_SESSIONS_DIR", fs::canonicalize(dir.path()).unwrap())
        .env("CODEX_SESSIONS_DIR", fs::canonicalize(dir.path()).unwrap())
        .env("OPENCODE_DB_PATH", dir.path().join("absent.db"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}
#[test]
#[ignore = "subprocess fixture with an isolated environment"]
fn default_path_fixture() {
    let mode = std::env::var("SCANNER_PATH_FIXTURE").unwrap();
    if mode == "constructor" {
        let token = CancellationToken::new();
        let snapshot = ProcessSnapshot::default();
        assert_eq!(
            PiScanner::new(std::path::PathBuf::new())
                .scan(&token, &snapshot)
                .unwrap()[0]
                .session_id,
            "from-default"
        );
        assert_eq!(
            CodexScanner::new(std::path::PathBuf::new())
                .scan(&token, &snapshot)
                .unwrap()[0]
                .session_id,
            "from-default"
        );
        assert!(
            OpencodeScanner::new(std::path::PathBuf::new())
                .scan(&token, &snapshot)
                .unwrap()
                .is_empty()
        );
        return;
    }
    if mode == "custom" {
        assert_eq!(
            default_claude_session_dirs()[0],
            std::path::PathBuf::from("/custom/claude")
        );
        assert_eq!(
            default_claude_projects_dirs()[0],
            std::path::PathBuf::from("/custom/projects")
        );
        assert_eq!(
            default_pi_sessions_dir(),
            std::path::PathBuf::from("/custom/pi/sessions")
        );
        assert_eq!(
            default_codex_sessions_dir(),
            std::path::PathBuf::from("/custom/codex/sessions")
        );
        assert_eq!(
            default_opencode_db_path(),
            std::path::PathBuf::from("/custom/data/opencode/opencode.db")
        );
        assert_eq!(
            default_session_tracker_path(),
            std::path::PathBuf::from("/custom/cache/agent-watcher/observer-sessions.json")
        );
    } else {
        assert_eq!(
            default_claude_session_dirs()[0],
            std::path::PathBuf::from("/fixture/home/.local/share/ai-registry/claude-code/sessions")
        );
        assert_eq!(
            default_claude_projects_dirs()[0],
            std::path::PathBuf::from("/fixture/home/.local/share/ai-registry/claude-code/projects")
        );
        assert_eq!(
            default_pi_sessions_dir(),
            std::path::PathBuf::from("/fixture/home/.pi/agent/sessions")
        );
        assert_eq!(
            default_codex_sessions_dir(),
            std::path::PathBuf::from("/fixture/home/.codex/sessions")
        );
        assert_eq!(
            default_opencode_db_path(),
            std::path::PathBuf::from("/fixture/home/.local/share/opencode/opencode.db")
        );
        assert_eq!(
            default_session_tracker_path(),
            std::path::PathBuf::from("/fixture/home/.cache/agent-watcher/observer-sessions.json")
        );
    }
    assert!(
        ClaudeCodeScanner::new(vec![])
            .discover_subagents(&SessionDescriptor::default())
            .unwrap()
            .is_empty()
    );
}
