use agent_watcher::scanner::*;
use serde_json::{Value, json};
use std::{fs, io, path::PathBuf};
fn fixture() -> tempfile::TempDir {
    fs::create_dir_all(".tmp").unwrap();
    tempfile::tempdir_in(".tmp").unwrap()
}
#[test]
fn timestamp_formats_units_and_record_discriminants() {
    let dir = fixture();
    let path = dir.path().join("session.jsonl");
    let cases: [(Value, i64); 13] = [
        (json!("2026-09-18T20:53:30.007Z"), 1789764810007),
        (json!("2026-09-18T22:53:30.007+02:00"), 1789764810007),
        (json!("2026-09-18T20:53:30.007"), 1789764810007),
        (json!("2026-09-18 20:53:30"), 1789764810000),
        (json!(1700000000), 1700000000000),
        (json!(1700000000007i64), 1700000000007),
        (json!(1700000000007000i64), 1700000000007),
        (json!(1700000000007000000i64), 1700000000007),
        (json!("1700000000007.5"), 1700000000007),
        (json!(" 1700000000 "), 1700000000000),
        (json!("NaN"), 123),
        (json!("inf"), 123),
        (json!(10), 123),
    ];
    for (value, expected) in cases {
        fs::write(
            &path,
            json!({"type":"message","timestamp":value}).to_string(),
        )
        .unwrap();
        assert_eq!(
            extract_last_active_timestamp(&path, 123).unwrap(),
            expected,
            "{value}"
        );
    }
    for kind in [
        "user",
        "assistant",
        "message",
        "custom_message",
        "event_msg",
        "response_item",
        "turn_start",
        "turn_end",
        "turn_context",
        "agent_message",
        "user_message",
        "tool_call",
        "tool_result",
        "tool_use",
        "function_call",
        "function_call_output",
        "custom_tool_call",
        "custom_tool_call_output",
        "thinking",
        "question_prompt",
        "error",
    ] {
        fs::write(
            &path,
            json!({"event_type":kind,"created_at":1700000000000i64}).to_string(),
        )
        .unwrap();
        assert_eq!(
            extract_last_active_timestamp(&path, 123).unwrap(),
            1700000000000,
            "{kind}"
        );
    }
    for container in ["payload", "data", "message", "time"] {
        for field in [
            "timestamp",
            "created_at",
            "createdAt",
            "created",
            "time",
            "date",
            "completed",
        ] {
            let mut value = json!({"type":"message"});
            value[container] = json!({field:1700000000000i64});
            fs::write(&path, value.to_string()).unwrap();
            assert_eq!(
                extract_last_active_timestamp(&path, 123).unwrap(),
                1700000000000,
                "{container}.{field}"
            );
        }
    }
    fs::write(&path,"{\"type\":\"message\",\"createdAt\":1700000000000}\n{\"type\":\"message\",\"date\":1700000010000}\n{\"type\":\"assistant\",\"timestamp\":").unwrap();
    assert_eq!(
        extract_last_active_timestamp(&path, 123).unwrap(),
        1700000010000
    );
}
#[test]
fn bounded_reverse_and_forward_reads_preserve_complete_json_records() {
    let dir = fixture();
    let path = dir.path().join("session.jsonl");
    let large =
        json!({"type":"assistant","timestamp":1700000010000i64,"content":"x".repeat(300*1024)})
            .to_string();
    fs::write(
        &path,
        format!("{{\"type\":\"user\",\"timestamp\":1700000000000}}\n{large}\n"),
    )
    .unwrap();
    assert_eq!(
        extract_last_active_timestamp(&path, 123).unwrap(),
        1700000010000
    );
    let oversized = "x".repeat(9 * 1024 * 1024);
    fs::write(
        &path,
        format!("{oversized}\n{{\"data\":{{\"startedAt\":1700000000000}}}}"),
    )
    .unwrap();
    assert_eq!(extract_first_timestamp(&path).unwrap(), 1700000000000);
    fs::write(
        &path,
        "\ninvalid\n{\"payload\":{\"start\":1700000000000}}\n",
    )
    .unwrap();
    assert_eq!(extract_first_timestamp(&path).unwrap(), 1700000000000);
    fs::write(&path, "").unwrap();
    assert_eq!(extract_last_active_timestamp(&path, 123).unwrap(), 123);
    assert!(extract_first_timestamp(&path).unwrap() > 0);
    fs::write(&path, "{}\ninvalid").unwrap();
    assert!(extract_first_timestamp(&path).unwrap() > 0);
    assert_eq!(
        extract_first_timestamp(&dir.path().join("missing"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        extract_last_active_timestamp(&dir.path().join("missing"), 123)
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
}
#[test]
fn headers_validate_records_and_codex_metadata_and_fallback_paths() {
    let dir = fixture();
    let path = dir
        .path()
        .join("rollout-2026-06-12T16-08-36-custom-id.jsonl");
    fs::write(&path,json!({"type":"session_meta","timestamp":1700000000000i64,"payload":{"source":{"subagent":{"thread_spawn":true}},"cwd":"/repo"}}).to_string()).unwrap();
    let header = read_codex_session_header(&path).unwrap();
    assert_eq!(header.session_id, "custom-id");
    assert!(header.is_subagent);
    assert_eq!(header.timestamp, 1700000000000);
    fs::write(&path,json!({"type":"session_meta","payload":{"id":"one","timestamp":"2026-01-01T00:00:00Z","source":"cli"}}).to_string()).unwrap();
    assert!(!read_codex_session_header(&path).unwrap().is_subagent);
    fs::write(
        &path,
        json!({"type":"session_meta","payload":{"id":"one","source":"exec"}}).to_string(),
    )
    .unwrap();
    assert!(read_codex_session_header(&path).unwrap().is_subagent);
    for value in [
        json!({"type":"session_meta"}),
        json!({"type":"message"}),
        json!({}),
    ] {
        fs::write(&path, value.to_string()).unwrap();
        assert!(read_codex_session_header(&path).is_err());
        assert!(read_pi_session_header(&path).is_err());
    }
    fs::write(&path, "x".repeat(1024 * 1024 + 1)).unwrap();
    assert!(read_pi_session_header(&path).is_err());
    assert_eq!(encode_pi_cwd(&PathBuf::from("/a/./b/../c")), "--a-c--");
    assert_eq!(
        encode_pi_cwd(&PathBuf::from(r"C:\Users\dev")),
        "--C--Users-dev--"
    );
}
