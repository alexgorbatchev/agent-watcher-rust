use agent_parser::ParsedEvent;
use agent_watcher::{
    Acknowledgement, Event, Handler, Harness, tailer::Line, transcript::TranscriptReader,
};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio_util::sync::CancellationToken;
fn base(harness: Harness) -> Event {
    Event {
        parsed: ParsedEvent {
            cwd: "/fallback".into(),
            ..Default::default()
        },
        harness,
        harness_version: "1".into(),
        session_id: "session".into(),
        subagent_id: Some("child".into()),
        pid: 12,
    }
}
fn line(bytes: &str, offset: u64, generation: u64, count: Arc<AtomicUsize>) -> Line {
    Line {
        bytes: bytes.as_bytes().into(),
        offset,
        generation,
        acknowledgement: Acknowledgement::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
    }
}
#[tokio::test]
async fn batch_retry_preserves_accepted_events_and_parser_state() {
    let cancel = CancellationToken::new();
    let mut reader = TranscriptReader::new(base(Harness::ClaudeCode));
    let commits = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let failures = Arc::new(AtomicUsize::new(0));
    let handler: Handler = {
        let observed = observed.clone();
        let failures = failures.clone();
        Arc::new(move |_, event, ack| {
            let observed = observed.clone();
            let failures = failures.clone();
            Box::pin(async move {
                if event.parsed.event_type == "turn_end"
                    && failures.fetch_add(1, Ordering::SeqCst) == 0
                {
                    return Err(io::Error::other("backpressure"));
                }
                observed.lock().unwrap().push((event, ack.is_some()));
                if let Some(ack) = ack {
                    ack.acknowledge()?;
                }
                Ok(())
            })
        })
    };
    let bytes = r#"{"type":"assistant","message":{"id":"msg","role":"assistant","content":[{"type":"thinking","thinking":"plan"},{"type":"text","text":"answer"}],"usage":{"input_tokens":5,"output_tokens":2}}}"#;
    assert!(
        reader
            .process(&cancel, line(bytes, 100, 1, commits.clone()), &handler)
            .await
            .is_err()
    );
    assert_eq!(observed.lock().unwrap().len(), 1);
    assert_eq!(commits.load(Ordering::SeqCst), 0);
    reader
        .process(&cancel, line(bytes, 100, 1, commits.clone()), &handler)
        .await
        .unwrap();
    let events = observed.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .map(|(e, _)| e.parsed.event_type.as_str())
            .collect::<Vec<_>>(),
        ["thinking", "turn_end"]
    );
    assert!(!events[0].1);
    assert!(events[1].1);
    assert_eq!(events[1].0.parsed.data.usage.as_ref().unwrap().input, 5);
    assert_eq!(events[1].0.parsed.cwd, "/fallback");
    assert_eq!(events[1].0.subagent_id.as_deref(), Some("child"));
    assert_eq!(commits.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn malformed_empty_and_lifecycle_lines_acknowledge_and_generation_resets_dedup() {
    let cancel = CancellationToken::new();
    let count = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let handler: Handler = {
        let events = events.clone();
        Arc::new(move |_, event, ack| {
            let events = events.clone();
            Box::pin(async move {
                events.lock().unwrap().push(event);
                if let Some(ack) = ack {
                    ack.acknowledge()?;
                }
                Ok(())
            })
        })
    };
    let mut reader = TranscriptReader::new(base(Harness::ClaudeCode));
    reader
        .process(&cancel, line("broken", 1, 1, count.clone()), &handler)
        .await
        .unwrap();
    reader
        .process(
            &cancel,
            line(r#"{"type":"system","subtype":"init"}"#, 2, 1, count.clone()),
            &handler,
        )
        .await
        .unwrap();
    let bytes = r#"{"type":"assistant","message":{"id":"m","content":[{"type":"text","text":"hi"}],"usage":{"input_tokens":7}}}"#;
    reader
        .process(&cancel, line(bytes, 3, 1, count.clone()), &handler)
        .await
        .unwrap();
    reader
        .process(&cancel, line(bytes, 4, 1, count.clone()), &handler)
        .await
        .unwrap();
    reader
        .process(&cancel, line(bytes, 1, 2, count.clone()), &handler)
        .await
        .unwrap();
    reader
        .process(&cancel, line(bytes, 2, 1, count.clone()), &handler)
        .await
        .unwrap();
    let events = events.lock().unwrap();
    assert_eq!(events.len(), 3);
    assert!(events[0].parsed.data.usage.is_some());
    assert!(events[1].parsed.data.usage.is_none());
    assert!(events[2].parsed.data.usage.is_some());
    assert_eq!(count.load(Ordering::SeqCst), 5);
}
#[tokio::test]
async fn harness_dispatch_and_cancellation() {
    let token = CancellationToken::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let handler: Handler = {
        let events = events.clone();
        Arc::new(move |_, event, ack| {
            let events = events.clone();
            Box::pin(async move {
                events.lock().unwrap().push(event);
                if let Some(ack) = ack {
                    ack.acknowledge()?;
                }
                Ok(())
            })
        })
    };
    let n = Arc::new(AtomicUsize::new(0));
    for (harness, bytes) in [
        (
            Harness::Pi,
            r#"{"type":"message","message":{"role":"user","content":[{"type":"text","text":"pi"}]}}"#,
        ),
        (
            Harness::Codex,
            r#"{"type":"event_msg","payload":{"type":"user_message","message":"codex"}}"#,
        ),
    ] {
        TranscriptReader::new(base(harness))
            .process(&token, line(bytes, 1, 1, n.clone()), &handler)
            .await
            .unwrap();
    }
    assert_eq!(events.lock().unwrap().len(), 2);
    assert!(
        TranscriptReader::new(base(Harness::Opencode))
            .process(&token, line("{}", 1, 1, n.clone()), &handler)
            .await
            .is_err()
    );
    token.cancel();
    assert_eq!(
        TranscriptReader::new(base(Harness::Pi))
            .process(&token, line("{}", 1, 1, n), &handler)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
}
