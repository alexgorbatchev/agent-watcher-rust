use agent_parser::*;
use agent_watcher::{
    Acknowledgement, Event, Handler, Harness,
    checkpoint::CheckpointStore,
    opencode::{OpencodeReader, opencode_message_events},
};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio_util::sync::CancellationToken;
fn session() -> OpencodeSession {
    OpencodeSession {
        id: "s".into(),
        directory: "/project".into(),
        model: "model".into(),
        time_created: 100,
        ..Default::default()
    }
}
fn message(raw: &str) -> OpencodeMessage {
    OpencodeMessage {
        id: "m".into(),
        session_id: "s".into(),
        time_created: 110,
        data: parse_opencode_message_data(raw).unwrap(),
    }
}
fn part(id: &str, raw: &str, time: i64) -> OpencodePart {
    OpencodePart {
        id: id.into(),
        message_id: "m".into(),
        session_id: "s".into(),
        time_created: time,
        data: parse_opencode_part_data(raw).unwrap(),
    }
}
fn base() -> Event {
    Event {
        parsed: ParsedEvent::default(),
        harness: Harness::Opencode,
        harness_version: "1".into(),
        session_id: "s".into(),
        subagent_id: None,
        pid: 5,
    }
}
fn snapshot(message: OpencodeMessage, parts: Vec<OpencodePart>) -> OpencodeSessionSnapshot {
    OpencodeSessionSnapshot {
        session: session(),
        messages: vec![message],
        parts,
    }
}
#[test]
fn provisional_usage_is_suppressed_and_terminal_errors_survive_later_parts() {
    let provisional = message(r#"{"role":"assistant","tokens":{"input":5},"cost":0.2}"#);
    let text = part("text", r#"{"type":"text","text":"draft"}"#, 200);
    let events = opencode_message_events(&session(), &provisional, std::slice::from_ref(&text));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].parsed.event_type, "text");
    assert!(!events[0].immutable);
    let completed = message(
        r#"{"role":"assistant","tokens":{"input":5},"finish":"error","time":{"completed":150},"error":{"message":"terminal"}}"#,
    );
    let events = opencode_message_events(&session(), &completed, &[text]);
    assert_eq!(
        events
            .iter()
            .find(|event| event.key == "message:m:error")
            .unwrap()
            .parsed
            .data
            .content
            .as_deref(),
        Some("terminal")
    );
    assert!(
        events
            .iter()
            .find(|event| event.parsed.event_type == "turn_end")
            .unwrap()
            .immutable
    );
    let retry = part(
        "retry",
        r#"{"type":"retry","error":{"statusCode":429,"message":"retry"}}"#,
        140,
    );
    let events = opencode_message_events(&session(), &completed, &[retry]);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.parsed.event_type == "error")
            .count(),
        2
    );
    assert!(events.iter().any(|event| event.key == "part:retry:error"));
    assert!(events.iter().any(|event| event.key == "message:m:error"));
    let user = message(r#"{"role":"user"}"#);
    let parts = [
        part("b", r#"{"type":"text","text":"second"}"#, 130),
        part("a", r#"{"type":"text","text":"first"}"#, 120),
    ];
    assert_eq!(
        opencode_message_events(&session(), &user, &parts)[0]
            .parsed
            .data
            .content
            .as_deref(),
        Some("second\nfirst")
    );
}
#[tokio::test]
async fn revisions_retry_pending_replay_and_completed_usage_persist() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoint");
    let store = Arc::new(CheckpointStore::open(&path).unwrap());
    let reader = OpencodeReader::new(temp.path().join("db"), store.clone());
    let cancel = CancellationToken::new();
    let events = Arc::new(Mutex::new(Vec::<Event>::new()));
    let saved_ack = Arc::new(Mutex::new(None::<Acknowledgement>));
    let reject = Arc::new(AtomicBool::new(true));
    let handler: Handler = {
        let events = events.clone();
        let saved_ack = saved_ack.clone();
        let reject = reject.clone();
        Arc::new(move |_, event, ack| {
            let events = events.clone();
            let saved_ack = saved_ack.clone();
            let reject = reject.clone();
            Box::pin(async move {
                if reject.swap(false, Ordering::SeqCst) {
                    return Err(io::Error::other("backpressure"));
                }
                events.lock().unwrap().push(event.clone());
                if event.parsed.event_type == "text" {
                    *saved_ack.lock().unwrap() = ack;
                } else if let Some(ack) = ack {
                    ack.acknowledge()?;
                }
                Ok(())
            })
        })
    };
    let msg = message(r#"{"role":"assistant","tokens":{"input":8},"finish":"stop"}"#);
    let original = snapshot(
        msg.clone(),
        vec![part("text", r#"{"type":"text","text":"one"}"#, 120)],
    );
    assert!(
        reader
            .deliver_snapshot(&cancel, &base(), original.clone(), &handler)
            .await
            .is_err()
    );
    reader
        .deliver_snapshot(&cancel, &base(), original.clone(), &handler)
        .await
        .unwrap();
    reader
        .deliver_snapshot(&cancel, &base(), original.clone(), &handler)
        .await
        .unwrap();
    assert_eq!(events.lock().unwrap().len(), 2);
    store.flush().unwrap();
    let reopened = Arc::new(CheckpointStore::open(&path).unwrap());
    let reader = OpencodeReader::new(temp.path().join("db"), reopened.clone());
    reader
        .deliver_snapshot(&cancel, &base(), original, &handler)
        .await
        .unwrap();
    assert_eq!(events.lock().unwrap().len(), 3);
    saved_ack
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .acknowledge()
        .unwrap();
    let mut revised = msg;
    revised.data.tokens.as_mut().unwrap().input = 100;
    reader
        .deliver_snapshot(
            &cancel,
            &base(),
            snapshot(
                revised,
                vec![part("text", r#"{"type":"text","text":"two"}"#, 120)],
            ),
            &handler,
        )
        .await
        .unwrap();
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.parsed.event_type == "turn_end")
            .count(),
        1
    );
    assert_eq!(
        events
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .parsed
            .data
            .content
            .as_deref(),
        Some("two")
    );
    cancel.cancel();
    assert_eq!(
        reader
            .deliver_snapshot(
                &cancel,
                &base(),
                snapshot(
                    message(r#"{"role":"user"}"#),
                    vec![part("t", r#"{"type":"text","text":"p"}"#, 130)]
                ),
                &handler
            )
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
}
const SCHEMA: &str = "CREATE TABLE session(id TEXT,project_id TEXT,parent_id TEXT,slug TEXT,directory TEXT,title TEXT,version TEXT,agent TEXT,model TEXT,cost REAL,tokens_input INTEGER DEFAULT 0,tokens_output INTEGER DEFAULT 0,tokens_reasoning INTEGER DEFAULT 0,tokens_cache_read INTEGER DEFAULT 0,tokens_cache_write INTEGER DEFAULT 0,time_created INTEGER,time_updated INTEGER,time_archived INTEGER,metadata TEXT);CREATE TABLE message(id TEXT,session_id TEXT,time_created INTEGER,data TEXT);CREATE TABLE part(id TEXT,message_id TEXT,session_id TEXT,time_created INTEGER,data TEXT);";
#[tokio::test]
async fn real_database_poll_is_read_only_and_reports_missing_corrupt_and_cancelled() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch(SCHEMA).unwrap();
    connection.execute_batch("INSERT INTO session(id,directory,title,time_created,time_updated)VALUES('s','/project','title',100,100);INSERT INTO message VALUES('m','s',110,'{\"role\":\"user\"}');INSERT INTO part VALUES('t','m','s',120,'{\"type\":\"text\",\"text\":\"prompt\"}');").unwrap();
    let store = Arc::new(CheckpointStore::open(temp.path().join("checkpoint")).unwrap());
    let reader = OpencodeReader::new(&path, store);
    let cancel = CancellationToken::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let handler: Handler = {
        let events = events.clone();
        Arc::new(move |_, event, ack| {
            let events = events.clone();
            Box::pin(async move {
                events.lock().unwrap().push(event);
                ack.unwrap().acknowledge()
            })
        })
    };
    reader.poll(&cancel, &base(), &handler).await.unwrap();
    assert_eq!(
        events.lock().unwrap()[0].parsed.data.content.as_deref(),
        Some("prompt")
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM part", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    let mut absent = base();
    absent.session_id = "absent".into();
    assert_eq!(
        reader
            .poll(&cancel, &absent, &handler)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    connection
        .execute("UPDATE part SET data='broken'", [])
        .unwrap();
    assert!(reader.poll(&cancel, &base(), &handler).await.is_err());
    cancel.cancel();
    assert_eq!(
        reader
            .poll(&cancel, &base(), &handler)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    let missing = OpencodeReader::new(
        temp.path().join("missing"),
        Arc::new(CheckpointStore::open(temp.path().join("other")).unwrap()),
    );
    assert!(
        missing
            .poll(&CancellationToken::new(), &base(), &handler)
            .await
            .is_err()
    );
    assert!(!temp.path().join("missing").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_interrupts_and_joins_a_running_sqlite_read() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch(SCHEMA).unwrap();
    connection.execute_batch("INSERT INTO session(id,directory,title,time_created,time_updated)VALUES('s','/project','title',100,100);ALTER TABLE session RENAME TO session_data;").unwrap();
    let columns=["id","project_id","parent_id","slug","directory","title","version","agent","model","cost","tokens_input","tokens_output","tokens_reasoning","tokens_cache_read","tokens_cache_write","time_created","time_updated","time_archived","metadata"].map(|name|if name=="time_created"{"(WITH RECURSIVE numbers(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM numbers WHERE x<100000000) SELECT SUM(x) FROM numbers) AS time_created"}else{name}).join(",");
    connection
        .execute_batch(&format!(
            "CREATE VIEW session AS SELECT {columns} FROM session_data"
        ))
        .unwrap();
    let reader = OpencodeReader::new(
        &path,
        Arc::new(CheckpointStore::open(temp.path().join("checkpoints")).unwrap()),
    );
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let cancellation = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        stop.cancel();
    });
    let handler: Handler =
        Arc::new(|_, _, _| Box::pin(async { panic!("cancelled query delivered an event") }));
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reader.poll(&cancel, &base(), &handler),
    )
    .await
    .expect("SQLite worker did not join")
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    cancellation.await.unwrap();
    // The external writer remains usable after the observer's read-only worker ends.
    connection
        .execute("UPDATE session_data SET title='after cancellation'", [])
        .unwrap();
    assert_eq!(
        connection
            .query_row("SELECT title FROM session_data", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        "after cancellation"
    );
}
