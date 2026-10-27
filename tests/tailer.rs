use agent_watcher::{
    Acknowledgement,
    tailer::{CursorStore, DeliveryMode, Tailer, WatcherRegistry},
};
use std::{
    fs, io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[test]
fn native_watch_registration_during_events_does_not_deadlock() {
    use std::{
        process::{Command, Stdio},
        thread,
        time::Instant,
    };
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "native_watch_churn_worker",
            "--test-threads=1",
        ])
        .env("WATCHER_CHURN_WORKER", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "native watch churn failed: {status}");
            break;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("native watch registration deadlocked while processing events");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "isolated bounded subprocess; run by native_watch_registration_during_events_does_not_deadlock"]
fn native_watch_churn_worker() {
    use std::sync::atomic::{AtomicBool, Ordering};
    if std::env::var_os("WATCHER_CHURN_WORKER").is_none() {
        return;
    }
    fs::create_dir_all(".tmp").unwrap();
    let dir = tempfile::tempdir_in(".tmp").unwrap();
    let path = dir.path().join("session");
    fs::write(&path, b"initial").unwrap();
    let registry = WatcherRegistry::new().unwrap();
    let _anchor = registry.subscribe(&path).unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let writer_done = done.clone();
    let writer_path = path.clone();
    let writer = std::thread::spawn(move || {
        while !writer_done.load(Ordering::Acquire) {
            fs::write(&writer_path, b"event").unwrap();
        }
    });
    for index in 0..3000 {
        let other_dir = dir.path().join(index.to_string());
        fs::create_dir(&other_dir).unwrap();
        let subscription = registry.subscribe(other_dir.join("session")).unwrap();
        drop(subscription);
    }
    done.store(true, Ordering::Release);
    writer.join().unwrap();
    assert_eq!(registry.subscriber_count(), 1);
    registry.close();
}

#[tokio::test]
async fn partial_lines_backpressure_and_restart_use_acknowledged_byte_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session.jsonl");
    fs::write(&path, "one\r\ntwo\npartial").unwrap();
    let store = Arc::new(CursorStore::open(dir.path().join("cursors")).unwrap());
    let registry = WatcherRegistry::new().unwrap();
    let tail = Tailer::new(
        &path,
        store.clone(),
        registry.clone(),
        DeliveryMode::Explicit,
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let mut accepted = Vec::new();
    let mut acks = Vec::new();
    let mut reject = true;
    let result = tail
        .read_available(&cancel, |line| {
            let rejected = reject && line.bytes == b"two";
            if rejected {
                reject = false;
            } else {
                accepted.push(line.bytes);
                acks.push(line.acknowledgement);
            }
            std::future::ready(if rejected {
                Err(io::Error::other("backpressure"))
            } else {
                Ok(())
            })
        })
        .await;
    assert!(result.is_err());
    assert_eq!(tail.current_offset(), 5);
    assert_eq!(tail.committed_offset(), 0);
    acks[0].acknowledge().unwrap();
    tail.read_available(&cancel, |line| {
        accepted.push(line.bytes);
        acks.push(line.acknowledgement);
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    assert_eq!(accepted, vec![b"one".to_vec(), b"two".to_vec()]);
    assert_eq!(tail.current_offset(), 9);
    store.flush().unwrap();
    let resumed = Tailer::new(
        &path,
        Arc::new(CursorStore::open(dir.path().join("cursors")).unwrap()),
        registry,
        DeliveryMode::Automatic,
    )
    .unwrap();
    let mut replay = Vec::new();
    resumed
        .read_available(&cancel, |line| {
            replay.push(line.bytes);
            std::future::ready(Ok(()))
        })
        .await
        .unwrap();
    assert_eq!(replay, vec![b"two".to_vec()]);
    assert_eq!(resumed.committed_offset(), 9);
}

#[tokio::test]
async fn replacement_and_truncation_ignore_old_generation_acknowledgements() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session");
    fs::write(&path, "old line\n").unwrap();
    let tail = Tailer::new(
        &path,
        Arc::new(CursorStore::open(dir.path().join("state")).unwrap()),
        WatcherRegistry::new().unwrap(),
        DeliveryMode::Explicit,
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let mut acks: Vec<Acknowledgement> = Vec::new();
    tail.read_available(&cancel, |l| {
        acks.push(l.acknowledgement);
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    let replacement = dir.path().join("replacement");
    fs::write(&replacement, "new\n").unwrap();
    fs::rename(&replacement, &path).unwrap();
    tail.read_available(&cancel, |l| {
        acks.push(l.acknowledgement);
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    acks[0].acknowledge().unwrap();
    assert_eq!(tail.committed_offset(), 0);
    acks[1].acknowledge().unwrap();
    assert_eq!(tail.committed_offset(), 4);
    fs::write(&path, "x\n").unwrap();
    tail.read_available(&cancel, |l| {
        acks.push(l.acknowledgement);
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    acks[1].acknowledge().unwrap();
    assert_eq!(tail.committed_offset(), 0);
    acks[2].acknowledge().unwrap();
    assert_eq!(tail.committed_offset(), 2);
}

#[tokio::test]
async fn native_watches_multiplex_paths_and_cleanup_on_stop() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a");
    let b = dir.path().join("b");
    fs::write(&a, "").unwrap();
    fs::write(&b, "").unwrap();
    let registry = WatcherRegistry::new().unwrap();
    let mut first = registry.subscribe(&a).unwrap();
    let second = registry.subscribe(&b).unwrap();
    assert_eq!(registry.watched_dir_count(), 1);
    assert_eq!(registry.subscriber_count(), 2);
    fs::write(&a, "hello\n").unwrap();
    tokio::time::timeout(Duration::from_secs(3), first.changed())
        .await
        .unwrap();
    drop(first);
    drop(second);
    assert_eq!(registry.watched_dir_count(), 0);
    let store = Arc::new(CursorStore::open(dir.path().join("state")).unwrap());
    let tail = Arc::new(Tailer::new(&a, store, registry.clone(), DeliveryMode::Automatic).unwrap());
    let lines = Arc::new(Mutex::new(Vec::new()));
    let output = lines.clone();
    let cancel = CancellationToken::new();
    let task_tail = tail.clone();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        task_tail
            .run(task_cancel, move |line| {
                output.lock().unwrap().push(line.bytes);
                std::future::ready(Ok(()))
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while lines.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tail.stop();
    task.await.unwrap().unwrap();
    assert_eq!(registry.subscriber_count(), 0);
    registry.close();
    assert!(registry.subscribe(&a).is_err());
}

#[tokio::test]
async fn truncation_never_restores_an_earlier_cursor_into_the_new_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session");
    fs::write(&path, "old\na very long unacknowledged line\n").unwrap();
    let store = Arc::new(CursorStore::open(dir.path().join("state")).unwrap());
    let tail = Tailer::new(
        &path,
        store,
        WatcherRegistry::new().unwrap(),
        DeliveryMode::Explicit,
    )
    .unwrap();
    let cancel = CancellationToken::new();
    tail.read_available(&cancel, |line| {
        if line.bytes == b"old" {
            line.acknowledgement.acknowledge().unwrap();
        }
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    assert_eq!(tail.committed_offset(), 4);
    fs::write(&path, "x\nnew\n").unwrap();
    let mut observed = Vec::new();
    tail.read_available(&cancel, |line| {
        observed.push(line.bytes);
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    assert_eq!(observed, vec![b"x".to_vec(), b"new".to_vec()]);
    assert_eq!(tail.committed_offset(), 0);
}

#[tokio::test]
async fn large_lines_empty_lines_prefix_rewrites_and_cancellation_remain_observable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("session");
    let mut contents = vec![b'a'; 16 * 1024 * 1024];
    contents.extend_from_slice(b"\n\r\nlast\n");
    fs::write(&path, &contents).unwrap();
    let tail = Tailer::new(
        &path,
        Arc::new(CursorStore::open(dir.path().join("state")).unwrap()),
        WatcherRegistry::new().unwrap(),
        DeliveryMode::Automatic,
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let mut lengths = Vec::new();
    tail.read_available(&cancel, |line| {
        lengths.push(line.bytes.len());
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    assert_eq!(lengths, vec![16 * 1024 * 1024, 4]);
    assert_eq!(tail.committed_offset(), contents.len() as u64);
    contents[0] = b'b';
    fs::write(&path, &contents).unwrap();
    let mut first = None;
    tail.read_available(&cancel, |line| {
        if first.is_none() {
            first = line.bytes.first().copied();
        }
        std::future::ready(Ok(()))
    })
    .await
    .unwrap();
    assert_eq!(first, Some(b'b'));
    cancel.cancel();
    fs::remove_file(&path).unwrap();
    tail.read_available(&cancel, |_| std::future::ready(Ok(())))
        .await
        .unwrap();
    Arc::new(tail)
        .run(cancel, |_| std::future::ready(Ok(())))
        .await
        .unwrap();
}

#[tokio::test]
async fn registry_closure_wakes_all_subscribers_and_absent_parents_return_errors() {
    let dir = tempfile::tempdir().unwrap();
    let registry = WatcherRegistry::new().unwrap();
    assert!(
        registry
            .subscribe(dir.path().join("missing/session"))
            .is_err()
    );
    let path = dir.path().join("session");
    let mut first = registry.subscribe(&path).unwrap();
    let mut second = registry.subscribe(&path).unwrap();
    assert_eq!(registry.fallback_file_count(), 0);
    fs::write(&path, "created\n").unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), first.changed())
            .await
            .unwrap()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(3), second.changed())
            .await
            .unwrap()
    );
    registry.close();
    assert!(!first.changed().await);
    assert!(!second.changed().await);
    drop(first);
    drop(second);
    assert_eq!(registry.subscriber_count(), 0);
    assert_eq!(registry.watched_dir_count(), 0);
    registry.close();
}

#[tokio::test]
async fn tailer_reports_open_errors_and_stopped_runs_do_not_read_files() {
    let dir = tempfile::tempdir().unwrap();
    let registry = WatcherRegistry::new().unwrap();
    let tail = Arc::new(
        Tailer::new(
            dir.path().join("absent"),
            Arc::new(CursorStore::open(dir.path().join("state")).unwrap()),
            registry.clone(),
            DeliveryMode::Explicit,
        )
        .unwrap(),
    );
    let error = tail
        .read_available(&CancellationToken::new(), |_| std::future::ready(Ok(())))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    tail.stop();
    tail.run(CancellationToken::new(), |_| {
        std::future::ready(Err(io::Error::other("must not run")))
    })
    .await
    .unwrap();
    assert_eq!(registry.subscriber_count(), 0);
}
