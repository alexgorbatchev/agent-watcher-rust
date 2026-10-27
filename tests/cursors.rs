use agent_watcher::tailer::{Cursor, CursorStore, FileIdentity, prefix_hash, should_reset};
use std::{fs, time::Duration};

#[test]
fn persistence_and_pruning_preserve_existing_transcripts_regardless_of_age() {
    let dir = tempfile::tempdir().unwrap();
    let transcript = dir.path().join("transcript.jsonl");
    fs::write(&transcript, "a line\n").unwrap();
    let id = FileIdentity::read(&fs::metadata(&transcript).unwrap());
    let state = dir.path().join("state/cursors.json");
    let store = CursorStore::open(&state).unwrap();
    let cursor = Cursor {
        key: id.key(&transcript),
        path: transcript.clone(),
        offset: 7,
        size: 7,
        inode: id.inode,
        device: id.device,
        updated_at: 1,
        ..Default::default()
    };
    store.set(cursor.clone());
    assert!(store.is_dirty());
    store.flush().unwrap();
    assert!(!store.is_dirty());
    assert_eq!(
        CursorStore::open(&state).unwrap().get(&cursor.key),
        Some(cursor.clone())
    );
    assert_eq!(store.prune(Duration::from_millis(1)).unwrap(), 0);
    fs::remove_file(&transcript).unwrap();
    assert_eq!(store.prune(Duration::from_secs(86_400)).unwrap(), 1);
    store.flush().unwrap();
    assert_eq!(CursorStore::open(&state).unwrap().get(&cursor.key), None);
}

#[test]
fn invalid_state_and_io_errors_are_reported_and_flush_does_not_lose_updates() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state.json");
    fs::write(&state, "{").unwrap();
    assert!(CursorStore::open(&state).is_err());
    fs::write(&state, "null").unwrap();
    let store = CursorStore::open(&state).unwrap();
    store.set(Cursor {
        key: "first".into(),
        ..Default::default()
    });
    assert!(store.get("first").unwrap().updated_at > 0);
    store.delete("first");
    store.flush().unwrap();
    assert_eq!(fs::read_to_string(&state).unwrap(), "{}");
    let blocked = dir.path().join("blocked");
    fs::write(&blocked, "file").unwrap();
    let broken = CursorStore::open(blocked.join("state.json"));
    assert!(broken.is_err());
    let store = CursorStore::open(dir.path().join("missing/state.json")).unwrap();
    fs::write(dir.path().join("missing"), "file").unwrap();
    store.set(Cursor {
        key: "pending".into(),
        ..Default::default()
    });
    assert!(store.flush().is_err());
    assert!(store.is_dirty());
}

#[test]
fn prefix_and_identity_detect_replacement_and_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log");
    fs::write(&path, vec![b'a'; 512]).unwrap();
    let hash = prefix_hash(&mut fs::File::open(&path).unwrap(), 512).unwrap();
    assert_eq!(hash.len(), 64);
    assert!(
        prefix_hash(&mut fs::File::open(&path).unwrap(), 511)
            .unwrap()
            .is_empty()
    );
    let c = Cursor {
        inode: 12,
        offset: 500,
        prefix_hash: hash.clone(),
        ..Default::default()
    };
    assert!(!should_reset(&c, 12, 512, &hash));
    assert!(should_reset(&c, 13, 512, &hash));
    assert!(should_reset(&c, 12, 499, &hash));
    assert!(should_reset(&c, 12, 512, "different"));
    assert!(!should_reset(&c, 0, 512, ""));
}

#[test]
fn concurrent_saves_and_updates_leave_a_complete_json_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state");
    let store = std::sync::Arc::new(CursorStore::open(&path).unwrap());
    let threads: Vec<_> = (0..4)
        .map(|thread| {
            let store = store.clone();
            std::thread::spawn(move || {
                for index in 0..20 {
                    store.set(Cursor {
                        key: format!("{thread}:{index}"),
                        offset: index,
                        ..Default::default()
                    });
                    store.save().unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    store.flush().unwrap();
    let reopened = CursorStore::open(&path).unwrap();
    for thread in 0..4 {
        for index in 0..20 {
            assert_eq!(
                reopened.get(&format!("{thread}:{index}")).unwrap().offset,
                index
            );
        }
    }
    assert!(!store.is_dirty());
}

#[test]
fn retention_refresh_null_entries_and_replaced_identities_survive_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let transcript = dir.path().join("transcript");
    fs::write(&transcript, "old\n").unwrap();
    let identity = FileIdentity::read(&fs::metadata(&transcript).unwrap());
    let key = identity.key(&transcript);
    let state = dir.path().join("cursors");
    let mut records = serde_json::Map::new();
    records.insert(
        key.clone(),
        serde_json::to_value(Cursor {
            key: key.clone(),
            path: transcript.clone(),
            updated_at: 1,
            ..Default::default()
        })
        .unwrap(),
    );
    records.insert("null".into(), serde_json::Value::Null);
    records.insert(
        "empty".into(),
        serde_json::to_value(Cursor::default()).unwrap(),
    );
    fs::write(&state, serde_json::to_vec(&records).unwrap()).unwrap();
    let store = CursorStore::open(&state).unwrap();
    assert_eq!(store.prune(Duration::ZERO).unwrap(), 2);
    store.flush().unwrap();
    let mut refreshed = store.get(&key).unwrap();
    refreshed.updated_at = 2;
    store.set(refreshed.clone());
    assert!(store.is_dirty());
    store.flush().unwrap();
    assert_eq!(
        CursorStore::open(&state)
            .unwrap()
            .get(&key)
            .unwrap()
            .updated_at,
        2
    );
    store.set(refreshed);
    store.delete("unknown");
    store.flush().unwrap();
    assert!(!store.is_dirty());
    assert_eq!(store.prune(Duration::from_nanos(1)).unwrap(), 0);
    let replacement = dir.path().join("replacement");
    fs::write(&replacement, "new\n").unwrap();
    fs::rename(&replacement, &transcript).unwrap();
    #[cfg(unix)]
    assert_eq!(store.prune(Duration::ZERO).unwrap(), 1);
}

#[test]
fn inaccessible_transcripts_use_retention_and_prefix_errors_are_preserved() {
    use std::io::{self, Read, Seek, SeekFrom};
    let dir = tempfile::tempdir().unwrap();
    let blocked = dir.path().join("blocked");
    fs::write(&blocked, "file").unwrap();
    let store = CursorStore::open(dir.path().join("state")).unwrap();
    store.set(Cursor {
        key: "blocked".into(),
        path: blocked.join("session"),
        updated_at: 1,
        ..Default::default()
    });
    assert_eq!(store.prune(Duration::ZERO).unwrap(), 0);
    assert_eq!(store.prune(Duration::from_millis(1)).unwrap(), 1);
    let mut short = io::Cursor::new(b"short".as_slice());
    assert!(prefix_hash(&mut short, 512).unwrap().is_empty());
    struct Broken;
    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "read denied",
            ))
        }
    }
    impl Seek for Broken {
        fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
            Ok(0)
        }
    }
    assert_eq!(
        prefix_hash(&mut Broken, 512).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
}
