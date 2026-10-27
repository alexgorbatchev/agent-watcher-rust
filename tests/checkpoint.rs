use agent_watcher::checkpoint::CheckpointStore;
use std::sync::Arc;
#[test]
fn revisions_acknowledge_out_of_order_retry_and_survive_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoints");
    let store = Arc::new(CheckpointStore::open(&path).unwrap());
    let first = store.reserve("part", "first", false).unwrap().unwrap();
    assert!(store.reserve("part", "first", false).unwrap().is_none());
    let second = store.reserve("part", "second", false).unwrap().unwrap();
    second.acknowledgement.acknowledge().unwrap();
    first.acknowledgement.acknowledge().unwrap();
    store.flush().unwrap();
    let restored = Arc::new(CheckpointStore::open(&path).unwrap());
    assert!(restored.reserve("part", "second", false).unwrap().is_none());
    let rejected = restored.reserve("bad", "one", false).unwrap().unwrap();
    rejected.reject().unwrap();
    let retry = restored.reserve("bad", "one", false).unwrap().unwrap();
    rejected.acknowledgement.acknowledge().unwrap();
    assert!(restored.reserve("bad", "one", false).unwrap().is_none());
    retry.reject().unwrap();
    // The stale callback must neither commit bytes nor consume the retry.
    restored
        .reserve("bad", "one", false)
        .unwrap()
        .expect("stale ACK committed a rejected reservation")
        .acknowledgement
        .acknowledge()
        .unwrap();
    let usage = restored.reserve("usage", "one", true).unwrap().unwrap();
    assert!(restored.reserve("usage", "two", true).unwrap().is_none());
    usage.acknowledgement.acknowledge().unwrap();
    assert!(restored.reserve("usage", "two", true).unwrap().is_none());
}
#[test]
fn invalid_storage_is_reported_and_failed_flush_remains_dirty() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoints");
    std::fs::write(&path, "broken").unwrap();
    assert!(CheckpointStore::open(&path).is_err());
    std::fs::remove_file(&path).unwrap();
    let s = Arc::new(CheckpointStore::open(&path).unwrap());
    s.reserve("k", "v", false)
        .unwrap()
        .unwrap()
        .acknowledgement
        .acknowledge()
        .unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(s.flush().is_err());
    std::fs::remove_dir(&path).unwrap();
    s.flush().unwrap();
    assert!(
        Arc::new(CheckpointStore::open(&path).unwrap())
            .reserve("k", "v", false)
            .unwrap()
            .is_none()
    );
}
