use agent_watcher::{Acknowledgement, delivery::DeliveryQueue};
use std::{
    io,
    sync::{Arc, Mutex},
};
#[test]
fn commits_only_contiguous_acknowledged_lines_and_rejects_old_generations() {
    let q = Arc::new(DeliveryQueue::default());
    let commits = Arc::new(Mutex::new(Vec::new()));
    let add = |generation, n| {
        let commits = commits.clone();
        q.add(
            generation,
            Acknowledgement::new(move || {
                commits.lock().unwrap().push(n);
                Ok(())
            }),
        )
        .unwrap()
    };
    let first = add(1, 1);
    let second = add(1, 2);
    second.acknowledge().unwrap();
    assert!(commits.lock().unwrap().is_empty());
    first.acknowledge().unwrap();
    first.acknowledge().unwrap();
    assert_eq!(*commits.lock().unwrap(), [1, 2]);
    let old = add(1, 3);
    let current = add(2, 4);
    old.acknowledge().unwrap();
    current.acknowledge().unwrap();
    add(1, 5).acknowledge().unwrap();
    assert_eq!(*commits.lock().unwrap(), [1, 2, 4]);
}
#[test]
fn failed_commit_remains_retryable() {
    let q = Arc::new(DeliveryQueue::default());
    let attempts = Arc::new(Mutex::new(0));
    let count = attempts.clone();
    let ack = q
        .add(
            1,
            Acknowledgement::new(move || {
                let mut n = count.lock().unwrap();
                *n += 1;
                if *n == 1 {
                    Err(io::Error::other("disk"))
                } else {
                    Ok(())
                }
            }),
        )
        .unwrap();
    assert!(ack.acknowledge().is_err());
    ack.acknowledge().unwrap();
    assert_eq!(*attempts.lock().unwrap(), 2);
}
