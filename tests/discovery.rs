use agent_watcher::discovery::start_timestamp;
#[test]
fn chooses_observed_activity_then_session_start_then_clock() {
    assert_eq!(start_timestamp(10, 20, 100), 20);
    assert_eq!(start_timestamp(10, 101, 100), 10);
    assert_eq!(start_timestamp(0, 0, 100), 100);
    assert_eq!(start_timestamp(10, 100, 100), 10);
}
