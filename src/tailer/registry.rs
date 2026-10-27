use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::{
    collections::BTreeMap,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct Signal {
    changed: Notify,
    closed: CancellationToken,
}
#[derive(Default)]
struct State {
    closed: bool,
    next: u64,
    directories: BTreeMap<PathBuf, usize>,
    files: BTreeMap<PathBuf, usize>,
    subscribers: BTreeMap<PathBuf, BTreeMap<u64, Weak<Signal>>>,
}
pub struct WatcherRegistry {
    watcher: Mutex<Option<RecommendedWatcher>>,
    state: Arc<Mutex<State>>,
}
pub struct Subscription {
    registry: Weak<WatcherRegistry>,
    path: PathBuf,
    id: u64,
    directory: bool,
    signal: Arc<Signal>,
}
impl Subscription {
    /// Returns false after the registry closes; notifications coalesce.
    pub async fn changed(&mut self) -> bool {
        tokio::select! { biased; _=self.signal.closed.cancelled()=>false, _=self.signal.changed.notified()=>true }
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        self.signal.closed.cancel();
        if let Some(registry) = self.registry.upgrade() {
            registry.unsubscribe(&self.path, self.id, self.directory);
        }
    }
}
fn absolute(path: &Path) -> io::Result<PathBuf> {
    std::path::absolute(path).map(|p| p.components().collect())
}
impl WatcherRegistry {
    pub fn new() -> io::Result<Arc<Self>> {
        let state = Arc::new(Mutex::new(State::default()));
        let weak = Arc::downgrade(&state);
        let watcher = notify::recommended_watcher(move |event: notify::Result<Event>| {
            let Some(state) = weak.upgrade() else { return };
            match event {
                Ok(event)
                    if matches!(
                        event.kind,
                        EventKind::Create(_)
                            | EventKind::Modify(_)
                            | EventKind::Remove(_)
                            | EventKind::Any
                    ) =>
                {
                    let state = state.lock().unwrap();
                    for path in event.paths {
                        if let Some(subscribers) = state.subscribers.get(&path) {
                            for signal in subscribers.values().filter_map(Weak::upgrade) {
                                signal.changed.notify_one();
                            }
                        }
                    }
                }
                Err(error) => tracing::warn!(action="watcher_event_error",%error),
                _ => {}
            }
        })
        .map_err(io::Error::other)?;
        Ok(Arc::new(Self {
            watcher: Mutex::new(Some(watcher)),
            state,
        }))
    }
    pub fn subscribe(self: &Arc<Self>, path: impl AsRef<Path>) -> io::Result<Subscription> {
        let path = absolute(path.as_ref())?;
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("watched file has no parent"))?
            .to_owned();
        let mut watcher = self.watcher.lock().unwrap();
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "watcher registry is closed",
            ));
        }
        let watcher = watcher
            .as_mut()
            .ok_or_else(|| io::Error::other("watcher registry is closed"))?;
        // notify waits for its event thread when changing watches. Its callback
        // takes state, so never hold state across watch/unwatch. The watcher
        // mutex serializes subscription changes while state is unlocked.
        let directory = if let Some(count) = state.directories.get_mut(&parent) {
            *count += 1;
            true
        } else {
            drop(state);
            let watched_directory = watcher.watch(&parent, RecursiveMode::NonRecursive).is_ok();
            state = self.state.lock().unwrap();
            if watched_directory {
                state.directories.insert(parent, 1);
            } else {
                let needs_watch = !state.files.contains_key(&path);
                drop(state);
                if needs_watch {
                    watcher
                        .watch(&path, RecursiveMode::NonRecursive)
                        .map_err(io::Error::other)?;
                }
                state = self.state.lock().unwrap();
                *state.files.entry(path.clone()).or_default() += 1;
            }
            watched_directory
        };
        state.next += 1;
        let id = state.next;
        let signal = Arc::new(Signal {
            changed: Notify::new(),
            closed: CancellationToken::new(),
        });
        state
            .subscribers
            .entry(path.clone())
            .or_default()
            .insert(id, Arc::downgrade(&signal));
        Ok(Subscription {
            registry: Arc::downgrade(self),
            path,
            id,
            directory,
            signal,
        })
    }
    fn unsubscribe(&self, path: &Path, id: u64, directory: bool) {
        let mut watcher = self.watcher.lock().unwrap();
        let mut state = self.state.lock().unwrap();
        if let Some(subscribers) = state.subscribers.get_mut(path) {
            subscribers.remove(&id);
            if subscribers.is_empty() {
                state.subscribers.remove(path);
            }
        }
        let watched = if directory {
            path.parent().unwrap_or(path)
        } else {
            path
        };
        let counts = if directory {
            &mut state.directories
        } else {
            &mut state.files
        };
        if let Some(count) = counts.get_mut(watched) {
            *count -= 1;
            if *count == 0 {
                counts.remove(watched);
                drop(state);
                if let Some(w) = watcher.as_mut()
                    && let Err(error) = w.unwatch(watched)
                {
                    tracing::warn!(action="watcher_unwatch_error",%error);
                }
            }
        }
    }
    pub fn close(&self) {
        let watcher = {
            let mut watcher = self.watcher.lock().unwrap();
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            for signal in state
                .subscribers
                .values()
                .flat_map(|s| s.values())
                .filter_map(Weak::upgrade)
            {
                signal.closed.cancel();
            }
            state.subscribers.clear();
            state.directories.clear();
            state.files.clear();
            watcher.take()
        };
        drop(watcher);
    }
    pub fn watched_dir_count(&self) -> usize {
        self.state.lock().unwrap().directories.len()
    }
    pub fn fallback_file_count(&self) -> usize {
        self.state.lock().unwrap().files.len()
    }
    pub fn subscriber_count(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .subscribers
            .values()
            .map(BTreeMap::len)
            .sum()
    }
}
impl Drop for WatcherRegistry {
    fn drop(&mut self) {
        self.close();
    }
}
