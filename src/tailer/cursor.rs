use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, Metadata},
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Cursor {
    pub key: String,
    pub path: PathBuf,
    pub offset: u64,
    pub size: u64,
    pub inode: u64,
    pub device: u64,
    pub prefix_hash: String,
    pub updated_at: i64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileIdentity {
    pub device: u64,
    pub inode: u64,
}
impl FileIdentity {
    pub fn read(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            Self::default()
        }
    }
    pub fn key(self, path: &Path) -> String {
        if self == Self::default() {
            path.to_string_lossy().into_owned()
        } else {
            format!("{}:{}", self.device, self.inode)
        }
    }
}
pub fn default_cursor_path() -> PathBuf {
    cache_path("observer-cursors.json")
}
pub(crate) fn cache_path(name: &str) -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME")
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|p| p.join(".cache")))
        .unwrap_or_else(std::env::temp_dir)
        .join("agent-watcher")
        .join(name)
}
pub(crate) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
#[derive(Default)]
struct State {
    cursors: BTreeMap<String, Option<Cursor>>,
    generation: u64,
    dirty: bool,
}
pub struct CursorStore {
    path: PathBuf,
    state: Mutex<State>,
    save_lock: Mutex<()>,
}
impl CursorStore {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = if path.as_ref().as_os_str().is_empty() {
            default_cursor_path()
        } else {
            path.as_ref().to_owned()
        };
        let cursors = crate::persistence::load(&path)?;
        Ok(Self {
            path,
            state: Mutex::new(State {
                cursors,
                ..Default::default()
            }),
            save_lock: Mutex::new(()),
        })
    }
    pub fn get(&self, key: &str) -> Option<Cursor> {
        self.state
            .lock()
            .unwrap()
            .cursors
            .get(key)
            .cloned()
            .flatten()
    }
    pub fn set(&self, mut cursor: Cursor) {
        if cursor.updated_at == 0 {
            cursor.updated_at = now_millis();
        }
        let mut state = self.state.lock().unwrap();
        // Retention refreshes are persistent state even when the offset is unchanged.
        if state.cursors.get(&cursor.key).and_then(Option::as_ref) != Some(&cursor) {
            state.generation += 1;
            state.dirty = true;
            state.cursors.insert(cursor.key.clone(), Some(cursor));
        }
    }
    pub fn delete(&self, key: &str) {
        let mut state = self.state.lock().unwrap();
        if state.cursors.remove(key).is_some() {
            state.dirty = true;
            state.generation += 1;
        }
    }
    pub fn is_dirty(&self) -> bool {
        self.state.lock().unwrap().dirty
    }
    pub fn save(&self) -> io::Result<()> {
        let _save = self.save_lock.lock().unwrap();
        let (generation, snapshot) = {
            let state = self.state.lock().unwrap();
            (state.generation, state.cursors.clone())
        };
        crate::persistence::save(&self.path, &snapshot)?;
        let mut state = self.state.lock().unwrap();
        if state.generation == generation {
            state.dirty = false;
        }
        Ok(())
    }
    pub fn flush(&self) -> io::Result<()> {
        if self.is_dirty() { self.save() } else { Ok(()) }
    }
    pub fn prune(&self, retention: Duration) -> io::Result<usize> {
        let snapshot = self.state.lock().unwrap().cursors.clone();
        let cutoff =
            now_millis().saturating_sub(retention.as_millis().min(i64::MAX as u128) as i64);
        let mut candidates = Vec::new();
        for (key, cursor) in &snapshot {
            let remove = match cursor {
                None => true,
                Some(c) if c.path.as_os_str().is_empty() => true,
                Some(c) => match fs::metadata(&c.path) {
                    Ok(metadata) => {
                        let id = FileIdentity::read(&metadata);
                        let key_id = key.split_once(':').and_then(|(dev, ino)| {
                            Some(FileIdentity {
                                device: dev.parse().ok()?,
                                inode: ino.parse().ok()?,
                            })
                        });
                        let device = if c.device == 0 {
                            key_id.unwrap_or_default().device
                        } else {
                            c.device
                        };
                        let inode = if c.inode == 0 {
                            key_id.unwrap_or_default().inode
                        } else {
                            c.inode
                        };
                        (device != 0 && id.device != 0 && device != id.device)
                            || (inode != 0 && id.inode != 0 && inode != id.inode)
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => true,
                    Err(_) => !retention.is_zero() && c.updated_at < cutoff,
                },
            };
            if remove {
                candidates.push(key.clone());
            }
        }
        let mut state = self.state.lock().unwrap();
        let mut count = 0;
        for key in candidates {
            if state.cursors.get(&key) == snapshot.get(&key) {
                state.cursors.remove(&key);
                count += 1;
            }
        }
        if count > 0 {
            state.dirty = true;
            state.generation += 1;
        }
        Ok(count)
    }
}
pub fn should_reset(cursor: &Cursor, inode: u64, size: u64, hash: &str) -> bool {
    (inode != 0 && cursor.inode != 0 && inode != cursor.inode)
        || size < cursor.offset
        || (!hash.is_empty() && !cursor.prefix_hash.is_empty() && hash != cursor.prefix_hash)
}
pub fn prefix_hash(reader: &mut (impl Read + Seek), size: u64) -> io::Result<String> {
    if size < 512 {
        return Ok(String::new());
    }
    reader.seek(SeekFrom::Start(0))?;
    let mut prefix = [0; 512];
    match reader.read_exact(&mut prefix) {
        Ok(()) => Ok(format!("{:x}", Sha256::digest(prefix))),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(String::new()),
        Err(e) => Err(e),
    }
}
