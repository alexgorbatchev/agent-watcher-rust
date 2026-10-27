use super::{
    ProcessSnapshot, SessionDescriptor, check_cancel,
    paths::clean_path,
    process::pair_process,
    timestamps::{extract_last_active_timestamp, header_timestamp, mtime, rfc_timestamp},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, Metadata},
    io::{self, BufRead, BufReader, Read},
    path::{Path, PathBuf},
    time::SystemTime,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct PiHeader {
    #[serde(rename = "type")]
    pub record_type: String,
    pub version: i32,
    pub id: String,
    pub timestamp: Value,
    pub cwd: String,
}
#[derive(Clone, Debug, Default)]
pub struct CodexHeader {
    pub session_id: String,
    pub timestamp: i64,
    pub cwd: PathBuf,
    pub cli_version: String,
    pub parent_thread_id: String,
    pub agent_nickname: String,
    pub agent_role: String,
    pub is_subagent: bool,
}
fn header_value(path: &Path) -> io::Result<Value> {
    let mut reader = BufReader::new(File::open(path)?).take(1024 * 1024 + 1);
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line)?;
    if line.len() > 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "session header exceeds 1 MiB",
        ));
    }
    serde_json::from_slice(&line).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
pub fn read_pi_session_header(path: &Path) -> io::Result<PiHeader> {
    let header: PiHeader = serde_json::from_value(header_value(path)?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if header.record_type != "session" || header.id.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Pi session header",
        ));
    }
    Ok(header)
}
pub fn read_codex_session_header(path: &Path) -> io::Result<CodexHeader> {
    let value = header_value(path)?;
    if value["type"] != "session_meta" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Codex session header",
        ));
    }
    let p = &value["payload"];
    if !p.is_object() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing Codex metadata payload",
        ));
    }
    let text = |key: &str| {
        p.get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let mut session_id = text("id");
    if session_id.is_empty()
        && let Ok(info) = agent_parser::parse_codex_rollout_path(&path.to_string_lossy())
    {
        session_id = info.session_id;
    }
    let parent_thread_id = text("parent_thread_id");
    let agent_nickname = text("agent_nickname");
    let agent_role = text("agent_role");
    let is_subagent = !parent_thread_id.is_empty()
        || !agent_nickname.is_empty()
        || !agent_role.is_empty()
        || p.get("source").is_some_and(|s| {
            s.get("subagent").is_some_and(|v| !v.is_null())
                || s.as_str().is_some_and(|s| !s.is_empty() && s != "cli")
        });
    Ok(CodexHeader {
        session_id,
        timestamp: p
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(rfc_timestamp)
            .unwrap_or_else(|| header_timestamp(&value["timestamp"])),
        cwd: PathBuf::from(text("cwd")),
        cli_version: text("cli_version"),
        parent_thread_id,
        agent_nickname,
        agent_role,
        is_subagent,
    })
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct FileStamp {
    size: u64,
    modified: Option<SystemTime>,
    identity: (u64, u64),
}
impl FileStamp {
    fn from(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        let identity = {
            use std::os::unix::fs::MetadataExt;
            (metadata.dev(), metadata.ino())
        };
        #[cfg(not(unix))]
        let identity = (0, 0);
        Self {
            size: metadata.len(),
            modified: metadata.modified().ok(),
            identity,
        }
    }
}
#[derive(Clone, Debug)]
struct Cached {
    stamp: FileStamp,
    descriptor: SessionDescriptor,
    header_time: i64,
    file_time: i64,
}
#[derive(Debug, Default)]
struct TranscriptScanner {
    root: PathBuf,
    cache: HashMap<PathBuf, Cached>,
    recursive: bool,
}
fn entries(path: &Path) -> io::Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(path)?.collect::<io::Result<Vec<_>>>()?;
    entries.sort_by_key(|e| e.file_name());
    Ok(entries)
}
impl TranscriptScanner {
    fn files(&self, token: &CancellationToken) -> io::Result<Vec<PathBuf>> {
        if self.root.as_os_str().is_empty() {
            return Ok(Vec::new());
        }
        let root = fs::canonicalize(&self.root).unwrap_or_else(|_| self.root.clone());
        let mut files = Vec::new();
        let mut directories = vec![(root, 0)];
        while let Some((directory, depth)) = directories.pop() {
            check_cancel(token)?;
            let listing = match entries(&directory) {
                Ok(e) => e,
                Err(e) if depth > 0 || e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            for entry in listing {
                check_cancel(token)?;
                let Ok(kind) = entry.file_type() else {
                    continue;
                };
                if kind.is_dir() && (self.recursive || depth == 0) {
                    directories.push((entry.path(), depth + 1));
                } else if kind.is_file()
                    && (self.recursive || depth == 1)
                    && entry.path().extension().is_some_and(|e| e == "jsonl")
                {
                    files.push(entry.path());
                }
            }
        }
        files.sort();
        Ok(files)
    }
    fn scan(
        &mut self,
        token: &CancellationToken,
        processes: &[super::ProcessInfo],
    ) -> io::Result<Vec<SessionDescriptor>> {
        check_cancel(token)?;
        let files = self.files(token)?;
        let mut seen = HashSet::new();
        let mut candidates = Vec::new();
        for path in files {
            check_cancel(token)?;
            let Ok(metadata) = fs::metadata(&path) else {
                continue;
            };
            let stamp = FileStamp::from(&metadata);
            if let Some(cached) = self.cache.get(&path).filter(|c| c.stamp == stamp) {
                seen.insert(path);
                candidates.push(cached.clone());
                continue;
            }
            let parsed = (|| -> io::Result<Cached> {
                let mut descriptor = SessionDescriptor {
                    transcript_path: path.clone(),
                    ..Default::default()
                };
                let header_time = if self.recursive {
                    let header = read_codex_session_header(&path)?;
                    descriptor.session_id = header.session_id;
                    descriptor.cwd = header.cwd;
                    descriptor.harness_version = header.cli_version;
                    descriptor.parent_thread_id = header.parent_thread_id;
                    descriptor.agent_nickname = header.agent_nickname;
                    descriptor.agent_role = header.agent_role;
                    descriptor.is_subagent = header.is_subagent;
                    header.timestamp
                } else {
                    let header = read_pi_session_header(&path)?;
                    descriptor.session_id = header.id;
                    descriptor.cwd = PathBuf::from(header.cwd);
                    descriptor.harness_version = header.version.to_string();
                    header_timestamp(&header.timestamp)
                };
                descriptor.cwd = clean_path(&descriptor.cwd);
                let file_time = mtime(&metadata);
                descriptor.last_active = extract_last_active_timestamp(&path, file_time)?;
                descriptor.started_at = if header_time > 0 {
                    header_time
                } else {
                    descriptor.last_active
                };
                if descriptor.last_active <= 0 {
                    descriptor.last_active = descriptor.started_at;
                }
                Ok(Cached {
                    stamp,
                    descriptor,
                    header_time,
                    file_time,
                })
            })();
            if let Ok(cached) = parsed {
                seen.insert(path.clone());
                self.cache.insert(path, cached.clone());
                candidates.push(cached);
            }
        }
        self.cache.retain(|path, _| seen.contains(path));
        candidates.sort_by(|a, b| {
            b.descriptor
                .last_active
                .max(b.file_time)
                .cmp(&a.descriptor.last_active.max(a.file_time))
                .then_with(|| b.header_time.cmp(&a.header_time))
        });
        let mut used = HashSet::new();
        let mut result = Vec::new();
        for cached in candidates {
            check_cancel(token)?;
            let mut descriptor = cached.descriptor;
            if let Some(process) = pair_process(
                &descriptor.cwd,
                cached.header_time,
                descriptor.last_active.max(cached.file_time),
                processes,
                &mut used,
            ) {
                descriptor.pid = process.pid;
                descriptor.is_active = true;
                if process.create_time > 0 {
                    descriptor.started_at = process.create_time;
                }
            }
            result.push(descriptor);
        }
        Ok(result)
    }
}
#[derive(Debug)]
pub struct PiScanner(TranscriptScanner);
impl PiScanner {
    pub fn new(root: PathBuf) -> Self {
        Self(TranscriptScanner {
            root: if root.as_os_str().is_empty() {
                super::default_pi_sessions_dir()
            } else {
                root
            },
            recursive: false,
            ..Default::default()
        })
    }
    pub fn scan(
        &mut self,
        token: &CancellationToken,
        snapshot: &ProcessSnapshot,
    ) -> io::Result<Vec<SessionDescriptor>> {
        self.0.scan(token, snapshot.pi_processes())
    }
}
#[derive(Debug)]
pub struct CodexScanner(TranscriptScanner);
impl CodexScanner {
    pub fn new(root: PathBuf) -> Self {
        Self(TranscriptScanner {
            root: if root.as_os_str().is_empty() {
                super::default_codex_sessions_dir()
            } else {
                root
            },
            recursive: true,
            ..Default::default()
        })
    }
    pub fn scan(
        &mut self,
        token: &CancellationToken,
        snapshot: &ProcessSnapshot,
    ) -> io::Result<Vec<SessionDescriptor>> {
        self.0.scan(token, snapshot.codex_processes())
    }
}
