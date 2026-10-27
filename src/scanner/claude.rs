use super::{
    ProcessSnapshot, SessionDescriptor, check_cancel, default_claude_projects_dirs,
    default_claude_session_dirs,
};
use serde::Deserialize;
use std::{
    collections::HashSet,
    fs, io,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubagentTranscript {
    pub subagent_id: String,
    pub path: PathBuf,
}
#[derive(Debug)]
pub struct ClaudeCodeScanner {
    session_dirs: Vec<PathBuf>,
    projects_dirs: Vec<PathBuf>,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct DescriptorFile {
    pid: i32,
    session_id: String,
    cwd: PathBuf,
    status: String,
    transcript_path: PathBuf,
    started_at: i64,
    version: String,
    name: String,
    name_source: String,
}
fn listing(path: &Path) -> io::Result<Vec<fs::DirEntry>> {
    let mut result = fs::read_dir(path)?.collect::<io::Result<Vec<_>>>()?;
    result.sort_by_key(|e| e.file_name());
    Ok(result)
}
fn safe_id(id: &str) -> bool {
    !id.is_empty() && !id.contains(['/', '\\']) && id != "." && id != ".."
}
impl ClaudeCodeScanner {
    pub fn new(session_dirs: Vec<PathBuf>) -> Self {
        Self {
            session_dirs: if session_dirs.is_empty() {
                default_claude_session_dirs()
            } else {
                session_dirs
            },
            projects_dirs: default_claude_projects_dirs(),
        }
    }
    pub fn set_projects_dirs(&mut self, paths: Vec<PathBuf>) {
        self.projects_dirs = paths;
    }
    pub fn scan(
        &self,
        token: &CancellationToken,
        snapshot: &ProcessSnapshot,
    ) -> io::Result<Vec<SessionDescriptor>> {
        check_cancel(token)?;
        let mut used = HashSet::new();
        let mut result = Vec::new();
        for root in &self.session_dirs {
            let root = fs::canonicalize(root).unwrap_or_else(|_| root.clone());
            let Ok(entries) = listing(&root) else {
                continue;
            };
            for entry in entries {
                check_cancel(token)?;
                if entry.file_type().is_ok_and(|t| t.is_dir())
                    || entry.path().extension().is_none_or(|e| e != "json")
                {
                    continue;
                }
                let Ok(bytes) = fs::read(entry.path()) else {
                    continue;
                };
                let Ok(raw) = serde_json::from_slice::<DescriptorFile>(&bytes) else {
                    continue;
                };
                if used.contains(&raw.pid)
                    || !snapshot.verify_process_match(raw.pid, raw.started_at)
                {
                    continue;
                }
                let mut descriptor = SessionDescriptor {
                    pid: raw.pid,
                    session_id: raw.session_id,
                    cwd: raw.cwd,
                    status: raw.status,
                    transcript_path: raw.transcript_path,
                    started_at: raw.started_at,
                    harness_version: raw.version,
                    name: raw.name,
                    name_source: raw.name_source,
                    is_active: true,
                    ..Default::default()
                };
                if descriptor.transcript_path.as_os_str().is_empty() {
                    descriptor.transcript_path =
                        self.locate_transcript(&descriptor.session_id, &descriptor.cwd);
                }
                used.insert(descriptor.pid);
                result.push(descriptor);
            }
        }
        Ok(result)
    }
    fn locate_transcript(&self, id: &str, cwd: &Path) -> PathBuf {
        if !safe_id(id) {
            return PathBuf::new();
        }
        let filename = format!("{id}.jsonl");
        let roots: Vec<_> = self
            .projects_dirs
            .iter()
            .map(|p| fs::canonicalize(p).unwrap_or_else(|_| p.clone()))
            .collect();
        if !cwd.as_os_str().is_empty() {
            let mut slug = cwd.to_string_lossy().replace(['/', '.'], "-");
            if !slug.starts_with('-') {
                slug.insert(0, '-')
            }
            for root in &roots {
                for slug in [&slug, slug.trim_start_matches('-')] {
                    let path = root.join(slug).join(&filename);
                    if path.is_file() {
                        return path;
                    }
                }
            }
        }
        for root in roots {
            if let Ok(entries) = listing(&root) {
                for entry in entries {
                    if entry.file_type().is_ok_and(|t| t.is_dir()) {
                        let path = entry.path().join(&filename);
                        if path.is_file() {
                            return path;
                        }
                    }
                }
            }
        }
        PathBuf::new()
    }
    pub fn discover_subagents(
        &self,
        descriptor: &SessionDescriptor,
    ) -> io::Result<Vec<SubagentTranscript>> {
        if descriptor.transcript_path.as_os_str().is_empty() {
            return Ok(Vec::new());
        }
        let directory = descriptor
            .transcript_path
            .parent()
            .unwrap_or(Path::new("."));
        let mut roots = Vec::new();
        if safe_id(&descriptor.session_id) {
            roots.push(directory.join(&descriptor.session_id).join("subagents"));
        }
        roots.push(directory.join("subagents"));
        let mut result = Vec::new();
        for root in roots {
            if let Ok(entries) = listing(&root) {
                for entry in entries {
                    let path = entry.path();
                    if !entry.file_type().is_ok_and(|t| t.is_dir())
                        && path.extension().is_some_and(|e| e == "jsonl")
                    {
                        result.push(SubagentTranscript {
                            subagent_id: path
                                .file_stem()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .into_owned(),
                            path,
                        });
                    }
                }
            }
        }
        Ok(result)
    }
}
