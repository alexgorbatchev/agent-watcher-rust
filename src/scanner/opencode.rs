use super::{
    ProcessSnapshot, SessionDescriptor, check_cancel, paths::clean_path, process::pair_process,
};
use agent_parser::OpencodeDb;
use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};
use tokio_util::sync::CancellationToken;
#[derive(Debug)]
pub struct OpencodeScanner {
    path: PathBuf,
}
impl OpencodeScanner {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path: if path.as_os_str().is_empty() {
                super::default_opencode_db_path()
            } else {
                path
            },
        }
    }
    pub fn scan(
        &self,
        token: &CancellationToken,
        snapshot: &ProcessSnapshot,
    ) -> io::Result<Vec<SessionDescriptor>> {
        check_cancel(token)?;
        if self.path.as_os_str().is_empty() {
            return Ok(Vec::new());
        }
        match fs::metadata(&self.path) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        }
        let db = OpencodeDb::open(&self.path).map_err(io::Error::other)?;
        let interrupt = db.interrupt_handle();
        let done = AtomicBool::new(false);
        let mut sessions = thread::scope(|scope| {
            let worker = scope.spawn(|| {
                while !done.load(Ordering::Acquire) {
                    if token.is_cancelled() {
                        interrupt.interrupt();
                    }
                    thread::park_timeout(Duration::from_millis(10));
                }
            });
            let result = db.get_sessions();
            done.store(true, Ordering::Release);
            worker.thread().unpark();
            result
        })
        .map_err(|e| {
            if token.is_cancelled() {
                io::Error::new(io::ErrorKind::Interrupted, e)
            } else {
                io::Error::other(e)
            }
        })?;
        check_cancel(token)?;
        sessions.sort_by(|a, b| {
            b.time_updated
                .cmp(&a.time_updated)
                .then_with(|| b.time_created.cmp(&a.time_created))
        });
        let mut used = HashSet::new();
        let mut descriptors = Vec::with_capacity(sessions.len());
        for session in &sessions {
            check_cancel(token)?;
            let mut descriptor = SessionDescriptor {
                session_id: session.id.clone(),
                cwd: clean_path(&PathBuf::from(&session.directory)),
                started_at: session.time_created,
                last_active: session.time_updated,
                updated_at: session.time_updated,
                harness_version: session.version.clone(),
                parent_id: session.parent_id.clone().unwrap_or_default(),
                is_subagent: session.is_subagent,
                title: session.title.clone(),
                ..Default::default()
            };
            if !session.is_archived
                && descriptor.parent_id.is_empty()
                && let Some(process) = pair_process(
                    &descriptor.cwd,
                    session.time_created,
                    session.time_updated,
                    snapshot.opencode_processes(),
                    &mut used,
                )
            {
                descriptor.pid = process.pid;
                descriptor.is_active = true;
                if process.create_time > 0 {
                    descriptor.started_at = process.create_time;
                }
            }
            descriptors.push(descriptor);
        }
        let indices: HashMap<_, _> = descriptors
            .iter()
            .enumerate()
            .map(|(i, d)| (d.session_id.clone(), i))
            .collect();
        for index in 0..descriptors.len() {
            check_cancel(token)?;
            if sessions[index].is_archived || descriptors[index].parent_id.is_empty() {
                continue;
            }
            let mut seen = HashSet::from([index]);
            let mut cursor = index;
            while let Some(&parent) = indices.get(&descriptors[cursor].parent_id) {
                if !seen.insert(parent) || sessions[parent].is_archived {
                    break;
                }
                if descriptors[parent].is_active {
                    descriptors[index].is_active = true;
                    descriptors[index].pid = descriptors[parent].pid;
                    break;
                }
                cursor = parent;
            }
        }
        Ok(descriptors)
    }
}
