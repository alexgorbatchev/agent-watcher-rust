use super::{check_cancel, paths::clean_path};
use std::{collections::BTreeMap, io, path::PathBuf};
use sysinfo::{ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System, UpdateKind};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcessMeta {
    pub pid: i32,
    pub name: String,
    pub create_time: i64,
    pub alive: bool,
    pub cwd: PathBuf,
    pub cmdline: Vec<String>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcessInfo {
    pub pid: i32,
    pub cwd: PathBuf,
    pub create_time: i64,
}

/// Captured process information, independent of subsequent process churn.
#[derive(Clone, Debug, Default)]
pub struct ProcessSnapshot {
    processes: BTreeMap<i32, ProcessMeta>,
    pi: Vec<ProcessInfo>,
    codex: Vec<ProcessInfo>,
    opencode: Vec<ProcessInfo>,
}
pub trait SnapshotProvider: Send + Sync {
    fn snapshot(&self, token: &CancellationToken) -> io::Result<ProcessSnapshot>;
}
impl<F> SnapshotProvider for F
where
    F: Fn(&CancellationToken) -> io::Result<ProcessSnapshot> + Send + Sync,
{
    fn snapshot(&self, token: &CancellationToken) -> io::Result<ProcessSnapshot> {
        self(token)
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct LiveProcessProvider;
impl SnapshotProvider for LiveProcessProvider {
    fn snapshot(&self, token: &CancellationToken) -> io::Result<ProcessSnapshot> {
        ProcessSnapshot::capture(token)
    }
}
fn basename(name: &str) -> String {
    name.rsplit(['/', '\\'])
        .next()
        .unwrap_or(name)
        .to_ascii_lowercase()
        .trim_end_matches(".exe")
        .into()
}
fn plausible(name: &str) -> bool {
    matches!(
        name,
        "pi" | "codex" | "codex-tui" | "codex-cli" | "opencode" | "claude"
    ) || interpreter(name)
}
fn interpreter(name: &str) -> bool {
    ["node", "bun", "electron"]
        .iter()
        .any(|prefix| name.starts_with(prefix))
}
impl ProcessSnapshot {
    pub fn capture(token: &CancellationToken) -> io::Result<Self> {
        check_cancel(token)?;
        let mut system = System::new();
        system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().without_tasks(),
        );
        check_cancel(token)?;
        let selected: Vec<_> = system
            .processes()
            .iter()
            .filter(|(_, p)| {
                let name = basename(&p.name().to_string_lossy());
                plausible(&name) && name != "claude"
            })
            .map(|(pid, _)| *pid)
            .collect();
        if !selected.is_empty() {
            system.refresh_processes_specifics(
                ProcessesToUpdate::Some(&selected),
                true,
                ProcessRefreshKind::nothing()
                    .without_tasks()
                    .with_cmd(UpdateKind::Always)
                    .with_cwd(UpdateKind::Always),
            );
        }
        let mut processes = Vec::with_capacity(system.processes().len());
        for (pid, p) in system.processes() {
            check_cancel(token)?;
            let Ok(pid) = i32::try_from(pid.as_u32()) else {
                continue;
            };
            processes.push(ProcessMeta {
                pid,
                name: p.name().to_string_lossy().into_owned(),
                create_time: i64::try_from(p.start_time())
                    .unwrap_or(i64::MAX)
                    .saturating_mul(1000),
                alive: !matches!(p.status(), ProcessStatus::Zombie | ProcessStatus::Dead),
                cwd: p.cwd().map_or_else(PathBuf::new, |p| p.to_path_buf()),
                cmdline: p
                    .cmd()
                    .iter()
                    .map(|s| s.to_string_lossy().into_owned())
                    .collect(),
            });
        }
        Ok(Self::from_processes(processes))
    }
    pub fn from_processes(processes: impl IntoIterator<Item = ProcessMeta>) -> Self {
        let mut snapshot = Self::default();
        for mut p in processes {
            if !p.cwd.as_os_str().is_empty() {
                p.cwd = clean_path(&p.cwd)
            }
            snapshot.processes.insert(p.pid, p);
        }
        for p in snapshot.processes.values() {
            if p.pid <= 0 || !p.alive || p.cwd.as_os_str().is_empty() {
                continue;
            }
            let name = basename(&p.name);
            if !plausible(&name) {
                continue;
            }
            let names: Vec<_> = if interpreter(&name) {
                p.cmdline.iter().map(|s| basename(s)).collect()
            } else {
                vec![name]
            };
            let info = ProcessInfo {
                pid: p.pid,
                cwd: p.cwd.clone(),
                create_time: p.create_time,
            };
            if names.iter().any(|n| n == "pi") {
                snapshot.pi.push(info.clone())
            }
            if names
                .iter()
                .any(|n| matches!(n.as_str(), "codex" | "codex-cli" | "codex-tui"))
            {
                snapshot.codex.push(info.clone())
            }
            if names.iter().any(|n| n == "opencode") {
                snapshot.opencode.push(info)
            }
        }
        snapshot
    }
    pub fn process(&self, pid: i32) -> Option<&ProcessMeta> {
        self.processes.get(&pid)
    }
    /// Iterate the captured metadata without re-reading a changing process table.
    pub fn processes(&self) -> impl ExactSizeIterator<Item = &ProcessMeta> {
        self.processes.values()
    }
    /// Associate an explicitly selected harness with a captured live process.
    ///
    /// The caller supplies its observed working directory and exact captured start
    /// time. This permits alias/interpreter launches without changing process names
    /// or command lines. A subsequent declaration replaces the same PID's candidate.
    /// Claude Code discovers processes through its own PID descriptors instead.
    pub fn declare_candidate(
        &mut self,
        harness: crate::Harness,
        mut candidate: ProcessInfo,
    ) -> io::Result<()> {
        if candidate.pid <= 0
            || candidate.cwd.as_os_str().is_empty()
            || !self.process(candidate.pid).is_some_and(|process| {
                process.alive && process.create_time == candidate.create_time
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "candidate must identify a captured live process with its exact start time and working directory",
            ));
        }
        let candidates = match harness {
            crate::Harness::Pi => &mut self.pi,
            crate::Harness::Codex => &mut self.codex,
            crate::Harness::Opencode => &mut self.opencode,
            crate::Harness::ClaudeCode => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Claude Code uses PID descriptors",
                ));
            }
        };
        candidate.cwd = clean_path(&candidate.cwd);
        if let Some(existing) = candidates.iter_mut().find(|p| p.pid == candidate.pid) {
            *existing = candidate;
        } else {
            candidates.push(candidate);
        }
        Ok(())
    }
    pub fn is_process_alive(&self, pid: i32) -> bool {
        pid > 0 && self.process(pid).is_some_and(|p| p.alive)
    }
    pub fn verify_process_match(&self, pid: i32, expected_start: i64) -> bool {
        self.is_process_alive(pid)
            && self.process(pid).is_some_and(|p| {
                expected_start <= 0
                    || p.create_time <= 0
                    || p.create_time.abs_diff(expected_start) < 60_000
            })
    }
    pub fn pi_processes(&self) -> &[ProcessInfo] {
        &self.pi
    }
    pub fn codex_processes(&self) -> &[ProcessInfo] {
        &self.codex
    }
    pub fn opencode_processes(&self) -> &[ProcessInfo] {
        &self.opencode
    }
}

pub(super) fn pair_process(
    cwd: &std::path::Path,
    created: i64,
    recent: i64,
    processes: &[ProcessInfo],
    used: &mut std::collections::HashSet<i32>,
) -> Option<ProcessInfo> {
    let found = processes
        .iter()
        .filter(|p| !used.contains(&p.pid) && p.cwd == clean_path(cwd))
        .filter_map(|p| {
            let mut difference = i128::from(created) - i128::from(p.create_time);
            let resumed = i128::from(recent) - i128::from(p.create_time);
            if difference < -5000 && resumed >= -5000 {
                difference = resumed
            }
            (difference >= -5000).then_some((difference.unsigned_abs(), p))
        })
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, p)| p.clone());
    if let Some(p) = &found {
        used.insert(p.pid);
    }
    found
}
