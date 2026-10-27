use std::{
    env,
    path::{Component, Path, PathBuf},
};
pub(super) fn clean_path(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if result.file_name().is_some_and(|n| n != "..") {
                    result.pop();
                } else if !result.has_root() {
                    result.push("..");
                }
            }
            other => result.push(other.as_os_str()),
        }
    }
    if result.as_os_str().is_empty() {
        result.push(".")
    }
    result
}
fn variable(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
}
fn home() -> PathBuf {
    dirs::home_dir().unwrap_or_default()
}
fn data() -> PathBuf {
    variable("XDG_DATA_HOME").unwrap_or_else(|| home().join(".local/share"))
}
pub fn default_claude_session_dirs() -> Vec<PathBuf> {
    let mut paths: Vec<_> = variable("CLAUDE_SESSIONS_DIR").into_iter().collect();
    paths.extend([
        data().join("ai-registry/claude-code/sessions"),
        home().join(".claude/sessions"),
    ]);
    paths
}
pub fn default_claude_projects_dirs() -> Vec<PathBuf> {
    let mut paths: Vec<_> = variable("CLAUDE_PROJECTS_DIR").into_iter().collect();
    paths.extend([
        data().join("ai-registry/claude-code/projects"),
        home().join(".claude/projects"),
    ]);
    paths
}
pub fn default_pi_sessions_dir() -> PathBuf {
    variable("PI_SESSIONS_DIR").unwrap_or_else(|| {
        variable("PI_CODING_AGENT_DIR")
            .unwrap_or_else(|| home().join(".pi/agent"))
            .join("sessions")
    })
}
pub fn default_codex_sessions_dir() -> PathBuf {
    variable("CODEX_SESSIONS_DIR").unwrap_or_else(|| {
        variable("CODEX_HOME")
            .unwrap_or_else(|| home().join(".codex"))
            .join("sessions")
    })
}
pub fn default_opencode_db_path() -> PathBuf {
    variable("OPENCODE_DB_PATH").unwrap_or_else(|| data().join("opencode/opencode.db"))
}
pub fn default_session_tracker_path() -> PathBuf {
    variable("XDG_CACHE_HOME")
        .unwrap_or_else(|| home().join(".cache"))
        .join("agent-watcher/observer-sessions.json")
}
pub fn encode_pi_cwd(cwd: &Path) -> String {
    format!(
        "--{}--",
        clean_path(cwd)
            .to_string_lossy()
            .trim_start_matches(['/', '\\'])
            .replace(['/', '\\', ':'], "-")
    )
}
