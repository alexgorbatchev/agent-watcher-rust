---
created_on: 2026-10-14 13:35
last_modified: 2026-10-14 13:35
status: current
---

# agent-watcher

Reusable Rust session discovery, lifecycle observation, and acknowledged transcript/database delivery. Consumers own telemetry, UI, and network delivery. Read the parent agent instructions and applicable skills before changing code.

## Commands

- Build: `just build`.
- Behavioral tests and doctests: `just test`.
- Format this package: `just fmt`.
- Format check and strict Clippy: `just lint`.
- At least 90% line coverage: `just coverage`.
- All checks: `just check`.

## Contracts

- Keep parsing in `agent-parser`. Use its read-only OpenCode API and transactional snapshots.
- Capture one shared process snapshot per discovery tick; inspect harness storage without modifying or locking it.
- The runtime owns its workers and joins them before returning on cancellation. Concurrent handlers must honor their cancellation token.
- An acknowledgement on the last event from a transcript line confirms the entire line. Commit only a contiguous acknowledged prefix, binding each callback to its file generation.
- Replacement and truncation begin a new generation. Load persisted byte offsets only at initial attachment; never resurrect a previous generation's cursor during an in-memory reset.
- Keep separate OpenCode revision checkpoints. Emit completion usage only after a completion marker, once per acknowledged assistant message across restarts. Drain final database activity before lifecycle exit.
- Preserve existing transcripts with matching device and inode regardless of checkpoint age. Prune deleted/replaced files; apply age retention to inaccessible files.
- Use native structured tracing. Return initialization failures, report runtime failures, and preserve retriable progress. Report corrupt state rather than silently replacing it.
- Each concurrent runtime requires separate state paths; there is no inter-process checkpoint locking. Use random atomic temporary files for persistence.
- Keep byte-cursor and wire schemas synchronized with their consumers. Read existing persisted fixtures before modifying formats.
- Accepted, unacknowledged events remain pending and replay after restart. A rejected event must not be acknowledged.
- Use injectable process snapshots in tests rather than a global environment bypass of process verification.
- Keep all fixtures, evidence, and temporary files inside this project's `.tmp/`; never read unrelated credentials or mutate real harness sessions in tests.
