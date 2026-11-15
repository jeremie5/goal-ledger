# Goal Ledger

Goal Ledger is a local task ledger for user prompts and mid-turn steers in
Codex. At startup, resume, and context compaction, it puts the most recent
known user request and its status first, followed by earlier unfinished work
and recent completions. It stores state in SQLite and exposes it through MCP.

For example, you might ask Codex to switch from task A to task B. After B is
completed and Codex resumes, the ledger says B is done and shows what is still
open on A. Automatic goals make the replay bug easier to see because Codex can
start the next turn without a new user message. See the
[upstream Codex report](https://github.com/openai/codex/issues/27731).
Goal Ledger is an independent project.

## Install

Requires Rust 1.85 or later and a C compiler for the bundled SQLite build.

```sh
cargo install --path . --locked
command -v goal-ledger
```

1. Copy the event groups from [hooks.example.json](hooks.example.json) into
   your existing `~/.codex/hooks.json`. Set each command to the absolute binary
   path from the previous step. Keep existing hook groups.
2. Register the MCP tools with `codex mcp add goal-ledger -- /absolute/path/to/goal-ledger mcp`.
   Or add the following to the Codex configuration used by your app:

   ```toml
   [mcp_servers.goal-ledger]
   command = "/absolute/path/to/goal-ledger"
   args = ["mcp"]
   ```

3. Review and trust the new hooks in Codex **Settings → Hooks**, or with
   `/hooks` in the CLI. If needed, reopen or resume a chat for it to load the
   new MCP connection. The
   [Codex hooks guide](https://learn.chatgpt.com/docs/hooks#sessionstart)
   describes the required immediate `SessionStart(source=compact)` behavior.

If you’re upgrading from 0.1.0, reinstall with `cargo install --path . --locked`,
add the `^(startup|resume)$` entry to `SessionStart`, and trust the new hook.
Goal Ledger migrates the existing database and preserves recorded request status.

The default database is `$XDG_STATE_HOME/goal-ledger/tasks.sqlite3`, or
`~/.local/state/goal-ledger/tasks.sqlite3`. To use a different database, set
`GOAL_LEDGER_DB` to the same local path in both the MCP server environment
and the hook commands. In Codex, set it under
`[mcp_servers.goal-ledger.env]` for the MCP process. No account, network
listener, or model API key is required by Goal Ledger.

## Track a request

The prompt hook gives Codex the ledger session ID. Use `request_latest` to
find the most recent admitted prompt or steer and its recorded status.
`requests_list` lists earlier requests newest first, with a status
filter and a cursor for older pages. `request_get` fetches an exact request
and its revision.

Save terse fragments with `request_update`. Use one constraint per requirement
and one concrete next action. Keep the outcome, blockers, and exact evidence
references. Mark work `completed` with evidence such as an artifact, commit,
test receipt, or job result. For example:

```json
{
  "session_id": "your-codex-session-id",
  "request_id": "the-observed-message-id",
  "expected_revision": 1,
  "status": "completed",
  "summary": "Bracket exported. Fit passed.",
  "evidence": ["reports/bracket-fit.json"]
}
```

An `observed` entry means the ledger has an ID but no recorded task outcome.
The agent records progress and completion explicitly. Two different inputs
with the same words can have different IDs, including a later request to
repeat completed work.

## What the adapter can verify

- It imports original user items from a bounded local rollout tail. It uses a
  native message ID where available, or a file-and-offset reference for legacy
  records. Nested retained history is never imported as a new submission.
- It stores IDs, status and explicitly supplied bookkeeping fields. It does
  not save prompt text or attachments. The local database records transcript
  paths and may contain sensitive summaries or evidence references; keep it
  local.
- At startup, resume, or immediately after compaction, the hook sends bounded
  developer context with the latest user request's recorded status, including
  an `observed` status. A new steer can change what should happen next.

Recovery context is advisory. On an automatic turn that starts without a
fresh prompt or a session hook, Codex can refresh the latest status using
`request_latest`. Very large or unavailable rollout records, changes in
rollout format, and transports without a local transcript can leave IDs
unobserved. The ledger relies on explicit completion records. Both hooks
and MCP work locally, so recovery delivery can be tested with a loopback model.

## Verify

```sh
cargo test --locked
cargo build --locked
python3 tests/desktop_compaction.py --codex "$(command -v codex)" --ledger target/debug/goal-ledger
python3 tests/steer_compaction.py --codex "$(command -v codex)" --ledger target/debug/goal-ledger
```

Use a Codex binary that supports the immediate compact-session hook. The
synthetic runtime test uses an isolated temporary home and a loopback Responses
mock. It checks what reaches the first real model request after auto-compaction;
it does not evaluate how a live model behaves.

The steer fixture sends a real `turn/steer` while the original request is in
flight. It marks the steer complete before automatic compaction and verifies
that the first resumed request sees that completion while the original work
remains in progress and queryable.

The loopback test was verified on Linux with the Codex Desktop bundle
`0.169.0-alpha.11.1`. The [upstream issue draft](docs/codex-issue-draft.md)
describes the stale input boundary separately from the local workaround.

In a matched live `/goal` comparison on 0.1.0, the completed action ran twice
with Goal Ledger disabled and once with it enabled. Both goals finished.
In a separate 0.2.0 spot check, all six active chats continued working. None
compacted during that window, so the spot check could only assess visible work
progress. Replay prevention in long-running chats under natural compaction
remains unverified.

To uninstall, remove the Goal Ledger hook groups and the
`mcp_servers.goal-ledger` config entry. Keep or delete the local database as
needed.
