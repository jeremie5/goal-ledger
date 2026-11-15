# Goal Ledger

Goal Ledger helps Codex resume the right work after context compaction. It records
user request identities in local SQLite and puts a short recovery packet in the
first model request after compaction. The project is independent of OpenAI.

When a long-running goal compacts, Codex can retain an old user instruction and
drop the inline goal continuation. A completed request may then look like a new
assignment. Goal Ledger distinguishes **observed input** from **open work** and
**completed work**, using the same ledger across context windows. See
[the upstream Codex report](https://github.com/openai/codex/issues/27731).

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

The default database is `$XDG_STATE_HOME/goal-ledger/tasks.sqlite3`, or
`~/.local/state/goal-ledger/tasks.sqlite3`. To use a different database, set
`GOAL_LEDGER_DB` to the same local path in both the MCP server environment
and the hook commands. In Codex, set it under
`[mcp_servers.goal-ledger.env]` for the MCP process. No account, network
listener, or model API key is required by Goal Ledger.

## Track a request

The user prompt hook gives Codex the ledger session ID. For actionable work,
find the input with `requests_list`, then use `request_update` to record a
summary, requirements, status, and the next action. When the work is done,
record `completed` with evidence such as an artifact, commit, test receipt, or
job result. `request_get` fetches the current record and revision. Example:

```json
{
  "session_id": "your-codex-session-id",
  "request_id": "the-observed-message-id",
  "expected_revision": 1,
  "status": "completed",
  "summary": "Generated the bracket once and checked the fit",
  "evidence": ["reports/bracket-fit.json"]
}
```

An observed request stays observed until it is explicitly tracked. An assistant
reply does not mark a task complete. Two different inputs with the same words
can still be separate requests. Repeating a completed request intentionally is
new work under the newly admitted ID.

## What the adapter can verify

- It imports original user items from a bounded local rollout tail. It uses a
  native message ID where available, or a file-and-offset reference for legacy
  records. Nested retained history is never imported as a new submission.
- It stores IDs, status and explicitly supplied bookkeeping fields. It does
  not save prompt text or attachments. The local database records transcript
  paths and may contain sensitive summaries or evidence references; keep it
  local.
- Immediately after compaction, the hook sends bounded developer context
  identifying recent unfinished and completed requests. It does not remove,
  rewrite, or block any user message. A genuine new steer retains precedence.

This is an advisory mitigation. A hook cannot change Codex's internal input
queue or guarantee the model will follow the ledger. Very large or unavailable
rollout records, changed internal formats, and transports without a local
transcript can leave IDs unobserved. The hook never guesses completion from
missing history. Both the tools and the hook work locally, so delivery can be
tested without sending private transcripts to a model.

## Verify

```sh
cargo test --locked
cargo build --locked
python3 tests/desktop_compaction.py --codex "$(command -v codex)" --ledger target/debug/goal-ledger
```

Use a Codex binary that supports the immediate compact-session hook. The
synthetic runtime test uses an isolated temporary home and a loopback Responses
mock. It checks what reaches the first real model request after auto-compaction;
it does not evaluate how a live model behaves.

The loopback test was verified on Linux with the Codex Desktop bundle
`0.169.0-alpha.11.1`. The [upstream issue draft](docs/codex-issue-draft.md)
describes the stale input boundary separately from the local workaround.

To uninstall, remove only the four Goal Ledger hook groups and the
`mcp_servers.goal-ledger` config entry. Keep or delete the local database as
needed.
