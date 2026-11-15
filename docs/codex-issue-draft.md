**Title:** Codex Desktop: active goals can revisit a completed steer after mid-turn compaction

This is related to [#27731](https://github.com/openai/codex/issues/27731),
[#29811](https://github.com/openai/codex/issues/29811), and
[#31659](https://github.com/openai/codex/issues/31659). This note adds a
stable-message-identity observation on a newer Desktop runtime.

### Environment

- Codex Desktop on Linux
- Bundled runtime: `codex-cli 0.169.0-alpha.11.1`
- Local paginated history, automatic compaction during an active goal

### Reproduction

1. In a disposable chat, ask Codex to perform request A (for example, generate
   a synthetic fixture and verify its checksum). Verify that A is finished.
2. Set an independent goal B and let it advance automatically. Submit no
   further chat messages.
3. Let an automatic continuation compact in the middle of a turn. Check what
   the assistant works on after compaction. Repeat across several compactions.

### Observed

Across seven compactions in one affected chat, replacement history retained
the same already-addressed user item ID, as well as an older attachment. The
inline goal continuation was present at the start of the automatic turn but
absent from the replacement history. The assistant subsequently revisited the
settled work. This wasn't seven new user submissions. The observed history
doesn't establish that every revisit caused a fresh build.

The separate, synthetic loopback fixture in this repository verifies that
the first request after mid-turn compaction retains ordinary user content and
can receive additional developer context from a compact-session hook. That
fixture verifies message delivery; it does not measure live model behavior.

### Expected

Persist which user inputs were admitted to the current turn and which are
already handled. After compaction, keep handled inputs as history and
regenerate continuation from the current persisted goal. New steers must stay
actionable, including ones submitted during compaction. Identical text under
a newly admitted ID should be treated as a new submission.

A regression case should test a completed request followed by an unrelated
active goal across two compactions, then a genuine new steer and a paused or
replaced goal. Assert the active vs. historical input classification in the
first resumed model request.
