//! Bounded parsing of original user inputs from Codex rollout JSONL.
//!
//! Rollout byte offsets define input order even when wall-clock time changes.

use serde_json::Value;
use std::collections::VecDeque;

const MAX_REQUESTS_PER_SCAN: usize = 48;
const MAX_NATIVE_ID_BYTES: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AdmittedInput {
    pub request_id: String,
    pub turn_id: Option<String>,
    pub byte_offset: u64,
}

/// Parse the latest admitted user inputs from a bounded rollout tail.
///
/// Only top-level `response_item` user messages with actual user content are
/// admitted. Replacement history nested under compaction events is ignored.
/// Results are in original JSONL order, even when their timestamps are absent
/// or disagree. Inputs without a native message ID receive a rollout-and-offset
/// identity, so equal text at different positions remains distinct.
pub(crate) fn parse_admitted_inputs(
    rollout_name: &str,
    base_offset: u64,
    bytes: &[u8],
) -> Vec<AdmittedInput> {
    let mut offset = base_offset;
    let mut turn_id = None;
    let mut latest = VecDeque::with_capacity(MAX_REQUESTS_PER_SCAN);

    // Ignore incomplete JSONL records still being appended at EOF.
    for line in bytes.split_inclusive(|&byte| byte == b'\n') {
        let position = offset;
        offset = offset.saturating_add(line.len() as u64);
        if !line.ends_with(b"\n") {
            break;
        }

        let Ok(event) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        let event_kind = event["type"].as_str().unwrap_or("");
        let payload = &event["payload"];

        if event_kind == "event_msg" {
            match payload["type"].as_str() {
                Some("task_started" | "turn_started") => {
                    turn_id = payload["turn_id"].as_str().map(str::to_owned);
                }
                Some("task_complete" | "turn_completed") => turn_id = None,
                _ => {}
            }
        }

        if event_kind != "response_item"
            || payload["type"] != "message"
            || payload["role"] != "user"
            || !actual_input(payload)
        {
            continue;
        }

        let request_id = native_id(payload).unwrap_or_else(|| {
            format!(
                "rollout:{:016x}:{position:x}",
                file_fingerprint(rollout_name)
            )
        });
        if latest.len() == MAX_REQUESTS_PER_SCAN {
            latest.pop_front();
        }
        latest.push_back(AdmittedInput {
            request_id,
            turn_id: turn_id.clone(),
            byte_offset: position,
        });
    }

    latest.into_iter().collect()
}

fn native_id(payload: &Value) -> Option<String> {
    payload["id"]
        .as_str()
        .filter(|id| !id.trim().is_empty() && id.len() <= MAX_NATIVE_ID_BYTES)
        .map(str::to_owned)
}

fn file_fingerprint(rollout_name: &str) -> u64 {
    // FNV-1a names a legacy rollout without retaining its path or content.
    rollout_name.bytes().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}

fn actual_input(payload: &Value) -> bool {
    let Some(parts) = payload["content"].as_array() else {
        return false;
    };
    let kinds =
        payload["internal_chat_message_metadata_passthrough"]["content_item_kinds"].as_array();

    let mut has_input = false;
    for (index, part) in parts.iter().enumerate() {
        if kinds
            .and_then(|items| items.get(index))
            .and_then(Value::as_str)
            .is_some_and(internal_item_kind)
        {
            continue;
        }

        match part["type"].as_str() {
            Some("input_text") => {
                let text = part["text"].as_str().unwrap_or("").trim_start();
                if internal_envelope(text) {
                    continue;
                }
                has_input |= !text.trim().is_empty();
            }
            Some("input_image" | "input_file" | "input_audio") => has_input = true,
            _ => {}
        }
    }
    has_input
}

fn internal_item_kind(kind: &str) -> bool {
    let kind = kind.to_ascii_lowercase();
    kind.ends_with(".internal_context")
        || kind.starts_with("hook.")
        || kind.contains("retained_history")
        || kind.contains("compaction")
        || kind.starts_with("goal_context")
        || kind.starts_with("environment_context")
}

fn internal_envelope(text: &str) -> bool {
    [
        "<codex_internal_context",
        "<environment_context",
        "<user_instructions",
        "<subagent_notification",
        "<collaboration_mode",
        "<skills_instructions",
        "<permissions instructions>",
        "<goal_context>",
        "<hook_prompt",
    ]
    .iter()
    .any(|prefix| text.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(kind: &str, payload: Value) -> String {
        format!("{}\n", json!({"type":kind,"payload":payload}))
    }

    fn user(id: Option<&str>, text: &str) -> Value {
        json!({"type":"message", "id":id, "role":"user",
            "content":[{"type":"input_text","text":text}]})
    }

    fn timestamped(timestamp: &str, payload: Value) -> String {
        format!(
            "{}\n",
            json!({"type":"response_item","timestamp":timestamp,"payload":payload})
        )
    }

    #[test]
    fn keeps_native_prompt_and_identical_steer_ids_in_original_input_order() {
        let prompt = timestamped(
            "2026-11-15T12:00:00.000Z",
            user(Some("native-prompt"), "Do the same work"),
        );
        let steer = timestamped(
            "2026-11-15T11:59:59.000Z",
            user(Some("native-steer"), "Do the same work"),
        );
        let bytes = format!(
            "{}{}{}",
            event(
                "event_msg",
                json!({"type":"task_started","turn_id":"turn-1"})
            ),
            prompt,
            steer,
        );

        let inputs = parse_admitted_inputs("rollout-a.jsonl", 0, bytes.as_bytes());

        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].request_id, "native-prompt");
        assert_eq!(inputs[1].request_id, "native-steer");
        assert!(inputs[0].byte_offset < inputs[1].byte_offset);
        assert_eq!(inputs[0].turn_id.as_deref(), Some("turn-1"));
        assert_eq!(inputs[1].turn_id.as_deref(), Some("turn-1"));
    }

    #[test]
    fn excludes_internal_goal_environment_hook_and_retained_compaction_content() {
        let ordinary = event(
            "response_item",
            user(Some("native-real"), "Original user request"),
        );
        let internal = [
            "<codex_internal_context source=\"goal\">Resume the goal",
            "<environment_context>Working directory",
            "<hook_prompt>Continue automatically",
        ]
        .into_iter()
        .enumerate()
        .map(|(index, text)| {
            event(
                "response_item",
                user(Some(&format!("internal-{index}")), text),
            )
        })
        .collect::<String>();
        let metadata_only = json!({
            "type":"message", "id":"internal-metadata", "role":"user",
            "content":[{"type":"input_text","text":"retained prompt text"}],
            "internal_chat_message_metadata_passthrough":{
                "content_item_kinds":["goal_context.internal_context"]
            }
        });
        let retained = event(
            "compacted",
            json!({"replacement_history":[user(Some("native-real"), "Original user request")]}),
        );
        let bytes = format!(
            "{ordinary}{internal}{}{retained}",
            event("response_item", metadata_only)
        );

        let inputs = parse_admitted_inputs("rollout-a.jsonl", 0, bytes.as_bytes());

        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].request_id, "native-real");
    }

    #[test]
    fn legacy_identity_uses_absolute_offset_and_survives_tail_scans() {
        let first = event("response_item", user(None, "Repeat this text"));
        let second = event("response_item", user(None, "Repeat this text"));
        let bytes = format!("{first}{second}");
        let expected_second_offset = first.len() as u64;

        let all = parse_admitted_inputs("rollout-a.jsonl", 0, bytes.as_bytes());
        let tail =
            parse_admitted_inputs("rollout-a.jsonl", expected_second_offset, second.as_bytes());

        assert_eq!(all.len(), 2);
        assert_ne!(all[0].request_id, all[1].request_id);
        assert_eq!(all[1].request_id, tail[0].request_id);
        assert_eq!(tail[0].byte_offset, expected_second_offset);
        assert!(tail[0].request_id.starts_with("rollout:"));
    }

    #[test]
    fn ignores_incomplete_final_record_and_keeps_only_latest_bounded_inputs() {
        let mut bytes = String::new();
        for index in 0..(MAX_REQUESTS_PER_SCAN + 2) {
            bytes.push_str(&event(
                "response_item",
                user(Some(&format!("native-{index}")), "Synthetic input"),
            ));
        }
        let incomplete = event(
            "response_item",
            user(Some("incomplete"), "Must not be admitted"),
        );
        bytes.push_str(&incomplete[..incomplete.len() - 1]);

        let inputs = parse_admitted_inputs("rollout-a.jsonl", 0, bytes.as_bytes());

        assert_eq!(inputs.len(), MAX_REQUESTS_PER_SCAN);
        assert_eq!(inputs.first().unwrap().request_id, "native-2");
        assert_eq!(inputs.last().unwrap().request_id, "native-49");
    }
}
