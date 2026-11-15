use crate::ledger::{Ledger, RequestUpdate, Status};
use anyhow::{Context, Result};
use serde_json::{Map, Value, json};
use std::io::{self, BufRead, Write};
use std::path::Path;

const PROTOCOL_VERSION: &str = "2025-11-25";
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_TOOL_TEXT_BYTES: usize = 1024 * 1024;
const MAX_SESSION_ID_CHARS: usize = 128;
const MAX_REQUEST_ID_CHARS: usize = 200;
const MAX_LIST_LIMIT: u64 = 25;
const DEFAULT_LIST_LIMIT: u64 = 12;
const MAX_REQUIREMENTS: usize = 64;
const MAX_REQUIREMENT_CHARS: usize = 1_000;
const MAX_SUMMARY_CHARS: usize = 2_000;
const MAX_NEXT_ACTION_CHARS: usize = 2_000;
const MAX_EVIDENCE_ITEMS: usize = 64;
const MAX_EVIDENCE_CHARS: usize = 4_000;

/// Run the MCP stdio server against the session-scoped request ledger.
pub fn run(db_path: &Path) -> Result<()> {
    let mut ledger = Ledger::open(db_path).context("could not open goal ledger database")?;
    let stdin = io::stdin();
    let stdout = io::stdout();
    serve(&mut stdin.lock(), &mut stdout.lock(), &mut ledger)
}

fn serve<R: BufRead, W: Write>(reader: &mut R, writer: &mut W, ledger: &mut Ledger) -> Result<()> {
    loop {
        match read_frame(reader)? {
            Frame::Eof => return Ok(()),
            Frame::TooLarge => {
                write_response(writer, &rpc_error(Value::Null, -32700, "Parse error"))?
            }
            Frame::Line(line) => {
                let response = match serde_json::from_slice::<Value>(&line) {
                    Ok(value) => handle_message(value, ledger),
                    Err(_) => Some(rpc_error(Value::Null, -32700, "Parse error")),
                };
                if let Some(response) = response {
                    write_response(writer, &response)?;
                }
            }
        }
    }
}

enum Frame {
    Eof,
    Line(Vec<u8>),
    TooLarge,
}

fn read_frame<R: BufRead>(reader: &mut R) -> io::Result<Frame> {
    let mut line = Vec::with_capacity(256);
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(Frame::Eof)
            } else {
                Ok(Frame::Line(line))
            };
        }

        let newline = available.iter().position(|byte| *byte == b'\n');
        let content_len = newline.unwrap_or(available.len());
        if line.len().saturating_add(content_len) > MAX_FRAME_BYTES {
            let consumed = content_len + usize::from(newline.is_some());
            reader.consume(consumed);
            if newline.is_none() {
                discard_to_newline(reader)?;
            }
            return Ok(Frame::TooLarge);
        }

        line.extend_from_slice(&available[..content_len]);
        let consumed = content_len + usize::from(newline.is_some());
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Frame::Line(line));
        }
    }
}

fn discard_to_newline<R: BufRead>(reader: &mut R) -> io::Result<()> {
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(());
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(());
        }
    }
}

fn write_response<W: Write>(writer: &mut W, response: &Value) -> Result<()> {
    serde_json::to_writer(&mut *writer, response).context("could not encode MCP response")?;
    writer
        .write_all(b"\n")
        .context("could not write MCP response")?;
    writer.flush().context("could not flush MCP response")?;
    Ok(())
}

fn handle_message(message: Value, ledger: &mut Ledger) -> Option<Value> {
    if message.is_array() {
        return Some(rpc_error(
            Value::Null,
            -32600,
            "Batch requests are not supported",
        ));
    }

    let Some(object) = message.as_object() else {
        return Some(rpc_error(Value::Null, -32600, "Invalid Request"));
    };
    let id = object.get("id").cloned();
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Some(rpc_error(
            id.unwrap_or(Value::Null),
            -32600,
            "Invalid Request",
        ));
    }
    if id
        .as_ref()
        .is_some_and(|value| !value.is_string() && !value.is_number() && !value.is_null())
    {
        return Some(rpc_error(Value::Null, -32600, "Invalid Request"));
    }
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return Some(rpc_error(
            id.unwrap_or(Value::Null),
            -32600,
            "Invalid Request",
        ));
    };

    if method == "notifications/initialized" {
        return None;
    }
    if method.starts_with("notifications/") || id.is_none() {
        return None;
    }
    let id = id.unwrap_or(Value::Null);

    let params = match object.get("params") {
        None => None,
        Some(Value::Object(params)) => Some(params),
        Some(_) => return Some(rpc_error(id, -32602, "Invalid params")),
    };

    let result = match method {
        "initialize" => initialize_result(params),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_definitions() })),
        "tools/call" => {
            let Some(params) = params else {
                return Some(rpc_error(id, -32602, "Invalid params"));
            };
            let Some(name) = params.get("name").and_then(Value::as_str) else {
                return Some(rpc_error(id, -32602, "Invalid params"));
            };
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            return Some(rpc_result(id, call_tool(ledger, name, arguments)));
        }
        _ => return Some(rpc_error(id, -32601, "Method not found")),
    };

    Some(match result {
        Ok(result) => rpc_result(id, result),
        Err(message) => rpc_error(id, -32602, message),
    })
}

fn initialize_result(
    params: Option<&Map<String, Value>>,
) -> std::result::Result<Value, &'static str> {
    let Some(params) = params else {
        return Err("Invalid params");
    };
    let Some(requested_version) = params.get("protocolVersion").and_then(Value::as_str) else {
        return Err("Invalid params");
    };
    let protocol_version = match requested_version {
        "2025-11-25" | "2025-06-18" | "2025-03-26" | "2024-11-05" => requested_version,
        _ => PROTOCOL_VERSION,
    };

    Ok(json!({
        "protocolVersion": protocol_version,
        "capabilities": { "tools": {} },
        "serverInfo": {
            "name": "goal-ledger",
            "version": env!("CARGO_PKG_VERSION")
        }
    }))
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message }
    })
}

fn tool_result(value: &Value) -> Value {
    match serde_json::to_string(value) {
        Ok(text) if text.len() <= MAX_TOOL_TEXT_BYTES => json!({
            "content": [{ "type": "text", "text": text }],
            "isError": false
        }),
        _ => tool_error("Result exceeds the response size limit."),
    }
}

fn tool_error(message: &str) -> Value {
    json!({
        "content": [{ "type": "text", "text": message }],
        "isError": true
    })
}

fn call_tool(ledger: &mut Ledger, name: &str, arguments: Value) -> Value {
    let args = match arguments {
        Value::Object(args) => args,
        _ => return tool_error("Tool arguments must be an object."),
    };

    match name {
        "request_latest" => request_latest(ledger, args),
        "requests_list" => requests_list(ledger, args),
        "request_get" => request_get(ledger, args),
        "request_update" => request_update(ledger, args),
        _ => tool_error("Unknown tool."),
    }
}

fn request_latest(ledger: &mut Ledger, args: Map<String, Value>) -> Value {
    if let Err(message) = ensure_only_keys(&args, &["session_id"]) {
        return tool_error(message);
    }
    let session_id = match required_session_id(&args) {
        Ok(value) => value,
        Err(message) => return tool_error(message),
    };
    let synchronized = match synchronize(ledger, &session_id) {
        Ok(count) => count,
        Err(message) => return tool_error(message),
    };
    match ledger.latest_admitted(&session_id) {
        Ok(request) => tool_result(&json!({"request": request, "synchronized": synchronized})),
        Err(_) => tool_error("Could not read the latest user request."),
    }
}

fn requests_list(ledger: &mut Ledger, mut args: Map<String, Value>) -> Value {
    if let Err(message) = ensure_only_keys(
        &args,
        &["session_id", "status", "limit", "before_request_id"],
    ) {
        return tool_error(message);
    }
    let session_id = match required_session_id(&args) {
        Ok(value) => value,
        Err(message) => return tool_error(message),
    };
    let synchronized = match synchronize(ledger, &session_id) {
        Ok(count) => count,
        Err(message) => return tool_error(message),
    };

    let status = match args.remove("status") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => match parse_status(&value) {
            Some(status) => Some(status),
            None => return tool_error("Invalid status."),
        },
        Some(_) => return tool_error("Status must be a string."),
    };
    let limit = match args.remove("limit") {
        None => DEFAULT_LIST_LIMIT as u32,
        Some(Value::Number(number)) => match number.as_u64() {
            Some(limit) if (1..=MAX_LIST_LIMIT).contains(&limit) => limit as u32,
            _ => return tool_error("Limit must be an integer from 1 to 25."),
        },
        Some(_) => return tool_error("Limit must be an integer from 1 to 25."),
    };
    let before = match args.get("before_request_id") {
        None | Some(Value::Null) => None,
        Some(_) => {
            match required_bounded_string(&args, "before_request_id", MAX_REQUEST_ID_CHARS) {
                Ok(value) => Some(value),
                Err(message) => return tool_error(message),
            }
        }
    };
    let mut requests = match ledger.list_admitted(&session_id, status, limit + 1, before.as_deref())
    {
        Ok(requests) => requests,
        Err(_) => {
            return tool_error(
                "Could not list requests. Check that before_request_id belongs to this session.",
            );
        }
    };
    let more = requests.len() > limit as usize;
    requests.truncate(limit as usize);
    let next_before = requests
        .last()
        .filter(|_| more)
        .map(|r| r.request_id.clone());
    match serde_json::to_value(requests) {
        Ok(requests) => tool_result(&json!({
            "synchronized": synchronized,
            "requests": requests,
            "next_before_request_id": next_before,
        })),
        Err(_) => tool_error("Could not encode requests."),
    }
}

fn request_get(ledger: &mut Ledger, args: Map<String, Value>) -> Value {
    if let Err(message) = ensure_only_keys(&args, &["session_id", "request_id"]) {
        return tool_error(message);
    }
    let session_id = match required_session_id(&args) {
        Ok(value) => value,
        Err(message) => return tool_error(message),
    };
    let synchronized = match synchronize(ledger, &session_id) {
        Ok(count) => count,
        Err(message) => return tool_error(message),
    };
    let request_id = match required_bounded_string(&args, "request_id", MAX_REQUEST_ID_CHARS) {
        Ok(value) => value,
        Err(message) => return tool_error(message),
    };
    let request = match ledger.get(&session_id, &request_id) {
        Ok(request) => request,
        Err(_) => return tool_error("Could not read request."),
    };
    match serde_json::to_value(request) {
        Ok(request) => tool_result(&json!({
            "synchronized": synchronized,
            "request": request
        })),
        Err(_) => tool_error("Could not encode request."),
    }
}

fn request_update(ledger: &mut Ledger, mut args: Map<String, Value>) -> Value {
    const UPDATE_KEYS: &[&str] = &[
        "session_id",
        "request_id",
        "expected_revision",
        "status",
        "summary",
        "requirements",
        "next_action",
        "evidence",
    ];
    if let Err(message) = ensure_only_keys(&args, UPDATE_KEYS) {
        return tool_error(message);
    }
    let session_id = match required_session_id(&args) {
        Ok(value) => value,
        Err(message) => return tool_error(message),
    };
    let synchronized = match synchronize(ledger, &session_id) {
        Ok(count) => count,
        Err(message) => return tool_error(message),
    };
    let request_id = match required_bounded_string(&args, "request_id", MAX_REQUEST_ID_CHARS) {
        Ok(value) => value,
        Err(message) => return tool_error(message),
    };

    if let Err(message) = validate_update_bounds(&args) {
        return tool_error(message);
    }
    args.remove("session_id");
    args.remove("request_id");
    let change: RequestUpdate = match serde_json::from_value(Value::Object(args)) {
        Ok(change) => change,
        Err(_) => return tool_error("Invalid request update."),
    };
    let request = match ledger.update(&session_id, &request_id, &change) {
        Ok(request) => request,
        Err(_) => {
            return tool_error(
                "Could not update request; verify it exists and its revision matches.",
            );
        }
    };
    match serde_json::to_value(request) {
        Ok(request) => tool_result(&json!({
            "synchronized": synchronized,
            "request": request
        })),
        Err(_) => tool_error("Could not encode updated request."),
    }
}

fn synchronize(ledger: &mut Ledger, session_id: &str) -> std::result::Result<usize, &'static str> {
    crate::codex::synchronize(ledger, session_id)
        .map_err(|_| "Could not synchronize this Codex session.")
}

fn ensure_only_keys(
    args: &Map<String, Value>,
    allowed: &[&str],
) -> std::result::Result<(), &'static str> {
    if args.keys().all(|key| allowed.contains(&key.as_str())) {
        Ok(())
    } else {
        Err("Tool arguments contain an unsupported field.")
    }
}

fn required_bounded_string(
    args: &Map<String, Value>,
    key: &str,
    max_chars: usize,
) -> std::result::Result<String, &'static str> {
    let Some(Value::String(value)) = args.get(key) else {
        return Err("A required string argument is missing or invalid.");
    };
    let length = value.chars().count();
    if value.trim().is_empty() || length > max_chars || value.chars().any(char::is_control) {
        return Err("A required string argument is empty or exceeds its limit.");
    }
    Ok(value.clone())
}

fn required_session_id(args: &Map<String, Value>) -> std::result::Result<String, &'static str> {
    let session_id = required_bounded_string(args, "session_id", MAX_SESSION_ID_CHARS)?;
    if session_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        Ok(session_id)
    } else {
        Err("Session ID is invalid.")
    }
}

fn parse_status(value: &str) -> Option<Status> {
    serde_json::from_value(Value::String(value.to_owned())).ok()
}

fn validate_update_bounds(args: &Map<String, Value>) -> std::result::Result<(), &'static str> {
    if let Some(value) = args.get("status") {
        let Some(status) = value.as_str() else {
            return Err("Status must be a string.");
        };
        if parse_status(status).is_none() {
            return Err("Invalid status.");
        }
    }
    validate_optional_string(args, "summary", MAX_SUMMARY_CHARS)?;
    validate_optional_string(args, "next_action", MAX_NEXT_ACTION_CHARS)?;
    validate_string_array(
        args,
        "requirements",
        MAX_REQUIREMENTS,
        MAX_REQUIREMENT_CHARS,
    )?;
    validate_string_array(args, "evidence", MAX_EVIDENCE_ITEMS, MAX_EVIDENCE_CHARS)?;
    if args
        .get("expected_revision")
        .is_some_and(|value| !value.is_null() && value.as_u64().is_none())
    {
        return Err("Expected revision must be a non-negative integer.");
    }
    Ok(())
}

fn validate_optional_string(
    args: &Map<String, Value>,
    key: &str,
    max_chars: usize,
) -> std::result::Result<(), &'static str> {
    let Some(value) = args.get(key) else {
        return Ok(());
    };
    if value.is_null() {
        return Ok(());
    }
    let Some(value) = value.as_str() else {
        return Err("Summary and next_action must be strings.");
    };
    if value.chars().count() > max_chars {
        return Err("A text field exceeds its size limit.");
    }
    Ok(())
}

fn validate_string_array(
    args: &Map<String, Value>,
    key: &str,
    max_items: usize,
    max_chars: usize,
) -> std::result::Result<(), &'static str> {
    let Some(value) = args.get(key) else {
        return Ok(());
    };
    if value.is_null() {
        return Ok(());
    }
    let Some(items) = value.as_array() else {
        return Err("Requirements and evidence must be arrays of strings.");
    };
    if items.len() > max_items
        || items.iter().any(|item| {
            item.as_str()
                .is_none_or(|text| text.chars().count() > max_chars)
        })
    {
        return Err("A requirements or evidence array exceeds its size limit.");
    }
    Ok(())
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "request_latest",
            "description": "Read the latest known original user prompt or steer and its recorded task status. On continuation, check this before treating retained input as current work.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string", "minLength": 1, "maxLength": MAX_SESSION_ID_CHARS, "pattern": "^[A-Za-z0-9_-]+$" }
                },
                "required": ["session_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "requests_list",
            "description": "List a Codex session's original prompts and steers in reverse admission order. Use next_before_request_id to page to earlier requests.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string", "minLength": 1, "maxLength": MAX_SESSION_ID_CHARS, "pattern": "^[A-Za-z0-9_-]+$" },
                    "status": { "type": "string", "enum": status_values() },
                    "limit": { "type": "integer", "minimum": 1, "maximum": MAX_LIST_LIMIT, "default": DEFAULT_LIST_LIMIT },
                    "before_request_id": { "type": "string", "minLength": 1, "maxLength": MAX_REQUEST_ID_CHARS }
                },
                "required": ["session_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "request_get",
            "description": "Read one tracked request by its session and request IDs.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string", "minLength": 1, "maxLength": MAX_SESSION_ID_CHARS, "pattern": "^[A-Za-z0-9_-]+$" },
                    "request_id": { "type": "string", "minLength": 1, "maxLength": MAX_REQUEST_ID_CHARS }
                },
                "required": ["session_id", "request_id"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "request_update",
            "description": "Update a tracked request's status, summary, requirements, next action, or evidence.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": { "type": "string", "minLength": 1, "maxLength": MAX_SESSION_ID_CHARS, "pattern": "^[A-Za-z0-9_-]+$" },
                    "request_id": { "type": "string", "minLength": 1, "maxLength": MAX_REQUEST_ID_CHARS },
                    "expected_revision": { "type": "integer", "minimum": 0 },
                    "status": { "type": "string", "enum": status_values() },
                    "summary": { "type": "string", "maxLength": MAX_SUMMARY_CHARS },
                    "requirements": {
                        "type": "array",
                        "maxItems": MAX_REQUIREMENTS,
                        "items": { "type": "string", "maxLength": MAX_REQUIREMENT_CHARS }
                    },
                    "next_action": { "type": "string", "maxLength": MAX_NEXT_ACTION_CHARS },
                    "evidence": {
                        "type": "array",
                        "maxItems": MAX_EVIDENCE_ITEMS,
                        "items": { "type": "string", "maxLength": MAX_EVIDENCE_CHARS }
                    }
                },
                "required": ["session_id", "request_id"],
                "additionalProperties": false
            }
        }),
    ]
}

fn status_values() -> [&'static str; 6] {
    [
        "observed",
        "open",
        "in_progress",
        "waiting",
        "completed",
        "superseded",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DB: AtomicU64 = AtomicU64::new(0);

    struct TestDb(PathBuf);

    impl TestDb {
        fn new() -> Self {
            let suffix = NEXT_DB.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "goal-ledger-mcp-{}-{suffix}.sqlite3",
                std::process::id()
            )))
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
            let _ = fs::remove_file(self.0.with_extension("sqlite3-wal"));
            let _ = fs::remove_file(self.0.with_extension("sqlite3-shm"));
        }
    }

    fn run_input(db_path: &Path, input: &[u8]) -> Vec<Value> {
        let mut ledger = Ledger::open(db_path).expect("test ledger opens");
        let mut reader = Cursor::new(input);
        let mut output = Vec::new();
        serve(&mut reader, &mut output, &mut ledger).expect("stdio loop succeeds");
        String::from_utf8(output)
            .expect("responses are UTF-8")
            .lines()
            .map(|line| serde_json::from_str(line).expect("response is JSON"))
            .collect()
    }

    #[test]
    fn handshake_and_tool_discovery_use_one_json_message_per_line() {
        let db = TestDb::new();
        let input = concat!(
            "{\"jsonrpc\":\"2.0\",\"id\":\"init\",\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{},\"clientInfo\":{\"name\":\"test\",\"version\":\"1\"}}}\n",
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n",
        );
        let responses = run_input(&db.0, input.as_bytes());

        assert_eq!(responses.len(), 2, "initialized notification has no reply");
        assert_eq!(responses[0]["id"], "init");
        assert_eq!(responses[0]["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert!(responses[0]["result"]["capabilities"]["tools"].is_object());
        assert_eq!(responses[1]["id"], 2);
        let tools = responses[1]["result"]["tools"]
            .as_array()
            .expect("tools list is an array");
        assert_eq!(tools.len(), 4);
        assert_eq!(tools[0]["name"], "request_latest");
        assert_eq!(tools[1]["name"], "requests_list");
        assert_eq!(tools[2]["name"], "request_get");
        assert_eq!(tools[3]["name"], "request_update");
        assert_eq!(tools[3]["inputSchema"]["additionalProperties"], false);
    }

    #[test]
    fn initialize_negotiates_common_legacy_protocol_dates() {
        for requested in ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"] {
            let params = json!({ "protocolVersion": requested });
            let result = initialize_result(params.as_object()).expect("valid initialize params");
            assert_eq!(result["protocolVersion"], requested);
        }

        let params = json!({ "protocolVersion": "unknown-version" });
        let result = initialize_result(params.as_object()).expect("fallback initialize params");
        assert_eq!(result["protocolVersion"], PROTOCOL_VERSION);
    }

    #[test]
    fn parse_errors_and_oversized_frames_do_not_break_the_next_request() {
        let db = TestDb::new();
        let mut input = b"not-json\n".to_vec();
        input.extend(std::iter::repeat_n(b'x', MAX_FRAME_BYTES + 1));
        input.push(b'\n');
        input.extend_from_slice(b"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"ping\"}\n");

        let responses = run_input(&db.0, &input);

        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0]["error"]["code"], -32700);
        assert_eq!(responses[1]["error"]["code"], -32700);
        assert_eq!(responses[2]["id"], 3);
        assert_eq!(responses[2]["result"], json!({}));
    }

    #[test]
    fn invalid_tool_arguments_return_text_content_without_exposing_internal_errors() {
        let db = TestDb::new();
        let input = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"requests_list\",\"arguments\":{}}}\n";
        let responses = run_input(&db.0, input.as_bytes());

        assert_eq!(responses.len(), 1);
        let result = &responses[0]["result"];
        assert_eq!(result["isError"], true);
        assert_eq!(result["content"][0]["type"], "text");
        assert_eq!(
            result["content"][0]["text"],
            "A required string argument is missing or invalid."
        );
        assert!(result["content"][0]["text"].as_str().unwrap().len() < 128);
    }
}
