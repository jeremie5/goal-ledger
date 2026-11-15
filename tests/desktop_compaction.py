#!/usr/bin/env python3
"""Exercise Codex's immediate compact hook with a loopback-only model fixture."""

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

OLD_INPUT = "Synthetic old request: count the amber markers and report the result."
FRESH_INPUT = "Synthetic fresh input: count the blue markers and report the result."
OLD_SUMMARY = "Synthetic old request completed before the follow-up turn."
OLD_EVIDENCE = "The local fixture completed the old request before compaction."
RECOVERY_PREFIX = "Goal Ledger: compaction recovery"
COMPACT_SUMMARY = "Synthetic compaction summary retained the old request history."
SESSION_RE = re.compile(r"session id:\s*([0-9a-fA-F-]{36})", re.IGNORECASE)


def json_texts(body, role):
    texts = []
    for item in body.get("input", []):
        if item.get("type") != "message" or item.get("role") != role:
            continue
        for part in item.get("content", []):
            if part.get("type") == "input_text" and isinstance(part.get("text"), str):
                texts.append(part["text"])
    return texts


def sse_response(request_no):
    response_id = f"synthetic-{request_no}"
    events = [{"type": "response.created", "response": {"id": response_id}}]
    if request_no == 1:
        events.append({"type": "response.output_item.done", "item": {
            "type": "message", "role": "assistant", "id": "old-turn-answer",
            "content": [{"type": "output_text", "text": "Synthetic first turn completed."}],
        }})
        input_tokens = 20
    elif request_no == 2:
        events.append({"type": "response.output_item.done", "item": {
            "type": "function_call", "id": "tool-item-1", "call_id": "synthetic-call-1",
            "name": "exec_command",
            "arguments": json.dumps({"cmd": "printf SYNTHETIC_TOOL_OUTPUT"}),
        }})
        input_tokens = 250000
    elif request_no == 3:
        events.append({"type": "response.output_item.done", "item": {
            "type": "message", "role": "assistant", "id": "compact-summary",
            "content": [{"type": "output_text", "text": COMPACT_SUMMARY}],
        }})
        input_tokens = 50
    else:
        events.append({"type": "response.output_item.done", "item": {
            "type": "message", "role": "assistant", "id": f"continuation-{request_no}",
            "content": [{"type": "output_text", "text": "Synthetic continuation completed."}],
        }})
        input_tokens = 80
    events.append({"type": "response.completed", "response": {
        "id": response_id,
        "usage": {
            "input_tokens": input_tokens,
            "input_tokens_details": None,
            "output_tokens": 1,
            "output_tokens_details": None,
            "total_tokens": input_tokens + 1,
        },
    }})
    return "".join(
        f"event: {event['type']}\ndata: {json.dumps(event, separators=(',', ':'))}\n\n"
        for event in events
    )


class MockResponsesServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), MockResponsesHandler)
        self.requests = []
        self.lock = threading.Lock()


class MockResponsesHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def send_body(self, status, body, content_type):
        data = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def do_GET(self):
        if self.path.endswith("/models"):
            self.send_body(200, json.dumps({
                "object": "list",
                "data": [{"id": "gpt-5.5", "object": "model", "owned_by": "synthetic"}],
            }), "application/json")
        else:
            self.send_body(404, "not found", "text/plain")

    def do_POST(self):
        if not self.path.endswith("/responses"):
            self.send_body(404, "not found", "text/plain")
            return
        size = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(size))
        with self.server.lock:
            self.server.requests.append(request)
            request_no = len(self.server.requests)
        self.send_body(200, sse_response(request_no), "text/event-stream")


def call_mcp_tool(ledger, env, tool_name, arguments):
    messages = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-11-25", "capabilities": {},
            "clientInfo": {"name": "desktop-compaction-test", "version": "1"},
        }},
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
            "name": tool_name, "arguments": arguments,
        }},
    ]
    payload = "".join(json.dumps(message, separators=(",", ":")) + "\n" for message in messages)
    result = subprocess.run(
        [str(ledger), "mcp"], input=payload, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, env=env, timeout=15,
    )
    if result.returncode != 0:
        raise RuntimeError(f"goal-ledger MCP exited {result.returncode}: {result.stderr[-1000:]}")
    replies = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    listing = next((reply for reply in replies if reply.get("id") == 2), None)
    response = next((reply for reply in replies if reply.get("id") == 3), None)
    if not listing or "error" in listing:
        raise RuntimeError(f"goal-ledger tools/list failed: {listing}")
    tool_names = {item.get("name") for item in listing.get("result", {}).get("tools", [])}
    if tool_name not in tool_names:
        raise RuntimeError(f"goal-ledger MCP did not expose {tool_name}")
    if not response or "error" in response:
        raise RuntimeError(f"goal-ledger {tool_name} failed: {response}")
    mcp_result = response.get("result", {})
    content = mcp_result.get("content", [])
    if mcp_result.get("isError") or not content or not isinstance(content[0].get("text"), str):
        raise RuntimeError(f"goal-ledger {tool_name} returned an error: {mcp_result}")
    try:
        return json.loads(content[0]["text"])
    except json.JSONDecodeError as error:
        raise RuntimeError(f"goal-ledger {tool_name} returned invalid JSON text") from error


def run_codex(codex, cwd, env, prompt, resume=False):
    options = [
        "--skip-git-repo-check",
        "--dangerously-bypass-hook-trust",
        "--dangerously-bypass-approvals-and-sandbox",
    ]
    if resume:
        args = [str(codex), "--no-daemon", "exec", "resume", "--last", *options, prompt]
    else:
        args = [str(codex), "--no-daemon", "exec", *options, prompt]
    result = subprocess.run(
        args, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, timeout=180,
    )
    if result.returncode != 0:
        detail = (result.stderr + "\n" + result.stdout)[-5000:]
        raise RuntimeError(f"Codex exited {result.returncode}:\n{detail}")
    match = SESSION_RE.search(result.stderr + "\n" + result.stdout)
    if not match:
        raise RuntimeError("Codex did not report the synthetic session ID")
    return match.group(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", required=True, type=Path, help="Codex CLI binary to exercise")
    parser.add_argument("--ledger", required=True, type=Path, help="compiled goal-ledger binary")
    parser.add_argument("--scratch-root", type=Path, help="existing parent directory for disposable test state")
    args = parser.parse_args()
    codex = args.codex.resolve()
    ledger = args.ledger.resolve()
    for name, binary in (("Codex", codex), ("goal-ledger", ledger)):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(f"{name} binary is not executable: {binary}")

    scratch_root = args.scratch_root.resolve() if args.scratch_root else None
    if scratch_root and not scratch_root.is_dir():
        parser.error(f"scratch root is not a directory: {scratch_root}")
    scratch_owner = tempfile.TemporaryDirectory(
        prefix="goal-ledger-desktop-compaction-", dir=scratch_root,
    )
    scratch = Path(scratch_owner.name)
    server = MockResponsesServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        home = scratch / "home"
        codex_home = scratch / "codex-home"
        project = scratch / "project"
        for path in (home, codex_home, project, scratch / "xdg-config", scratch / "xdg-state", scratch / "xdg-cache"):
            path.mkdir(parents=True, exist_ok=True)
        database = scratch / "goal-ledger.sqlite3"
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(home),
            "CODEX_HOME": str(codex_home),
            "XDG_CONFIG_HOME": str(scratch / "xdg-config"),
            "XDG_STATE_HOME": str(scratch / "xdg-state"),
            "XDG_CACHE_HOME": str(scratch / "xdg-cache"),
            "GOAL_LEDGER_DB": str(database),
            "TMPDIR": str(scratch),
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
            "HTTP_PROXY": "",
            "HTTPS_PROXY": "",
            "ALL_PROXY": "",
            "LANG": "C.UTF-8",
            "TERM": "dumb",
        }
        base_url = f"http://127.0.0.1:{server.server_address[1]}/v1"
        config = f'''model = "gpt-5.5"
model_provider = "synthetic"
model_context_window = 400000
model_auto_compact_token_limit = 200000
approval_policy = "never"

[features]
hooks = true

[model_providers.synthetic]
name = "Synthetic loopback provider"
base_url = {json.dumps(base_url)}
wire_api = "responses"
requires_openai_auth = false

[mcp_servers."goal-ledger"]
command = {json.dumps(str(ledger))}
args = ["mcp"]

[mcp_servers."goal-ledger".env]
GOAL_LEDGER_DB = {json.dumps(str(database))}
'''
        (codex_home / "config.toml").write_text(config, encoding="utf-8")
        hook_command = f"GOAL_LEDGER_DB={shlex.quote(str(database))} {shlex.quote(str(ledger))} hook"
        hook_groups = {
            "UserPromptSubmit": [{"hooks": [{"type": "command", "command": hook_command, "timeout": 10}]}],
            "PreCompact": [{"hooks": [{"type": "command", "command": hook_command, "timeout": 10}]}],
            "SessionStart": [{"matcher": "^compact$", "hooks": [{"type": "command", "command": hook_command, "timeout": 10}]}],
            "Stop": [{"hooks": [{"type": "command", "command": hook_command, "timeout": 10}]}],
        }
        (codex_home / "hooks.json").write_text(json.dumps({"hooks": hook_groups}), encoding="utf-8")

        session_id = run_codex(codex, project, env, OLD_INPUT)
        listed = call_mcp_tool(ledger, env, "requests_list", {"session_id": session_id})
        requests = listed.get("requests", [])
        if len(requests) != 1:
            raise RuntimeError(f"expected one prior synthetic request, found {len(requests)}")
        old_request = requests[0]
        request_id = old_request.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise RuntimeError("requests_list returned no canonical request_id")
        updated = call_mcp_tool(ledger, env, "request_update", {
            "session_id": session_id,
            "request_id": request_id,
            "expected_revision": old_request["revision"],
            "status": "completed",
            "summary": OLD_SUMMARY,
            "evidence": [OLD_EVIDENCE],
        }).get("request")
        if not updated or updated.get("status") != "completed" or OLD_EVIDENCE not in updated.get("evidence", []):
            raise RuntimeError("MCP request_update did not mark the old synthetic request completed with evidence")

        resumed_session_id = run_codex(codex, project, env, FRESH_INPUT, resume=True)
        if resumed_session_id != session_id:
            raise RuntimeError("Codex resumed a different synthetic session")
        captured = server.requests
        if len(captured) != 4:
            raise RuntimeError(f"expected 4 local model requests across two turns and one compaction, got {len(captured)}")

        continuation = captured[3]
        developer_texts = json_texts(continuation, "developer")
        frames = [text for text in developer_texts if RECOVERY_PREFIX in text]
        if not frames:
            raise RuntimeError("first post-compaction model request has no Goal Ledger recovery frame")
        frame = "\n".join(frames)
        if request_id not in frame or '"status":"completed"' not in frame:
            raise RuntimeError("recovery frame omitted the completed prior request ID/status")
        if OLD_EVIDENCE not in frame:
            raise RuntimeError("recovery frame omitted the prior completion evidence")
        if OLD_INPUT not in json_texts(continuation, "user"):
            raise RuntimeError("first post-compaction model request did not retain the old user input")
        if not any(FRESH_INPUT in text for text in json_texts(continuation, "user")):
            raise RuntimeError("first post-compaction model request did not retain the fresh user input")
        latest_line = next(
            (line for line in frame.splitlines() if line.startswith('{"latest_user_request":')),
            None,
        )
        latest = call_mcp_tool(ledger, env, "request_latest", {"session_id": session_id})["request"]
        if (
            not latest_line
            or not latest
            or latest["request_id"] == request_id
            or latest["status"] != "observed"
            or json.loads(latest_line)["latest_user_request"]["id"] != latest["request_id"]
            or json.loads(latest_line)["latest_user_request"]["status"] != "observed"
        ):
            raise RuntimeError("recovery did not identify the fresh input as the latest observed request")
        page = call_mcp_tool(ledger, env, "requests_list", {"session_id": session_id, "limit": 1})
        if page["next_before_request_id"] != latest["request_id"]:
            raise RuntimeError("request history did not return a cursor for the older request")
        older = call_mcp_tool(ledger, env, "requests_list", {
            "session_id": session_id, "before_request_id": page["next_before_request_id"],
        })
        if [r["request_id"] for r in older["requests"]] != [request_id]:
            raise RuntimeError("request history failed to retrieve precisely the older input")

        print("PASS: compact SessionStart context and completed request reached the immediate local continuation.")
        print("recovery_frame: present with completed request ID and evidence")
        print("old_input: retained in a user message")
        print("fresh_input: retained as a user message")
        print("latest_input: fresh request shown as observed and older completion queryable")
        advertised = json.dumps(captured[0].get("tools", []))
        print("mcp_tool_advertisement:", "inline" if "requests_list" in advertised else "deferred by Codex")
        print(f"session_id: {session_id}")
        print(f"completed_request_id: {request_id}")
        print("model_requests: old turn, fresh turn, compaction, immediate continuation")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        scratch_owner.cleanup()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        raise SystemExit(1)
