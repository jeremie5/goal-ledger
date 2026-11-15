#!/usr/bin/env python3
"""Exercise one app-server mid-turn steer across automatic compaction.

All Responses API traffic is served by a loopback fixture. The test requires a
Codex app-server with ``turn/steer`` and immediate compact-session hooks.
"""

import argparse
from collections import deque
import json
import os
from pathlib import Path
import queue
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from desktop_compaction import call_mcp_tool, json_texts


A_INPUT = "Synthetic request A: keep the original work active through a compaction."
C_STEER = "Synthetic steer C: finish the small marker check and preserve A as active."
A_CLIENT_ID = "goal-ledger-steer-request-a"
C_CLIENT_ID = "goal-ledger-steer-request-c"
C_EVIDENCE = "The loopback fixture completed C_WORK_COMPLETE."
RECOVERY_PREFIX = "Goal Ledger: compaction recovery"
COMPACT_SUMMARY = "Synthetic summary retained the active request and latest steer."
GATE_TIMEOUT_SECONDS = 120
EVENT_TIMEOUT_SECONDS = 60


def event_stream(request_no):
    response_id = f"steer-synthetic-{request_no}"
    events = [{"type": "response.created", "response": {"id": response_id}}]

    if request_no == 1:
        events.append({"type": "response.output_item.done", "item": {
            "type": "function_call", "id": "steer-tool-a", "call_id": "steer-call-a",
            "name": "exec_command",
            "arguments": json.dumps({"cmd": "printf A_STEP_COMPLETE"}),
        }})
        input_tokens = 20
    elif request_no == 2:
        events.append({"type": "response.output_item.done", "item": {
            "type": "function_call", "id": "steer-tool-c", "call_id": "steer-call-c",
            "name": "exec_command",
            "arguments": json.dumps({"cmd": "printf C_WORK_COMPLETE"}),
        }})
        input_tokens = 30
    elif request_no == 3:
        # This tool call keeps A in flight while its token usage forces auto-compact.
        events.append({"type": "response.output_item.done", "item": {
            "type": "function_call", "id": "steer-tool-a-next", "call_id": "steer-call-a-next",
            "name": "exec_command",
            "arguments": json.dumps({"cmd": "printf A_REMAINS_ACTIVE"}),
        }})
        input_tokens = 250000
    elif request_no == 4:
        events.append({"type": "response.output_item.done", "item": {
            "type": "message", "role": "assistant", "id": "steer-compact-summary",
            "content": [{"type": "output_text", "text": COMPACT_SUMMARY}],
        }})
        input_tokens = 50
    else:
        events.append({"type": "response.output_item.done", "item": {
            "type": "message", "role": "assistant", "id": f"steer-continuation-{request_no}",
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


class ResponsesServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), ResponsesHandler)
        self.condition = threading.Condition()
        self.requests = []
        self.release_first = threading.Event()
        self.release_compaction = threading.Event()
        self.failures = []

    def wait_for_count(self, count, timeout=EVENT_TIMEOUT_SECONDS):
        deadline = time.monotonic() + timeout
        with self.condition:
            while len(self.requests) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"timed out waiting for local Responses request {count}; "
                        f"captured {len(self.requests)}"
                    )
                self.condition.wait(remaining)
            return list(self.requests)

    def snapshot(self):
        with self.condition:
            return list(self.requests)


class ResponsesHandler(BaseHTTPRequestHandler):
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
        with self.server.condition:
            self.server.requests.append(request)
            request_no = len(self.server.requests)
            self.server.condition.notify_all()

        gate = None
        if request_no == 1:
            gate = self.server.release_first
        elif request_no == 3:
            gate = self.server.release_compaction
        if gate is not None and not gate.wait(GATE_TIMEOUT_SECONDS):
            with self.server.condition:
                self.server.failures.append(
                    f"local Responses request {request_no} gate was not released"
                )
            self.send_body(503, "test gate timed out", "text/plain")
            return

        self.send_body(200, event_stream(request_no), "text/event-stream")


class AppServer:
    def __init__(self, command, cwd, env):
        self.process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.messages = queue.Queue()
        self.notifications = deque(maxlen=500)
        self.stderr_tail = deque(maxlen=80)
        self.next_id = 1
        self.write_lock = threading.Lock()
        self.reader = threading.Thread(target=self._read_stdout, daemon=True)
        self.stderr_reader = threading.Thread(target=self._read_stderr, daemon=True)
        self.reader.start()
        self.stderr_reader.start()

    def _read_stdout(self):
        try:
            for line in self.process.stdout:
                try:
                    self.messages.put(json.loads(line))
                except json.JSONDecodeError:
                    self.messages.put({"_invalid_line": line.rstrip()})
        finally:
            self.messages.put({"_eof": True})

    def _read_stderr(self):
        for line in self.process.stderr:
            self.stderr_tail.append(line.rstrip())

    def request(self, method, params, timeout=EVENT_TIMEOUT_SECONDS):
        request_id = self.next_id
        self.next_id += 1
        payload = json.dumps({
            "jsonrpc": "2.0", "id": request_id, "method": method, "params": params,
        }, separators=(",", ":"))
        with self.write_lock:
            self.process.stdin.write(payload + "\n")
            self.process.stdin.flush()

        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"timed out waiting for app-server {method} response")
            try:
                message = self.messages.get(timeout=remaining)
            except queue.Empty as error:
                raise TimeoutError(
                    f"timed out waiting for app-server {method} response"
                ) from error
            if message.get("_eof"):
                raise RuntimeError(self._failure_detail("app-server closed stdout"))
            if "_invalid_line" in message:
                raise RuntimeError(f"app-server emitted non-JSON stdout: {message['_invalid_line']}")
            if message.get("id") == request_id:
                if "error" in message:
                    raise RuntimeError(
                        f"app-server {method} failed: {json.dumps(message['error'])}"
                    )
                return message.get("result", {})
            if isinstance(message.get("method"), str):
                self.notifications.append(message)

    def notify(self, method, params=None):
        payload = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        with self.write_lock:
            self.process.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
            self.process.stdin.flush()

    def _failure_detail(self, message):
        detail = "\n".join(self.stderr_tail)
        return f"{message}\n{detail}" if detail else message

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            if stream:
                stream.close()


def request_id(record):
    value = record.get("request_id", record.get("id"))
    return value if isinstance(value, str) else None


def record_by_id(records, wanted):
    return next((record for record in records if request_id(record) == wanted), None)


def hook_events(log_path):
    if not log_path.is_file():
        return []
    events = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def hook_session_id(log_path, wanted_prompt):
    events = hook_events(log_path)
    matching = [
        event for event in events
        if event.get("hook_event_name") == "UserPromptSubmit"
        and (wanted_prompt is None or event.get("prompt") == wanted_prompt)
        and isinstance(event.get("session_id"), str)
    ]
    return matching[-1]["session_id"] if matching else None


def write_hook_wrapper(path):
    source = '''#!/usr/bin/env python3
import json
import os
import subprocess
import sys

raw = sys.stdin.read()
log_path = os.environ.get("GOAL_LEDGER_HOOK_LOG")
if log_path:
    with open(log_path, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(json.loads(raw), separators=(",", ":")) + "\\n")
result = subprocess.run(
    [sys.argv[1], "hook"], input=raw, text=True,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy(),
)
sys.stdout.write(result.stdout)
sys.stderr.write(result.stderr)
raise SystemExit(result.returncode)
'''
    path.write_text(source, encoding="utf-8")
    path.chmod(0o700)


def app_initialize(app):
    app.request("initialize", {
        "clientInfo": {"name": "goal-ledger-steer-compaction", "version": "1"},
        "capabilities": {"experimentalApi": True, "requestAttestation": False},
    })
    app.notify("initialized")


def flatten_hooks(result):
    return [
        hook
        for entry in result.get("data", [])
        for hook in entry.get("hooks", [])
    ]


def latest_request_line(developer_text):
    for line in developer_text.splitlines():
        offset = line.find('{"latest_user_request":')
        if offset < 0:
            continue
        try:
            value = json.loads(line[offset:].strip())
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "latest_user_request" in value:
            return value["latest_user_request"]
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", required=True, type=Path, help="Codex app-server executable")
    parser.add_argument("--ledger", required=True, type=Path, help="compiled goal-ledger executable")
    parser.add_argument("--scratch-root", type=Path, help="registered disposable parent directory")
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
        prefix="goal-ledger-steer-compaction-", dir=scratch_root,
    )
    scratch = Path(scratch_owner.name)
    server = ResponsesServer()
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    app = None
    try:
        home = scratch / "home"
        codex_home = scratch / "codex-home"
        project = scratch / "project"
        for path in (
            home, codex_home, project,
            scratch / "xdg-config", scratch / "xdg-state", scratch / "xdg-cache",
        ):
            path.mkdir(parents=True, exist_ok=True)
        database = scratch / "goal-ledger.sqlite3"
        hook_log = scratch / "hook-events.jsonl"
        wrapper = scratch / "hook-wrapper.py"
        write_hook_wrapper(wrapper)

        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(home),
            "CODEX_HOME": str(codex_home),
            "XDG_CONFIG_HOME": str(scratch / "xdg-config"),
            "XDG_STATE_HOME": str(scratch / "xdg-state"),
            "XDG_CACHE_HOME": str(scratch / "xdg-cache"),
            "GOAL_LEDGER_DB": str(database),
            "GOAL_LEDGER_HOOK_LOG": str(hook_log),
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
        hook_command = (
            f"GOAL_LEDGER_DB={shlex.quote(str(database))} "
            f"GOAL_LEDGER_HOOK_LOG={shlex.quote(str(hook_log))} "
            f"{shlex.quote(sys.executable)} {shlex.quote(str(wrapper))} "
            f"{shlex.quote(str(ledger))}"
        )
        command_toml = json.dumps(hook_command)
        hook_config = f'''
[[hooks.UserPromptSubmit]]
[[hooks.UserPromptSubmit.hooks]]
type = "command"
command = {command_toml}
timeout = 10

[[hooks.PreCompact]]
[[hooks.PreCompact.hooks]]
type = "command"
command = {command_toml}
timeout = 10

[[hooks.SessionStart]]
matcher = "^compact$"
[[hooks.SessionStart.hooks]]
type = "command"
command = {command_toml}
timeout = 10

[[hooks.Stop]]
[[hooks.Stop.hooks]]
type = "command"
command = {command_toml}
timeout = 10
'''
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
{hook_config}'''
        (codex_home / "config.toml").write_text(config, encoding="utf-8")

        app_command = [str(codex), "app-server", "--enable", "hooks", "--stdio"]
        app = AppServer(app_command, project, env)
        app_initialize(app)
        discovered = flatten_hooks(app.request("hooks/list", {"cwds": [str(project)]}))
        expected_events = {"userPromptSubmit", "preCompact", "sessionStart", "stop"}
        own_hooks = [hook for hook in discovered if hook.get("command") == hook_command]
        if {hook.get("eventName") for hook in own_hooks} != expected_events:
            raise RuntimeError(
                "hooks/list did not discover the four fixture handlers: "
                f"{[(h.get('eventName'), h.get('command')) for h in own_hooks]}"
            )
        trust_state = "\n".join(
            f"[hooks.state.{json.dumps(hook['key'])}]\n"
            f"trusted_hash = {json.dumps(hook['currentHash'])}"
            for hook in own_hooks
        )
        with (codex_home / "config.toml").open("a", encoding="utf-8") as stream:
            stream.write("\n" + trust_state + "\n")
        app.close()

        # Hook configuration is cached per app-server process, so restart before
        # any turn starts and verify the isolated trust records took effect.
        app = AppServer(app_command, project, env)
        app_initialize(app)
        trusted_hooks = [
            hook for hook in flatten_hooks(
                app.request("hooks/list", {"cwds": [str(project)]})
            ) if hook.get("command") == hook_command
        ]
        trust_by_event = {hook.get("eventName"): hook.get("trustStatus") for hook in trusted_hooks}
        if trust_by_event != {event: "trusted" for event in expected_events}:
            raise RuntimeError(f"fixture hooks are not trusted in isolated config: {trust_by_event}")

        started = app.request("thread/start", {
            "cwd": str(project),
            "model": "gpt-5.5",
            "modelProvider": "synthetic",
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
            "historyMode": "paginated",
            "ephemeral": False,
        })
        thread_id = started.get("thread", {}).get("id")
        if not isinstance(thread_id, str) or not thread_id:
            raise RuntimeError(f"thread/start returned no thread id: {started}")

        turn = app.request("turn/start", {
            "threadId": thread_id,
            "input": [{"type": "text", "text": A_INPUT}],
            "clientUserMessageId": A_CLIENT_ID,
        })
        turn_id = turn.get("turn", {}).get("id")
        if not isinstance(turn_id, str) or not turn_id:
            raise RuntimeError(f"turn/start returned no turn id: {turn}")

        first = server.wait_for_count(1)[0]
        if A_INPUT not in json_texts(first, "user"):
            raise RuntimeError("the first model request did not contain request A")
        if C_STEER in "\n".join(json_texts(first, "user")):
            raise RuntimeError("the steer arrived before the gated in-flight request")

        steered = app.request("turn/steer", {
            "threadId": thread_id,
            "expectedTurnId": turn_id,
            "input": [{"type": "text", "text": C_STEER}],
            "clientUserMessageId": C_CLIENT_ID,
        })
        if steered.get("turnId") != turn_id:
            raise RuntimeError(f"turn/steer was not accepted by the active turn: {steered}")
        server.release_first.set()

        second_requests = server.wait_for_count(2)
        second = second_requests[1]
        if C_STEER not in "\n".join(json_texts(second, "user")):
            raise RuntimeError("the next model request did not receive the in-flight steer")

        server.wait_for_count(3)
        third = server.snapshot()[2]
        if C_STEER not in "\n".join(json_texts(third, "user")):
            raise RuntimeError("the steer was absent after the mocked C work completed")
        if "C_WORK_COMPLETE" not in json.dumps(third):
            raise RuntimeError("the loopback fixture did not return C work output before compaction")

        session_id = (
            hook_session_id(hook_log, C_STEER)
            or hook_session_id(hook_log, A_INPUT)
            or hook_session_id(hook_log, None)
            or thread_id
        )
        initial = call_mcp_tool(ledger, env, "requests_list", {
            "session_id": session_id, "limit": 25,
        })
        records = initial.get("requests", [])
        observed_latest = call_mcp_tool(ledger, env, "request_latest", {
            "session_id": session_id,
        }).get("request")
        c_request_id = request_id(observed_latest) if observed_latest else None
        if not c_request_id or not record_by_id(records, c_request_id):
            raise RuntimeError(
                "request_latest did not identify the admitted steer C; "
                f"IDs={[request_id(item) for item in records]}, latest={observed_latest!r}; "
                f"hook_events={hook_events(hook_log)!r}; "
                f"{app._failure_detail('app-server stderr')}"
            )
        if observed_latest.get("status") != "observed":
            raise RuntimeError(f"steer C was not first admitted as observed: {observed_latest}")
        earlier = call_mcp_tool(ledger, env, "requests_list", {
            "session_id": session_id, "limit": 25, "before_request_id": c_request_id,
        }).get("requests", [])
        if len(records) != 2 or len(earlier) != 1:
            raise RuntimeError(
                "expected only original A and steer C before compaction; "
                f"latest={[request_id(item) for item in records]}, "
                f"before_latest={[request_id(item) for item in earlier]}"
            )
        a_record = earlier[0]
        a_request_id = request_id(a_record)
        c_record = record_by_id(records, c_request_id)
        if not a_request_id or a_request_id == c_request_id or not c_record:
            raise RuntimeError("could not distinguish original A from steer C")

        a_updated = call_mcp_tool(ledger, env, "request_update", {
            "session_id": session_id,
            "request_id": a_request_id,
            "expected_revision": a_record["revision"],
            "status": "in_progress",
            "summary": "A remains active while C is handled.",
            "next_action": "Continue A after the compaction boundary.",
        }).get("request")
        if not a_updated or a_updated.get("status") != "in_progress":
            raise RuntimeError("could not keep request A explicitly in progress")

        c_updated = call_mcp_tool(ledger, env, "request_update", {
            "session_id": session_id,
            "request_id": c_request_id,
            "expected_revision": c_record["revision"],
            "status": "completed",
            "summary": "The steered marker check completed.",
            "evidence": [C_EVIDENCE],
        }).get("request")
        if not c_updated or c_updated.get("status") != "completed":
            raise RuntimeError("could not mark the completed steer C with evidence")
        if C_EVIDENCE not in c_updated.get("evidence", []):
            raise RuntimeError("request_update did not retain C completion evidence")

        latest = call_mcp_tool(ledger, env, "request_latest", {
            "session_id": session_id,
        }).get("request")
        if not latest or request_id(latest) != c_request_id or latest.get("status") != "completed":
            raise RuntimeError(f"request_latest did not return completed steer C: {latest}")
        earlier = call_mcp_tool(ledger, env, "requests_list", {
            "session_id": session_id,
            "limit": 25,
            "before_request_id": c_request_id,
        }).get("requests", [])
        if not record_by_id(earlier, a_request_id) or record_by_id(earlier, c_request_id):
            raise RuntimeError("before_request_id did not page back from C to earlier request A")

        # Keep A active. The third mock response reports a large token count and a
        # harmless fixed command, causing Codex to compact before its next request.
        server.release_compaction.set()
        # Request four receives the compaction summary. Request five resumes A.
        requests = server.wait_for_count(5)
        for request in requests[:4]:
            if RECOVERY_PREFIX in "\n".join(json_texts(request, "developer")):
                raise RuntimeError("recovery context appeared before compaction completed")
        continuation = requests[4]
        recovery_text = "\n".join(json_texts(continuation, "developer"))
        if RECOVERY_PREFIX not in recovery_text:
            raise RuntimeError(
                "the first resumed request omitted Goal Ledger compaction recovery context; "
                + app._failure_detail("app-server status")
            )

        latest_line = latest_request_line(recovery_text)
        if not isinstance(latest_line, dict):
            raise RuntimeError("recovery context omitted its latest_user_request JSON line")
        if latest_line.get("id") != c_request_id or latest_line.get("status") != "completed":
            raise RuntimeError(f"recovery line did not identify completed C as latest: {latest_line}")
        if C_EVIDENCE not in recovery_text:
            raise RuntimeError("first post-compaction request omitted C's completion evidence")

        latest_after = call_mcp_tool(ledger, env, "request_latest", {
            "session_id": session_id,
        }).get("request")
        records_after = call_mcp_tool(ledger, env, "requests_list", {
            "session_id": session_id, "limit": 25,
        }).get("requests", [])
        if not latest_after or request_id(latest_after) != c_request_id:
            raise RuntimeError(f"compacted retained history changed the latest admitted request: {latest_after}")
        expected_ids = {a_request_id, c_request_id}
        if len(records_after) != 2 or {request_id(item) for item in records_after} != expected_ids:
            raise RuntimeError(
                "nested compacted history created an extra admission: "
                f"{[request_id(item) for item in records_after]}"
            )
        a_after = record_by_id(records_after, a_request_id)
        c_after = record_by_id(records_after, c_request_id)
        if not a_after or a_after.get("status") != "in_progress":
            raise RuntimeError("request A is no longer queryable as in progress")
        if not c_after or c_after.get("status") != "completed":
            raise RuntimeError("completed steer C is no longer queryable")

        raw_steer_retained = C_STEER in "\n".join(json_texts(continuation, "user"))
        print("PASS: a real app-server turn/steer was admitted and recovered across auto-compaction.")
        print(f"thread_id: {thread_id}")
        print(f"session_id: {session_id}")
        print(f"request_A: {a_request_id} (in_progress)")
        print(f"steer_C: {c_request_id} (completed, latest)")
        print("first_compaction_recovery_model_request: 5")
        print(f"raw_steer_retained_in_user_messages: {'yes' if raw_steer_retained else 'no'}")
        print("admissions_after_compaction: A and C only")
    finally:
        server.release_first.set()
        server.release_compaction.set()
        if app:
            app.close()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        scratch_owner.cleanup()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        raise SystemExit(1)
