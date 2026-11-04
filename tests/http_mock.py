"""Real local HTTP servers emulating the Hetzner Cloud v1 and Telegram Bot APIs.

The point of these mocks is that the *production* urllib-based clients in
``deadman.hetzner`` / ``deadman.telegram`` / ``deadman.billing`` are exercised
over a real socket: request building, JSON encoding, status handling, pagination,
retries and error classification all run for real.  Only the remote project is
simulated.

Fidelity notes (things the engine depends on):

* ``DELETE /servers/{id}`` is refused with 422 while the server, or any attached
  Primary IP, has ``protection.delete == true`` (SPEC trap T2).
* ``DELETE /primary_ips/{id}`` is refused with 422 ``must_be_unassigned`` while
  assigned (T3), and with 422 ``protected`` while delete-protected.
* ``POST /primary_ips/{id}/actions/unassign`` requires the server to be ``off``
  (T4) and fails with 422 ``server_is_load_balancer_target`` while an LB targets
  the server (by server id, by IP target, or via a resolved label selector).
* ``change_protection {delete:true}`` on a Primary IP is refused while
  ``auto_delete == true`` (T5).
* Deleting a server detaches its Primary IPs (asynchronously, configurable
  delay) and deletes those with ``auto_delete == true``.
* Every mutation returns an async action id; ``GET /actions/{id}`` transitions
  from ``running`` to ``success``/``error``.

Fault injection is per-endpoint and can be transient (``times``).
"""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Optional


# --------------------------------------------------------------------------- #
# fault injection
# --------------------------------------------------------------------------- #
@dataclass
class Fault:
    """Inject a failure into matching requests.

    ``action`` is one of: ``http_500``, ``http_422``, ``http_403``, ``timeout``,
    ``malformed``, ``action_error``, ``drop``.
    ``times`` < 0 means "forever".
    """

    method: str = "*"
    path_contains: str = ""
    action: str = "http_500"
    times: int = 1
    status: Optional[int] = None
    code: str = "unavailable"
    message: str = "injected fault"
    delay: float = 1.0

    def matches(self, method: str, path: str) -> bool:
        if self.times == 0:
            return False
        if self.method not in ("*", method.upper()):
            return False
        return self.path_contains in path


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details


class _QuietHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args: Any) -> None:  # noqa: D401 - silence access logs
        return

    # -- helpers -----------------------------------------------------------
    @property
    def app(self):  # noqa: ANN201
        return self.server.app  # type: ignore[attr-defined]

    def _read_body(self) -> Any:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

    def _send(self, status: int, payload: Any = None, raw: Optional[bytes] = None) -> None:
        body = raw if raw is not None else (json.dumps(payload).encode() if payload is not None else b"")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("RateLimit-Limit", "3600")
        self.send_header("RateLimit-Remaining", "3599")
        self.send_header("RateLimit-Reset", str(int(time.time()) + 3600))
        self.end_headers()
        if body:
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def _dispatch(self, method: str) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        body = self._read_body()

        fault = self.app.take_fault(method, path)
        if fault is not None:
            action = fault.action
            if action == "timeout":
                time.sleep(fault.delay)
                self.close_connection = True
                return
            if action == "drop":
                self.close_connection = True
                return
            if action == "malformed":
                self._send(200, raw=b'{"servers": [ this is not json')
                self.app.record(method, path, 200, body)
                return
            if action == "action_error":
                self.app.next_action_error = {
                    "code": fault.code,
                    "message": fault.message,
                }
                # fall through to normal handling; the action will end in error
            elif action.startswith("http_"):
                status = fault.status or int(action.split("_", 1)[1])
                payload = {"error": {"code": fault.code, "message": fault.message}}
                if fault.code == "invalid_input" and fault.message:
                    payload["error"]["details"] = {"fields": [{"name": "x", "messages": [fault.message]}]}
                self._send(status, payload)
                self.app.record(method, path, status, body)
                return

        try:
            status, payload = self.app.handle(method, path, query, body)
        except ApiError as exc:
            status = exc.status
            payload = {"error": {"code": exc.code, "message": exc.message}}
            if exc.details is not None:
                payload["error"]["details"] = exc.details
        except Exception as exc:  # noqa: BLE001 - surface as a 500 like a real gateway
            status, payload = 500, {"error": {"code": "server_error", "message": str(exc)}}
        self._send(status, payload)
        self.app.record(method, path, status, body)

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._dispatch("PUT")

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch("DELETE")


class _AppServer:
    """Owns a ThreadingHTTPServer bound to 127.0.0.1:<ephemeral>."""

    #: subclasses may populate this with :class:`Fault` instances
    faults: list["Fault"] = []

    def record(self, method: str, path: str, status: int, body: Any) -> None:
        """Hook for request accounting; the Hetzner mock overrides it."""

    def take_fault(self, method: str, path: str) -> Optional["Fault"]:
        for fault in getattr(self, "faults", []):
            if fault.matches(method, path):
                if fault.times > 0:
                    fault.times -= 1
                return fault
        return None

    def __init__(self) -> None:
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _QuietHandler)
        self.httpd.daemon_threads = True
        self.httpd.app = self  # type: ignore[attr-defined]
        self.port = self.httpd.server_address[1]
        self._thread = threading.Thread(
            target=lambda: self.httpd.serve_forever(poll_interval=0.02),
            name="mock-http",
            daemon=True,
        )
        self._thread.start()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def close(self) -> None:
        try:
            self.httpd.shutdown()
            self.httpd.server_close()
        finally:
            self._thread.join(timeout=5)


# --------------------------------------------------------------------------- #
# Hetzner Cloud
# --------------------------------------------------------------------------- #
class HetznerMock(_AppServer):
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.servers: dict[int, dict] = {}
        self.primary_ips: dict[int, dict] = {}
        self.floating_ips: dict[int, dict] = {}
        self.images: dict[int, dict] = {}
        self.load_balancers: list[dict] = []
        self.actions: dict[int, dict] = {}
        self.requests: list[dict] = []
        self.actions_performed: list[dict] = []
        self.faults: list[Fault] = []
        self.server_delete_snapshots: list[dict] = []
        self.snapshot_seen_status: Optional[str] = None
        self.next_action_error: Optional[dict] = None
        #: how long after DELETE /servers/{id} the IPs detach
        self.delete_detach_delay = 0.0
        #: how long after DELETE /servers/{id} the server stops being visible
        self.server_remove_delay = 0.0
        #: never stop returning the server (simulates a stuck Hetzner action)
        self.keep_server_after_delete = False
        #: how long an image stays "creating"
        self.image_ready_delay = 0.05
        #: refuse PUT auto_delete=true (bug 5 path)
        self.reject_auto_delete_true = False
        #: refuse unassign while any LB targets the server
        self.lb_blocks_unassign = True
        self._seq = 0
        self._next_action_id = 1000
        self._next_image_id = 500
        self._events: list[tuple[float, int, Callable[[], None]]] = []
        self._stop = threading.Event()
        self._scheduler = threading.Thread(target=self._run_scheduler, name="mock-sched", daemon=True)
        super().__init__()
        self._scheduler.start()

    def close(self) -> None:
        self._stop.set()
        try:
            self._scheduler.join(timeout=5)
        finally:
            super().close()

    # ------------------------------------------------------------- plumbing
    def _run_scheduler(self) -> None:
        while not self._stop.is_set():
            due: list[Callable[[], None]] = []
            with self.lock:
                now = time.time()
                remaining: list[tuple[float, int, Callable[[], None]]] = []
                for when, seq, fn in self._events:
                    if when <= now:
                        due.append(fn)
                    else:
                        remaining.append((when, seq, fn))
                self._events = remaining
            for fn in due:
                try:
                    with self.lock:
                        fn()
                except Exception:  # noqa: BLE001 - a mock must not crash the server
                    pass
            self._stop.wait(0.01)

    def _schedule(self, delay: float, fn: Callable[[], None]) -> None:
        with self.lock:
            self._seq += 1
            self._events.append((time.time() + max(0.0, delay), self._seq, fn))

    def record(self, method: str, path: str, status: int, body: Any) -> None:
        with self.lock:
            self.requests.append(
                {
                    "method": method,
                    "path": path,
                    "status": status,
                    "body": body,
                    "at": time.time(),
                }
            )

    def take_fault(self, method: str, path: str) -> Optional[Fault]:
        with self.lock:
            for fault in self.faults:
                if fault.matches(method, path):
                    if fault.times > 0:
                        fault.times -= 1
                    return fault
        return None

    def clear_faults(self) -> None:
        with self.lock:
            self.faults.clear()

    # -------------------------------------------------------------- fixtures
    def add_server(
        self,
        server_id: int,
        name: str,
        ipv4_id: Optional[int] = None,
        ipv6_id: Optional[int] = None,
        status: str = "running",
        protected: bool = False,
    ) -> dict:
        with self.lock:
            server = {
                "id": server_id,
                "name": name,
                "status": status,
                "protection": {"delete": protected, "rebuild": protected},
                "public_net": {"ipv4": None, "ipv6": None},
                "private_net": [],
            }
            if ipv4_id:
                server["public_net"]["ipv4"] = {
                    "id": ipv4_id,
                    "ip": self.primary_ips[ipv4_id]["ip"],
                    "blocked": False,
                    "dns_ptr": f"{name}.example.com",
                }
            if ipv6_id:
                server["public_net"]["ipv6"] = {
                    "id": ipv6_id,
                    "ip": self.primary_ips[ipv6_id]["ip"],
                    "blocked": False,
                }
            self.servers[server_id] = server
            return server

    def add_primary_ip(
        self,
        ip_id: int,
        ip: str,
        ip_type: str = "ipv4",
        assignee_id: Optional[int] = None,
        auto_delete: bool = False,
        protected: bool = False,
    ) -> dict:
        with self.lock:
            item = {
                "id": ip_id,
                "ip": ip,
                "type": ip_type,
                "assignee_type": "server" if assignee_id else "unassigned",
                "assignee_id": assignee_id,
                "auto_delete": auto_delete,
                "protection": {"delete": protected},
            }
            self.primary_ips[ip_id] = item
            return item

    def add_floating_ip(
        self, ip_id: int, ip: str, server: Optional[int] = None, protected: bool = False
    ) -> dict:
        with self.lock:
            item = {"id": ip_id, "ip": ip, "server": server, "protection": {"delete": protected}}
            self.floating_ips[ip_id] = item
            return item

    def add_image(self, image_id: int, description: str, protected: bool = True, status: str = "available") -> dict:
        with self.lock:
            image = {
                "id": image_id,
                "description": description,
                "type": "snapshot",
                "status": status,
                "protection": {"delete": protected},
                "labels": {},
            }
            self.images[image_id] = image
            return image

    def add_load_balancer(self, lb_id: int, targets: list[dict]) -> dict:
        with self.lock:
            lb = {"id": lb_id, "name": f"lb-{lb_id}", "targets": list(targets)}
            self.load_balancers.append(lb)
            return lb

    # -------------------------------------------------------------- actions
    def _create_action(self, command: str, final: str = "success", error: Optional[dict] = None) -> dict:
        self._next_action_id += 1
        action = {
            "id": self._next_action_id,
            "command": command,
            "status": "success",
            "progress": 100,
            "started": _iso(),
            "finished": _iso(),
            "error": None,
        }
        if final != "success":
            action["status"] = "running"
            action["progress"] = 0
            action["finished"] = None
            plan = {"status": final, "error": error}

            def _finish(aid: int = action["id"], plan: dict = plan) -> None:
                act = self.actions.get(aid)
                if not act:
                    return
                act["status"] = plan["status"]
                act["error"] = plan["error"]
                act["progress"] = 100
                act["finished"] = _iso()

            self.actions[action["id"]] = action
            self._schedule(0.02, _finish)
            return action
        self.actions[action["id"]] = action
        return action

    def _take_action_error(self) -> Optional[dict]:
        err = self.next_action_error
        self.next_action_error = None
        return err

    # ------------------------------------------------------------- routing
    def handle(self, method: str, path: str, query: dict, body: Any) -> tuple[int, Any]:
        with self.lock:
            if path.startswith("/v1"):
                path = path[3:]
            path = path.rstrip("/") or "/"
            parts = [p for p in path.split("/") if p]

            if not parts:
                return 200, {"message": "hetzner mock"}

            if parts[0] == "actions" and len(parts) == 2 and method == "GET":
                action = self.actions.get(int(parts[1]))
                if action is None:
                    raise ApiError(404, "not_found", "action not found")
                return 200, {"action": action}

            if parts[0] == "servers":
                return self._servers(method, parts, query, body)
            if parts[0] == "primary_ips":
                return self._primary_ips(method, parts, query, body)
            if parts[0] == "floating_ips":
                return self._floating_ips(method, parts, query, body)
            if parts[0] == "images":
                return self._images(method, parts, query, body)
            if parts[0] == "load_balancers":
                return self._load_balancers(method, parts, query, body)
            raise ApiError(404, "not_found", f"no route for {method} {path}")

    # -------------------------------------------------------------- servers
    def _servers(self, method: str, parts: list[str], query: dict, body: Any) -> tuple[int, Any]:
        if len(parts) == 1 and method == "GET":
            page = int((query.get("page") or ["1"])[0])
            per_page = int((query.get("per_page") or ["50"])[0])
            items = list(self.servers.values())
            chunk = items[(page - 1) * per_page : page * per_page]
            meta = {"pagination": {"page": page, "per_page": per_page, "total_entries": len(items)}}
            if page * per_page < len(items):
                meta["pagination"]["next_page"] = page + 1
            return 200, {"servers": [dict(s) for s in chunk], "meta": meta}

        server_id = int(parts[1])
        server = self.servers.get(server_id)

        if len(parts) == 2 and method == "GET":
            if server is None:
                raise ApiError(404, "not_found", "server not found")
            return 200, {"server": dict(server)}

        if len(parts) == 2 and method == "DELETE":
            if server is None:
                raise ApiError(404, "not_found", "server not found")
            self._delete_server(server)
            return 200, {"action": self._create_action("delete_server")}

        if len(parts) == 4 and parts[2] == "actions" and method == "POST":
            if server is None:
                raise ApiError(404, "not_found", "server not found")
            action_name = parts[3]
            if action_name == "change_protection":
                delete = bool((body or {}).get("delete"))
                rebuild = bool((body or {}).get("rebuild", delete))
                server["protection"] = {"delete": delete, "rebuild": rebuild}
                self.actions_performed.append(
                    {"command": "change_server_protection", "server_id": server_id, "delete": delete}
                )
                return 201, {"action": self._create_action("change_server_protection")}
            if action_name in ("shutdown", "poweroff"):
                if server["status"] != "off":
                    server["status"] = "off"
                    self.actions_performed.append(
                        {"command": action_name, "server_id": server_id}
                    )
                return 201, {"action": self._create_action(action_name)}
            if action_name == "create_image":
                return self._create_image(server, body or {})
            raise ApiError(404, "not_found", f"unknown server action {action_name}")

        raise ApiError(404, "not_found", f"no route for {method} {'/'.join(parts)}")

    def _delete_server(self, server: dict) -> None:
        sid = int(server["id"])
        protection = server.get("protection") or {}
        if protection.get("delete") or protection.get("rebuild"):
            raise ApiError(422, "protected", "server is protected")
        attached = self._attached_ip_ids(server)
        for ip_id in attached:
            ip = self.primary_ips.get(ip_id)
            if ip and (ip.get("protection") or {}).get("delete"):
                raise ApiError(
                    422,
                    "protected",
                    "server has a protected primary IP assigned; "
                    "delete protection must be disabled first",
                )
        self.server_delete_snapshots.append(
            {
                "server_id": sid,
                "server_protection": dict(protection),
                "primary_ips": {
                    ip_id: {
                        "auto_delete": self.primary_ips[ip_id].get("auto_delete"),
                        "protection": dict(self.primary_ips[ip_id].get("protection") or {}),
                        "assignee_id": self.primary_ips[ip_id].get("assignee_id"),
                    }
                    for ip_id in attached
                    if ip_id in self.primary_ips
                },
                "at": time.time(),
            }
        )
        server["status"] = "deleting"
        self.actions_performed.append({"command": "delete_server", "server_id": sid})
        if not self.keep_server_after_delete:
            self._schedule(self.server_remove_delay, lambda: self._remove_server(sid))
        if self.delete_detach_delay or self.server_remove_delay:
            self._schedule(self.delete_detach_delay, lambda: self._detach_server_ips(sid))
        else:
            self._detach_server_ips(sid)

    def _attached_ip_ids(self, server: dict) -> list[int]:
        ids = []
        public_net = server.get("public_net") or {}
        for key in ("ipv4", "ipv6"):
            net = public_net.get(key) or {}
            if net.get("id"):
                ids.append(int(net["id"]))
        return ids

    def _detach_server_ips(self, server_id: int) -> None:
        server = self.servers.get(server_id)
        if server is None:
            return
        for ip_id in self._attached_ip_ids(server):
            ip = self.primary_ips.get(ip_id)
            if ip is None:
                continue
            if ip.get("auto_delete"):
                self.primary_ips.pop(ip_id, None)
                self.actions_performed.append(
                    {"command": "auto_delete_primary_ip", "primary_ip_id": ip_id}
                )
            else:
                ip["assignee_id"] = None
                ip["assignee_type"] = "unassigned"

    def _remove_server(self, server_id: int) -> None:
        server = self.servers.get(server_id)
        if server is None:
            return
        self._detach_server_ips(server_id)
        self.servers.pop(server_id, None)

    # ---------------------------------------------------------- primary ips
    def _primary_ips(self, method: str, parts: list[str], query: dict, body: Any) -> tuple[int, Any]:
        if len(parts) == 1 and method == "GET":
            page = int((query.get("page") or ["1"])[0])
            per_page = int((query.get("per_page") or ["50"])[0])
            items = list(self.primary_ips.values())
            chunk = items[(page - 1) * per_page : page * per_page]
            meta = {"pagination": {"page": page, "per_page": per_page, "total_entries": len(items)}}
            if page * per_page < len(items):
                meta["pagination"]["next_page"] = page + 1
            return 200, {"primary_ips": [dict(p) for p in chunk], "meta": meta}

        ip_id = int(parts[1])
        ip = self.primary_ips.get(ip_id)

        if len(parts) == 2 and method == "GET":
            if ip is None:
                raise ApiError(404, "not_found", "primary ip not found")
            return 200, {"primary_ip": dict(ip)}

        if len(parts) == 2 and method == "PUT":
            if ip is None:
                raise ApiError(404, "not_found", "primary ip not found")
            payload = body or {}
            if "auto_delete" in payload:
                want = bool(payload["auto_delete"])
                if want and self.reject_auto_delete_true:
                    raise ApiError(
                        422, "invalid_input", "auto_delete cannot be enabled for this IP"
                    )
                if want and (ip.get("protection") or {}).get("delete"):
                    raise ApiError(
                        422, "invalid_input", "auto_delete cannot be enabled while protection is on"
                    )
                ip["auto_delete"] = want
                self.actions_performed.append(
                    {"command": "update_primary_ip", "primary_ip_id": ip_id, "auto_delete": want}
                )
            if "name" in payload:
                ip["name"] = payload["name"]
            return 200, {"primary_ip": dict(ip)}

        if len(parts) == 2 and method == "DELETE":
            if ip is None:
                raise ApiError(404, "not_found", "primary ip not found")
            if (ip.get("protection") or {}).get("delete"):
                raise ApiError(422, "protected", "primary ip is protected")
            if ip.get("assignee_id") is not None:
                raise ApiError(422, "must_be_unassigned", "primary ip must be unassigned")
            self.primary_ips.pop(ip_id, None)
            self.actions_performed.append({"command": "delete_primary_ip", "primary_ip_id": ip_id})
            return 204, None

        if len(parts) == 4 and parts[2] == "actions" and method == "POST":
            if ip is None:
                raise ApiError(404, "not_found", "primary ip not found")
            action_name = parts[3]
            if action_name == "change_protection":
                want = bool((body or {}).get("delete"))
                if want and ip.get("auto_delete"):
                    raise ApiError(
                        422,
                        "invalid_input",
                        "protection can only be enabled when auto_delete is false",
                    )
                ip["protection"] = {"delete": want}
                self.actions_performed.append(
                    {"command": "change_primary_ip_protection", "primary_ip_id": ip_id, "delete": want}
                )
                return 201, {"action": self._create_action("change_primary_ip_protection")}
            if action_name == "unassign":
                assignee = ip.get("assignee_id")
                if assignee is None:
                    return 201, {"action": self._create_action("unassign_primary_ip")}
                owner = self.servers.get(int(assignee))
                if owner is not None and owner.get("status") != "off":
                    raise ApiError(422, "server_not_stopped", "server must be powered off")
                if self.lb_blocks_unassign and self._server_is_lb_target(int(assignee)):
                    raise ApiError(
                        422,
                        "server_is_load_balancer_target",
                        "server is a load balancer target",
                    )
                ip["assignee_id"] = None
                ip["assignee_type"] = "unassigned"
                self.actions_performed.append(
                    {"command": "unassign_primary_ip", "primary_ip_id": ip_id}
                )
                return 201, {"action": self._create_action("unassign_primary_ip")}
            raise ApiError(404, "not_found", f"unknown primary ip action {action_name}")

        raise ApiError(404, "not_found", f"no route for {method} {'/'.join(parts)}")

    def _server_is_lb_target(self, server_id: int) -> bool:
        server = self.servers.get(server_id)
        addresses: set[str] = set()
        if server:
            public_net = server.get("public_net") or {}
            for key in ("ipv4", "ipv6"):
                net = public_net.get(key) or {}
                value = net.get("ip")
                if value:
                    addresses.add(str(value))
                    addresses.add(str(value).split("/")[0])
        for lb in self.load_balancers:
            for target in lb.get("targets") or []:
                kind = target.get("type")
                if kind == "server" and (target.get("server") or {}).get("id") == server_id:
                    return True
                if kind == "ip":
                    raw = target.get("ip")
                    addr = raw.get("ip") if isinstance(raw, dict) else raw
                    if addr and ({str(addr), str(addr).split("/")[0]} & addresses):
                        return True
                if kind == "label_selector":
                    for resolved in target.get("targets") or []:
                        if (resolved.get("server") or {}).get("id") == server_id:
                            return True
        return False

    # --------------------------------------------------------- floating ips
    def _floating_ips(self, method: str, parts: list[str], query: dict, body: Any) -> tuple[int, Any]:
        if len(parts) == 1 and method == "GET":
            return 200, {"floating_ips": [dict(f) for f in self.floating_ips.values()], "meta": {}}

        ip_id = int(parts[1])
        ip = self.floating_ips.get(ip_id)

        if len(parts) == 2 and method == "GET":
            if ip is None:
                raise ApiError(404, "not_found", "floating ip not found")
            return 200, {"floating_ip": dict(ip)}

        if len(parts) == 2 and method == "DELETE":
            if ip is None:
                raise ApiError(404, "not_found", "floating ip not found")
            if (ip.get("protection") or {}).get("delete"):
                raise ApiError(422, "protected", "floating ip is protected")
            if ip.get("server") is not None:
                raise ApiError(422, "must_be_unassigned", "floating ip must be unassigned")
            self.floating_ips.pop(ip_id, None)
            self.actions_performed.append({"command": "delete_floating_ip", "floating_ip_id": ip_id})
            return 204, None

        if len(parts) == 4 and parts[2] == "actions" and method == "POST":
            if ip is None:
                raise ApiError(404, "not_found", "floating ip not found")
            if parts[3] == "change_protection":
                want = bool((body or {}).get("delete"))
                ip["protection"] = {"delete": want}
                self.actions_performed.append(
                    {"command": "change_floating_ip_protection", "floating_ip_id": ip_id, "delete": want}
                )
                return 201, {"action": self._create_action("change_floating_ip_protection")}
            if parts[3] == "unassign":
                ip["server"] = None
                self.actions_performed.append(
                    {"command": "unassign_floating_ip", "floating_ip_id": ip_id}
                )
                return 201, {"action": self._create_action("unassign_floating_ip")}
            raise ApiError(404, "not_found", "unknown floating ip action")

        raise ApiError(404, "not_found", f"no route for {method} {'/'.join(parts)}")

    # --------------------------------------------------------------- images
    def _images(self, method: str, parts: list[str], query: dict, body: Any) -> tuple[int, Any]:
        if len(parts) == 1 and method == "GET":
            return 200, {"images": [dict(i) for i in self.images.values()], "meta": {}}
        image_id = int(parts[1])
        image = self.images.get(image_id)
        if len(parts) == 2 and method == "GET":
            if image is None:
                raise ApiError(404, "not_found", "image not found")
            return 200, {"image": dict(image)}
        if len(parts) == 2 and method == "DELETE":
            if image is None:
                raise ApiError(404, "not_found", "image not found")
            if (image.get("protection") or {}).get("delete"):
                raise ApiError(422, "protected", "image is protected")
            self.images.pop(image_id, None)
            return 204, None
        if len(parts) == 4 and parts[2] == "actions" and method == "POST":
            if image is None:
                raise ApiError(404, "not_found", "image not found")
            if parts[3] == "change_protection":
                want = bool((body or {}).get("delete"))
                image["protection"] = {"delete": want}
                self.actions_performed.append(
                    {"command": "change_image_protection", "image_id": image_id, "delete": want}
                )
                return 201, {"action": self._create_action("change_image_protection")}
            raise ApiError(404, "not_found", "unknown image action")
        raise ApiError(404, "not_found", f"no route for {method} {'/'.join(parts)}")

    def _create_image(self, server: dict, body: dict) -> tuple[int, Any]:
        self._next_image_id += 1
        image_id = self._next_image_id
        if body.get("type") != "snapshot":
            raise ApiError(422, "invalid_input", "type must be snapshot")
        self.snapshot_seen_status = server.get("status")
        image = {
            "id": image_id,
            "description": body.get("description", ""),
            "type": "snapshot",
            "status": "creating",
            "protection": {"delete": False},
            "labels": body.get("labels") or {},
            "image_size": 5.0,
            "disk_size": 40,
        }
        self.images[image_id] = image
        self.actions_performed.append(
            {"command": "create_snapshot", "server_id": server["id"], "image_id": image_id}
        )
        error = self._take_action_error()
        action = self._create_action(
            "create_image", final="error" if error else "success", error=error
        )

        def _ready() -> None:
            img = self.images.get(image_id)
            if img is not None and img.get("status") == "creating":
                img["status"] = "available"

        self._schedule(self.image_ready_delay, _ready)
        return 201, {"image": dict(image), "action": action}

    # ------------------------------------------------------ load balancers
    def _load_balancers(self, method: str, parts: list[str], query: dict, body: Any) -> tuple[int, Any]:
        if len(parts) == 1 and method == "GET":
            return 200, {"load_balancers": [dict(lb) for lb in self.load_balancers], "meta": {}}
        lb_id = int(parts[1])
        lb = next((item for item in self.load_balancers if item["id"] == lb_id), None)
        if lb is None:
            raise ApiError(404, "not_found", "load balancer not found")
        if len(parts) == 4 and parts[2] == "actions" and parts[3] == "remove_target":
            target = body or {}
            kind = target.get("type")
            if kind == "server":
                sid = (target.get("server") or {}).get("id")
                lb["targets"] = [
                    t
                    for t in lb["targets"]
                    if not (t.get("type") == "server" and (t.get("server") or {}).get("id") == sid)
                ]
            elif kind == "ip":
                addr = (target.get("ip") or {}).get("ip")
                lb["targets"] = [
                    t
                    for t in lb["targets"]
                    if not (t.get("type") == "ip" and (t.get("ip") or {}).get("ip") == addr)
                ]
            elif kind == "label_selector":
                selector = (target.get("label_selector") or {}).get("selector")
                lb["targets"] = [
                    t
                    for t in lb["targets"]
                    if not (
                        t.get("type") == "label_selector"
                        and (t.get("label_selector") or {}).get("selector") == selector
                    )
                ]
            else:
                raise ApiError(422, "invalid_input", "unknown target type")
            self.actions_performed.append(
                {"command": "remove_lb_target", "load_balancer_id": lb_id, "target": target}
            )
            return 201, {"action": self._create_action("remove_target")}
        raise ApiError(404, "not_found", f"no route for {method} {'/'.join(parts)}")

    # ------------------------------------------------------------ helpers
    def find_requests(self, method: str, path_contains: str) -> list[dict]:
        with self.lock:
            return [
                r
                for r in self.requests
                if r["method"] == method and path_contains in r["path"]
            ]

    def request_sequence(self) -> list[tuple[str, str]]:
        with self.lock:
            return [(r["method"], r["path"]) for r in self.requests]


def _iso() -> str:
    from datetime import datetime, timezone

    return (
        datetime.now(tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


# --------------------------------------------------------------------------- #
# Telegram Bot API
# --------------------------------------------------------------------------- #
class TelegramMock(_AppServer):
    def __init__(self, chat_id: str = "424242", user_id: str = "777") -> None:
        self.lock = threading.RLock()
        self.chat_id = str(chat_id)
        self.user_id = str(user_id)
        self.messages: list[dict] = []
        self.update_queue: list[dict] = []
        self.requests: list[dict] = []
        self.faults: list[Fault] = []
        self._next_update_id = 1
        self._next_message_id = 1
        super().__init__()

    # ----------------------------------------------------------- inventory
    def enqueue(self, text: str, chat_id: Optional[str] = None, user_id: Optional[str] = None) -> int:
        with self.lock:
            update_id = self._next_update_id
            self._next_update_id += 1
            self.update_queue.append(
                {
                    "update_id": update_id,
                    "message": {
                        "message_id": update_id,
                        "date": int(time.time()),
                        "from": {"id": int(user_id or self.user_id), "is_bot": False},
                        "chat": {"id": int(chat_id or self.chat_id), "type": "private"},
                        "text": text,
                    },
                }
            )
            return update_id

    def texts(self) -> list[str]:
        with self.lock:
            return [m["text"] for m in self.messages]

    def sent_containing(self, needle: str) -> list[str]:
        return [t for t in self.texts() if needle in t]

    def clear(self) -> None:
        with self.lock:
            self.messages.clear()
            self.requests.clear()

    # -------------------------------------------------------------- routing
    def handle(self, method: str, path: str, query: dict, body: Any) -> tuple[int, Any]:
        # /bot<token>/<method>
        match = re.match(r"^/bot([^/]+)/([A-Za-z]+)$", path)
        if not match:
            raise ApiError(404, "not_found", f"no route for {path}")
        token, api_method = match.group(1), match.group(2)
        payload = body or {}
        with self.lock:
            self.requests.append({"method": api_method, "path": path, "body": payload})
            fault = self.take_fault("POST", path)
            if fault is not None:
                if fault.action == "timeout":
                    time.sleep(fault.delay)
                    raise ApiError(500, "timeout", "injected timeout")
                if fault.action == "malformed":
                    return 200, None
                status = fault.status or int(fault.action.split("_", 1)[1])
                raise ApiError(status, fault.code, fault.message)

            if api_method == "sendMessage":
                text = str(payload.get("text") or "")
                self._next_message_id += 1
                result = {
                    "message_id": self._next_message_id,
                    "date": int(time.time()),
                    "chat": {"id": int(payload.get("chat_id") or self.chat_id), "type": "private"},
                    "text": text,
                }
                self.messages.append(
                    {"chat_id": str(payload.get("chat_id")), "text": text, "result": result}
                )
                return 200, {"ok": True, "result": result}

            if api_method == "getUpdates":
                offset = int(payload.get("offset") or 0)
                limit = int(payload.get("limit") or 50)
                timeout = float(payload.get("timeout") or 0)
                available = [u for u in self.update_queue if u["update_id"] >= offset]
                if not available and timeout > 0:
                    # bounded long poll; tests keep this short
                    pass
                return 200, {"ok": True, "result": available[:limit]}

            if api_method == "getMe":
                return 200, {"ok": True, "result": {"id": 1, "is_bot": True, "username": "deadman_bot"}}

            if api_method == "deleteWebhook":
                return 200, {"ok": True, "result": True}

            raise ApiError(404, "not_found", f"unknown telegram method {api_method}")

    def take_fault(self, method: str, path: str) -> Optional[Fault]:
        with self.lock:
            for fault in self.faults:
                if fault.matches(method, path):
                    if fault.times > 0:
                        fault.times -= 1
                    return fault
        return None


# --------------------------------------------------------------------------- #
# Billing webhook
# --------------------------------------------------------------------------- #
class BillingMock(_AppServer):
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.payloads: list[dict] = []
        self.status = 200
        self.delay = 0.0
        self.requests: list[dict] = []
        super().__init__()

    def handle(self, method: str, path: str, query: dict, body: Any) -> tuple[int, Any]:
        if path.rstrip("/") not in ("/billing", "/hook"):
            raise ApiError(404, "not_found", "no route")
        with self.lock:
            self.requests.append({"method": method, "path": path, "body": body})
            self.payloads.append(body or {})
            status = self.status
            delay = self.delay
        if delay:
            time.sleep(delay)
        if status >= 400:
            raise ApiError(status, "webhook_error", "injected webhook failure")
        return status or 200, {"ok": True}
