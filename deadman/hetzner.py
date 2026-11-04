"""Hetzner Cloud API v1 client (stdlib only).

Design notes that matter for this project:

* The process runs **on the main server**.  There is deliberately no
  ``shutdown``/``poweroff`` call for the main server in the teardown path; the
  snapshot is a live snapshot of a running server.  ``ensure_off`` refuses to
  touch the main server unless explicitly allowed.
* ``DELETE`` may return 200 (servers) or 204 (IPs); both are success, 404 is
  success for idempotent deletes.
* Every mutation returns an async action; callers poll ``GET /actions/{id}``.
* Rate-limit headers are tracked and honoured; 429 is a transport retry and
  never consumes a step's retry budget.
"""

from __future__ import annotations

import json
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Iterable, Optional

from .config import Config
from .log import JsonLogger, null_logger

USER_AGENT = "hetzner-deadman/1.0"
MAX_PAGES = 200
PER_PAGE = 50

TRANSIENT = "TRANSIENT"
RATE_LIMITED = "RATE_LIMITED"
ALREADY_DONE = "ALREADY_DONE"
PRECONDITION = "PRECONDITION"
PERMANENT = "PERMANENT"
UNKNOWN = "UNKNOWN"

_CLASS_BY_CODE = {
    "unauthorized": PERMANENT,
    "forbidden": PERMANENT,
    "invalid_input": PERMANENT,
    "invalid_token": PERMANENT,
    "token_readonly": PERMANENT,
    "server_not_stopped": PRECONDITION,
    "must_be_unassigned": PRECONDITION,
    "server_is_load_balancer_target": PRECONDITION,
    "conflict": TRANSIENT,
    "locked": TRANSIENT,
    "unavailable": TRANSIENT,
}


class HCloudError(RuntimeError):
    def __init__(
        self,
        message: str,
        status: Optional[int] = None,
        code: Optional[str] = None,
        details: Any = None,
        method: Optional[str] = None,
        path: Optional[str] = None,
        classification: str = UNKNOWN,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.details = details
        self.method = method
        self.path = path
        self.classification = classification

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "code": self.code,
            "message": str(self),
            "details": self.details,
            "method": self.method,
            "path": self.path,
            "class": self.classification,
        }

    def __str__(self) -> str:  # pragma: no cover
        bits = []
        if self.status:
            bits.append(f"HTTP {self.status}")
        if self.code:
            bits.append(str(self.code))
        return f"{' '.join(bits)}: {super().__str__()}" if bits else super().__str__()


class NotFound(HCloudError):
    def __init__(self, message: str = "not found", **kwargs: Any) -> None:
        kwargs.setdefault("status", 404)
        kwargs.setdefault("classification", ALREADY_DONE)
        super().__init__(message, **kwargs)


def classify(status: Optional[int], code: Optional[str]) -> str:
    if status == 404:
        return ALREADY_DONE
    if status == 429:
        return RATE_LIMITED
    if status in (401, 403):
        return PERMANENT
    if status and 500 <= status <= 599:
        return TRANSIENT
    if code and code in _CLASS_BY_CODE:
        return _CLASS_BY_CODE[code]
    if status == 409:
        return TRANSIENT
    if status and 400 <= status <= 499:
        return PERMANENT
    return UNKNOWN


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


class HCloudClient:
    def __init__(
        self,
        config: Config,
        state: Optional[dict] = None,
        logger: Optional[JsonLogger] = None,
        event_sink: Optional[Callable[[str, dict], None]] = None,
    ) -> None:
        self.config = config
        self.state = state if state is not None else {}
        self.logger = logger or null_logger()
        self.event_sink = event_sink
        self.base_url = config.base_url.rstrip("/")
        self.dry_run = bool(config.dry_run)
        self.main_server_id: Optional[int] = config.main_server_id
        self._opener = urllib.request.build_opener(_NoRedirect())
        self._rate = {"limit": None, "remaining": None, "reset": None}
        self.dry_run_log: list[dict[str, Any]] = []
        self.request_count = 0
        #: Optional callback invoked between poll sleeps.  The engine uses it to
        #: keep reading Telegram while a long action/snapshot wait is in flight,
        #: so /abort still works during a 30-minute image build.
        self.poll_hook: Optional[Callable[[], None]] = None

    def _call_poll_hook(self) -> None:
        if self.poll_hook is not None:
            self.poll_hook()

    # --------------------------------------------------------------- helpers
    def _url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return self.base_url + "/" + path.lstrip("/")

    def _record(self, method: str, path: str, status: Optional[int], body: Any = None) -> None:
        self.request_count += 1
        counters = self.state.setdefault("counters", {})
        counters["api_calls"] = int(counters.get("api_calls", 0)) + 1
        if status and status >= 400:
            counters["errors"] = int(counters.get("errors", 0)) + 1
        if self.event_sink:
            try:
                self.event_sink(
                    "api_request",
                    {"method": method, "path": path, "status": status},
                )
            except Exception:
                pass

    def _update_rate(self, headers: Any) -> None:
        if headers is None:
            return

        def _int(name: str) -> Optional[int]:
            try:
                raw = headers.get(name)
                return int(raw) if raw is not None else None
            except (TypeError, ValueError):
                return None

        limit = _int("RateLimit-Limit")
        remaining = _int("RateLimit-Remaining")
        reset = _int("RateLimit-Reset")
        if limit is not None:
            self._rate["limit"] = limit
        if remaining is not None:
            self._rate["remaining"] = remaining
        if reset is not None:
            self._rate["reset"] = reset
        if any(v is not None for v in (limit, remaining, reset)):
            self.state["rate_limit"] = dict(self._rate)
            self.state.setdefault("counters", {})["rate_limit"] = dict(self._rate)

    def _throttle(self) -> None:
        floor = self.config.rate_limit_floor
        remaining = self._rate.get("remaining")
        reset = self._rate.get("reset")
        if remaining is not None and reset is not None and remaining < floor:
            wait = int(reset) - int(time.time()) + 5
            if wait > 0:
                self.logger.warning(
                    "rate_limit_throttle",
                    f"only {remaining} requests left; sleeping {wait}s",
                    remaining=remaining,
                    reset=reset,
                )
                time.sleep(min(wait, 3600))

    # --------------------------------------------------------------- request
    def request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        expected: Iterable[int] = (200,),
        timeout: Optional[float] = None,
        allow_404: bool = False,
        dry_run_result: Optional[dict] = None,
    ) -> dict:
        method = method.upper()
        if self.dry_run and method != "GET":
            entry = {"method": method, "path": path, "body": body}
            self.dry_run_log.append(entry)
            self.logger.info("dry_run_mutation", f"would {method} {path}", **entry)
            self._record(method, path, None)
            return dry_run_result if dry_run_result is not None else {}

        self._throttle()
        attempt = 0
        while True:
            attempt += 1
            started = time.time()
            data_bytes: Optional[bytes] = None
            headers = {
                "Authorization": f"Bearer {self.config.token}",
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            }
            if body is not None:
                data_bytes = json.dumps(body).encode("utf-8")
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(
                self._url(path),
                data=data_bytes,
                method=method,
                headers=headers,
            )
            try:
                with self._opener.open(req, timeout=timeout or self.config.request_timeout_seconds) as resp:
                    raw = resp.read()
                    status = resp.status
                    self._update_rate(resp.headers)
                    self._record(method, path, status)
                    duration = round(time.time() - started, 3)
                    self.logger.debug(
                        "api_response",
                        f"{method} {path} -> {status}",
                        method=method,
                        path=path,
                        status=status,
                        duration=duration,
                    )
                    if not raw:
                        return {}
                    try:
                        return json.loads(raw.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        return {}
            except urllib.error.HTTPError as exc:
                raw = b""
                try:
                    raw = exc.read()
                except Exception:
                    pass
                finally:
                    # HTTPError is an open response object; not closing it leaks
                    # a socket and emits ResourceWarning under -W error.
                    try:
                        exc.close()
                    except Exception:
                        pass
                payload: dict[str, Any] = {}
                try:
                    payload = json.loads(raw.decode("utf-8")) if raw else {}
                except (UnicodeDecodeError, json.JSONDecodeError):
                    payload = {}
                self._update_rate(exc.headers)
                self._record(method, path, exc.code)
                err = payload.get("error") if isinstance(payload, dict) else None
                code = err.get("code") if isinstance(err, dict) else None
                message = err.get("message") if isinstance(err, dict) else str(exc)
                details = err.get("details") if isinstance(err, dict) else None
                classification = classify(exc.code, code)
                self.logger.error(
                    "api_error",
                    f"{method} {path} -> {exc.code} {code or ''}: {message}",
                    method=method,
                    path=path,
                    status=exc.code,
                    code=code,
                    class_=classification,
                )
                if exc.code == 429 and attempt <= self.config.rate_limit_max_wait:
                    reset = self._rate.get("reset")
                    wait = max(5, int(reset) - int(time.time()) + 5) if reset else 60
                    self.logger.warning("rate_limited", f"429, sleeping {wait}s")
                    time.sleep(min(wait, 3600))
                    continue
                if exc.code == 404 and allow_404:
                    raise NotFound(message, status=404, code=code, method=method, path=path)
                raise HCloudError(
                    message or str(exc),
                    status=exc.code,
                    code=code,
                    details=details,
                    method=method,
                    path=path,
                    classification=classification,
                ) from None
            except (urllib.error.URLError, socket.timeout, TimeoutError, ssl.SSLError, OSError) as exc:
                self._record(method, path, None)
                self.logger.error(
                    "api_network_error", f"{method} {path}: {exc}", method=method, path=path
                )
                raise HCloudError(
                    f"network error: {exc}",
                    method=method,
                    path=path,
                    classification=TRANSIENT,
                ) from None

    def _get(self, path: str, allow_404: bool = False) -> dict:
        return self.request("GET", path, expected=(200,), allow_404=allow_404)

    # ------------------------------------------------------------ pagination
    def _list(self, path: str, root: str, query: Optional[dict] = None) -> list[dict]:
        page = 1
        collected: list[dict] = []
        seen: set[Any] = set()
        while page <= MAX_PAGES:
            params = dict(query or {})
            params["page"] = page
            params["per_page"] = PER_PAGE
            qs = urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
            data = self._get(f"{path}?{qs}" if qs else path)
            items = data.get(root) or []
            if not isinstance(items, list):
                items = []
            for item in items:
                key = item.get("id")
                if key in seen:
                    continue
                seen.add(key)
                collected.append(item)
            meta = (data.get("meta") or {}).get("pagination") or {}
            nxt = meta.get("next_page")
            if not nxt:
                break
            try:
                nxt = int(nxt)
            except (TypeError, ValueError):
                break
            if nxt <= page:
                self.logger.warning("pagination_loop", f"next_page {nxt} <= page {page}", path=path)
                break
            page = nxt
        return collected

    # --------------------------------------------------------------- actions
    def wait_action(self, action: Any, timeout: Optional[float] = None) -> dict:
        if self.dry_run:
            return {"status": "success", "dry_run": True}
        action_id = action.get("id") if isinstance(action, dict) else action
        if not action_id:
            return {"status": "success"}
        deadline = time.time() + (timeout or self.config.action_timeout_seconds)
        delay = max(0.5, float(self.config.action_poll_seconds))
        while True:
            data = self._get(f"/actions/{int(action_id)}")
            act = data.get("action") or {}
            status = act.get("status")
            if status == "success":
                return act
            if status == "error":
                err = act.get("error") or {}
                code = err.get("code")
                raise HCloudError(
                    err.get("message") or f"action {action_id} failed",
                    code=code,
                    details=err,
                    method="ACTION",
                    path=f"/actions/{action_id}",
                    classification=classify(None, code),
                )
            if time.time() >= deadline:
                raise HCloudError(
                    f"action {action_id} timed out after {timeout or self.config.action_timeout_seconds}s",
                    code="action_timeout",
                    classification=TRANSIENT,
                )
            self._call_poll_hook()
            time.sleep(delay)

    def _post_action(self, path: str, body: Optional[dict] = None) -> dict:
        data = self.request("POST", path, body=body, expected=(201, 200))
        return data.get("action") or {}

    # --------------------------------------------------------------- servers
    def list_servers(self, label_selector: str = "") -> list[dict]:
        query = {}
        if label_selector:
            query["label_selector"] = label_selector
        return self._list("/servers", "servers", query)

    def get_server(self, server_id: int, allow_missing: bool = True) -> Optional[dict]:
        try:
            data = self._get(f"/servers/{int(server_id)}", allow_404=allow_missing)
        except NotFound:
            return None
        return data.get("server")

    def get_server_status(self, server_id: int) -> Optional[str]:
        server = self.get_server(server_id)
        return server.get("status") if server else None

    def delete_server(self, server_id: int) -> bool:
        """Returns True when the delete was accepted or the server is gone."""
        if self.dry_run:
            self.request("DELETE", f"/servers/{int(server_id)}")
            return True
        try:
            self.request(
                "DELETE", f"/servers/{int(server_id)}", expected=(200, 204), allow_404=True
            )
        except NotFound:
            return True
        return True

    def change_server_protection(
        self, server_id: int, delete: bool = False, rebuild: Optional[bool] = None
    ) -> dict:
        payload = {"delete": bool(delete), "rebuild": bool(delete if rebuild is None else rebuild)}
        return self._post_action(f"/servers/{int(server_id)}/actions/change_protection", payload)

    def ensure_off(self, server_id: int, allow_main: bool = False) -> None:
        """Graceful shutdown, then forced poweroff if needed.

        Never call this for the main server while the software runs on it: the
        machine would disappear mid-teardown.  The guard is intentional.
        """
        if self.main_server_id and int(server_id) == int(self.main_server_id) and not allow_main:
            raise HCloudError(
                "refusing to power off the main server (the software runs on it)",
                code="refuse_poweroff_main",
                classification=PERMANENT,
            )
        if self.dry_run:
            self.request("POST", f"/servers/{int(server_id)}/actions/shutdown")
            return
        server = self.get_server(server_id)
        if server is None:
            return
        status = server.get("status")
        if status == "off":
            return
        if status == "deleting":
            return
        try:
            self._post_action(f"/servers/{int(server_id)}/actions/shutdown")
        except HCloudError as exc:
            if exc.status == 404:
                return
            self.logger.warning("shutdown_failed", str(exc), server_id=server_id)
        if self._poll_status(server_id, {"off", "deleting"}, self.config.shutdown_grace_seconds):
            return
        self.logger.warning("shutdown_slow", "forcing poweroff", server_id=server_id)
        try:
            self._post_action(f"/servers/{int(server_id)}/actions/poweroff")
        except HCloudError as exc:
            if exc.status == 404:
                return
            raise
        if not self._poll_status(server_id, {"off", "deleting"}, self.config.poweroff_timeout_seconds):
            raise HCloudError(
                f"server {server_id} did not reach status 'off'",
                code="server_still_running",
                classification=TRANSIENT,
            )

    def _poll_delay(self) -> float:
        """Poll interval for status/image waits (never below 50 ms)."""
        try:
            return max(0.05, min(float(self.config.action_poll_seconds), 10.0))
        except (TypeError, ValueError):
            return 2.0

    def _poll_status(self, server_id: int, wanted: set[str], timeout: float) -> bool:
        deadline = time.time() + float(timeout)
        while time.time() < deadline:
            status = self.get_server_status(server_id)
            if status is None or status in wanted:
                return True
            self._call_poll_hook()
            time.sleep(self._poll_delay())
        return False

    # -------------------------------------------------------------- snapshots
    def create_snapshot(self, server_id: int, description: str, labels: dict) -> tuple[dict, dict]:
        data = self.request(
            "POST",
            f"/servers/{int(server_id)}/actions/create_image",
            body={"type": "snapshot", "description": description, "labels": labels},
            expected=(201, 200),
            dry_run_result={
                "image": {"id": 0, "status": "available", "type": "snapshot"},
                "action": {"id": 0, "status": "success"},
            },
        )
        return data.get("image") or {}, data.get("action") or {}

    def get_image(self, image_id: int, allow_missing: bool = True) -> Optional[dict]:
        try:
            data = self._get(f"/images/{int(image_id)}", allow_404=allow_missing)
        except NotFound:
            return None
        return data.get("image")

    def wait_image_available(self, image_id: int, timeout: Optional[float] = None) -> dict:
        if self.dry_run:
            return {"id": image_id, "status": "available", "dry_run": True}
        deadline = time.time() + (timeout or self.config.image_timeout_seconds)
        while True:
            image = self.get_image(image_id, allow_missing=False)
            if image is None:
                raise HCloudError(f"image {image_id} disappeared", status=404, classification=TRANSIENT)
            status = image.get("status")
            if status == "available":
                return image
            if status == "unavailable":
                raise HCloudError(
                    f"snapshot {image_id} failed (status unavailable)",
                    code="snapshot_unavailable",
                    classification=TRANSIENT,
                )
            if time.time() >= deadline:
                raise HCloudError(
                    f"image {image_id} not available within timeout",
                    code="image_timeout",
                    classification=TRANSIENT,
                )
            self._call_poll_hook()
            time.sleep(self._poll_delay())

    def protect_image(self, image_id: int, delete: bool = True) -> dict:
        if self.dry_run:
            return {}
        return self._post_action(f"/images/{int(image_id)}/actions/change_protection", {"delete": bool(delete)})

    # ------------------------------------------------------------ primary ips
    def list_primary_ips(self, label_selector: str = "") -> list[dict]:
        query = {"label_selector": label_selector} if label_selector else {}
        return self._list("/primary_ips", "primary_ips", query)

    def get_primary_ip(self, ip_id: int, allow_missing: bool = True) -> Optional[dict]:
        try:
            data = self._get(f"/primary_ips/{int(ip_id)}", allow_404=allow_missing)
        except NotFound:
            return None
        return data.get("primary_ip")

    def update_primary_ip(
        self,
        ip_id: int,
        auto_delete: Optional[bool] = None,
        name: Optional[str] = None,
        labels: Optional[dict] = None,
    ) -> dict:
        payload: dict[str, Any] = {}
        if auto_delete is not None:
            payload["auto_delete"] = bool(auto_delete)
        if name is not None:
            payload["name"] = name
        if labels is not None:
            payload["labels"] = labels
        if not payload:
            return {}
        data = self.request("PUT", f"/primary_ips/{int(ip_id)}", body=payload, expected=(200,))
        return data.get("primary_ip") or {}

    def unassign_primary_ip(self, ip_id: int) -> dict:
        return self._post_action(f"/primary_ips/{int(ip_id)}/actions/unassign")

    def change_primary_ip_protection(self, ip_id: int, delete: bool) -> dict:
        return self._post_action(
            f"/primary_ips/{int(ip_id)}/actions/change_protection", {"delete": bool(delete)}
        )

    def delete_primary_ip(self, ip_id: int) -> bool:
        if self.dry_run:
            self.request("DELETE", f"/primary_ips/{int(ip_id)}")
            return True
        try:
            self.request("DELETE", f"/primary_ips/{int(ip_id)}", expected=(204, 200), allow_404=True)
        except NotFound:
            return True
        return True

    # ----------------------------------------------------------- floating ips
    def list_floating_ips(self) -> list[dict]:
        return self._list("/floating_ips", "floating_ips")

    def get_floating_ip(self, ip_id: int, allow_missing: bool = True) -> Optional[dict]:
        try:
            data = self._get(f"/floating_ips/{int(ip_id)}", allow_404=allow_missing)
        except NotFound:
            return None
        return data.get("floating_ip")

    def change_floating_ip_protection(self, ip_id: int, delete: bool) -> dict:
        return self._post_action(
            f"/floating_ips/{int(ip_id)}/actions/change_protection", {"delete": bool(delete)}
        )

    def unassign_floating_ip(self, ip_id: int) -> dict:
        return self._post_action(f"/floating_ips/{int(ip_id)}/actions/unassign")

    def delete_floating_ip(self, ip_id: int) -> bool:
        if self.dry_run:
            self.request("DELETE", f"/floating_ips/{int(ip_id)}")
            return True
        try:
            self.request("DELETE", f"/floating_ips/{int(ip_id)}", expected=(204, 200), allow_404=True)
        except NotFound:
            return True
        return True

    # ----------------------------------------------------------- load balancer
    def list_load_balancers(self) -> list[dict]:
        return self._list("/load_balancers", "load_balancers")

    def remove_load_balancer_server_target(self, lb_id: int, server_id: int) -> dict:
        return self._post_action(
            f"/load_balancers/{int(lb_id)}/actions/remove_target",
            {"type": "server", "server": {"id": int(server_id)}},
        )

    def remove_load_balancer_ip_target(self, lb_id: int, ip: str) -> dict:
        return self._post_action(
            f"/load_balancers/{int(lb_id)}/actions/remove_target",
            {"type": "ip", "ip": {"ip": str(ip)}},
        )

    def remove_load_balancer_label_selector_target(self, lb_id: int, selector: str) -> dict:
        return self._post_action(
            f"/load_balancers/{int(lb_id)}/actions/remove_target",
            {"type": "label_selector", "label_selector": {"selector": str(selector)}},
        )

    @staticmethod
    def _server_address_set(server: Optional[dict]) -> set[str]:
        """Every address that identifies *server*: public IPv4, IPv6 (/64 and
        host form) and private IPs.  Used to match ``type: "ip"`` LB targets."""
        addresses: set[str] = set()
        if not server:
            return addresses
        public_net = server.get("public_net") or {}
        for key in ("ipv4", "ipv6"):
            net = public_net.get(key) or {}
            value = net.get("ip")
            if not value:
                continue
            addresses.add(str(value))
            addresses.add(str(value).split("/")[0])
        for net in server.get("private_net") or []:
            if net.get("ip"):
                addresses.add(str(net["ip"]))
        return addresses

    def remove_lb_targets_for_server(self, server_id: int) -> int:
        """Remove a server from every load balancer that targets it. Returns count.

        Three target shapes can pin the server and make
        ``unassign_primary_ip`` fail with ``422 server_is_load_balancer_target``:

        * ``{"type": "server", "server": {"id": N}}``
        * ``{"type": "ip", "ip": {"ip": "<one of the server's addresses>"}}``
        * ``{"type": "label_selector", ...}`` whose resolved ``targets[]``
          include the server.

        All three are handled; missing the ``ip`` shape leaves the unassign
        wedged forever.
        """
        removed = 0
        try:
            load_balancers = self.list_load_balancers()
        except HCloudError as exc:
            self.logger.warning("lb_list_failed", str(exc))
            return 0
        try:
            server = self.get_server(int(server_id))
        except HCloudError as exc:
            self.logger.warning("lb_server_lookup_failed", str(exc), server_id=server_id)
            server = None
        addresses = self._server_address_set(server)

        for lb in load_balancers:
            lb_id = lb.get("id")
            for target in lb.get("targets") or []:
                if not isinstance(target, dict):
                    continue
                kind = target.get("type")
                if kind == "server":
                    if (target.get("server") or {}).get("id") == int(server_id):
                        try:
                            self.remove_load_balancer_server_target(lb_id, server_id)
                            removed += 1
                        except HCloudError as exc:
                            self.logger.warning(
                                "lb_remove_target_failed", str(exc), lb_id=lb_id, target="server"
                            )
                elif kind == "ip":
                    raw = target.get("ip")
                    addr = raw.get("ip") if isinstance(raw, dict) else raw
                    if not addr:
                        continue
                    candidates = {str(addr), str(addr).split("/")[0]}
                    if candidates & addresses:
                        try:
                            self.remove_load_balancer_ip_target(lb_id, str(addr))
                            removed += 1
                        except HCloudError as exc:
                            self.logger.warning(
                                "lb_remove_target_failed", str(exc), lb_id=lb_id, target="ip"
                            )
                elif kind == "label_selector":
                    resolved = target.get("targets") or []
                    if any(
                        isinstance(t, dict)
                        and (t.get("server") or {}).get("id") == int(server_id)
                        for t in resolved
                    ):
                        selector = (target.get("label_selector") or {}).get("selector")
                        if not selector:
                            continue
                        try:
                            self.remove_load_balancer_label_selector_target(lb_id, selector)
                            removed += 1
                        except HCloudError as exc:
                            self.logger.warning(
                                "lb_remove_target_failed",
                                str(exc),
                                lb_id=lb_id,
                                target="label_selector",
                            )
        return removed

    # ------------------------------------------------------------- discovery
    @staticmethod
    def _metadata() -> dict:
        """Best-effort Hetzner Cloud metadata lookup (works on the VM itself).

        The metadata service is link-local and only reachable from inside a
        Hetzner Cloud server.  A 2 second timeout keeps other environments fast.
        """
        try:
            req = urllib.request.Request(
                "http://169.254.169.254/hetzner/v1/metadata",
                headers={"User-Agent": USER_AGENT},
            )
            with urllib.request.urlopen(req, timeout=2) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _local_ip_candidates() -> set[str]:
        ips: set[str] = set()
        try:
            hostname = socket.gethostname()
            for info in socket.getaddrinfo(hostname, None):
                addr = info[4][0]
                if ":" not in addr and not addr.startswith("127."):
                    ips.add(addr)
        except OSError:
            pass
        # UDP connect trick: no packets are sent, but the kernel picks the
        # interface address it would use for outbound traffic.
        for target in ("1.1.1.1", 53), ("8.8.8.8", 53):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(0.2)
                sock.connect(target)
                ips.add(sock.getsockname()[0])
                sock.close()
            except OSError:
                pass
        return {ip for ip in ips if ip and not ip.startswith("127.")}

    def detect_main_server(self) -> Optional[dict]:
        """Resolve the server this process runs on.

        Order: explicit MAIN_SERVER_ID -> MAIN_SERVER_NAME / hostname -> local
        interface IP match -> the only server in the project.
        """
        if self.config.main_server_id:
            server = self.get_server(self.config.main_server_id)
            if server is None:
                raise HCloudError(
                    f"MAIN_SERVER_ID={self.config.main_server_id} not found in project",
                    status=404,
                    code="main_server_not_found",
                    classification=PERMANENT,
                )
            self.main_server_id = int(server["id"])
            return server

        if not self.config.auto_detect_main_server:
            raise HCloudError(
                "MAIN_SERVER_ID is not set and AUTO_DETECT_MAIN_SERVER=false",
                code="main_server_unresolved",
                classification=PERMANENT,
            )

        # Metadata service: instance-id is the server id on Hetzner Cloud.
        meta = self._metadata()
        if meta:
            try:
                meta_id = int(str(meta.get("instance-id", "")).strip())
            except (TypeError, ValueError):
                meta_id = 0
            if meta_id:
                candidate = self.get_server(meta_id)
                if candidate is not None:
                    self.main_server_id = meta_id
                    return candidate

        servers = self.list_servers(self.config.label_selector)
        if not servers:
            return None
        if meta.get("public-ipv4"):
            for server in servers:
                ipv4 = ((server.get("public_net") or {}).get("ipv4") or {}).get("ip")
                if ipv4 == meta["public-ipv4"]:
                    self.main_server_id = int(server["id"])
                    return server
        wanted_names = []
        if self.config.main_server_name:
            wanted_names.append(self.config.main_server_name.lower())
        if meta.get("hostname"):
            wanted_names.append(str(meta["hostname"]).lower())
        try:
            host = socket.gethostname().lower()
            wanted_names.extend([host, host.split(".")[0]])
        except OSError:
            pass
        wanted_names = [n for n in dict.fromkeys(wanted_names) if n]

        if wanted_names:
            for server in servers:
                name = str(server.get("name", "")).lower()
                short = name.split(".")[0]
                if name in wanted_names or short in wanted_names:
                    self.main_server_id = int(server["id"])
                    return server

        local_ips = self._local_ip_candidates()
        if local_ips:
            for server in servers:
                ipv4 = ((server.get("public_net") or {}).get("ipv4") or {}).get("ip")
                if ipv4 and ipv4 in local_ips:
                    self.main_server_id = int(server["id"])
                    return server
                for net in server.get("private_net") or []:
                    if net.get("ip") in local_ips:
                        self.main_server_id = int(server["id"])
                        return server

        if len(servers) == 1:
            self.main_server_id = int(servers[0]["id"])
            return servers[0]

        names = ", ".join(f"{s.get('name')}({s.get('id')})" for s in servers[:8])
        raise HCloudError(
            "cannot determine which server is the main server; set MAIN_SERVER_ID. "
            f"Candidates: {names}",
            code="main_server_unresolved",
            classification=PERMANENT,
        )

    def resolve_protected_ip_ids(
        self, server: Optional[dict], primary_ips: Optional[list[dict]] = None
    ) -> list[int]:
        """Resolve the set of Primary IP IDs that must never be deleted.

        Explicit configuration is authoritative:

        * ``MAIN_PRIMARY_IP_IDS`` / ``MAIN_PRIMARY_IP_ID`` set the exact IDs.
        * ``MAIN_PRIMARY_IP`` (an address) is mapped to the matching Primary IP
          ID (IPv4, or IPv6 compared with and without the ``/64`` suffix).
        * When either of those is present, the main server's *other* Primary IPs
          are **not** silently added: a user who configured "reserve this one
          IPv4" must not end up reserving an IPv6 they wanted deleted.  The
          attached non-reserved IPs are handled by ``_prepare_main_ips``, which
          sets ``auto_delete=true`` so they die with the server.
        * When nothing explicit is configured, the main server's IPv4 is always
          reserved and its IPv6 is reserved too unless
          ``RESERVE_MAIN_IPV6=false``.
        * ``PROTECTED_IP_IDS`` / ``PROTECTED_IPS`` are always unioned in, so a
          typo can never cause a user-listed IP to be deleted.
        """
        ips = primary_ips if primary_ips is not None else self.list_primary_ips()

        explicit_ids = [int(i) for i in self.config.main_primary_ip_ids]
        explicit_addr = str(self.config.main_primary_ip or "").strip()
        explicit = bool(explicit_ids or explicit_addr)

        ids: list[int] = list(explicit_ids)

        if explicit_addr:
            wanted = explicit_addr.split("/")[0].lower()
            for ip in ips:
                raw = str(ip.get("ip") or "")
                if raw and (raw == explicit_addr or raw.split("/")[0].lower() == wanted):
                    ids.append(int(ip["id"]))
            if server:
                public_net = server.get("public_net") or {}
                for key in ("ipv4", "ipv6"):
                    net = public_net.get(key) or {}
                    value = str(net.get("ip") or "")
                    if value and (value == explicit_addr or value.split("/")[0].lower() == wanted):
                        if net.get("id"):
                            ids.append(int(net["id"]))

        if not explicit and server:
            public_net = server.get("public_net") or {}
            ipv4 = public_net.get("ipv4") or {}
            ipv6 = public_net.get("ipv6") or {}
            if ipv4.get("id"):
                ids.append(int(ipv4["id"]))
            if self.config.reserve_main_ipv6 and ipv6.get("id"):
                ids.append(int(ipv6["id"]))

        ids.extend(int(i) for i in self.config.protected_ip_ids)

        # Resolve explicitly protected addresses to IDs too, so a config that
        # names an IP instead of an ID still protects it.
        for ip in ips:
            raw = str(ip.get("ip") or "")
            if not raw:
                continue
            bare = raw.split("/")[0].lower()
            for wanted in self.config.protected_ips:
                wanted_bare = str(wanted).split("/")[0].lower()
                if raw == wanted or bare == wanted_bare:
                    ids.append(int(ip["id"]))
        return list(dict.fromkeys(ids))
