"""Unit-level tests for the teardown state machine, using an in-memory fake.

These complement ``test_http_integration.py``: same scenarios, but with a
deterministic in-process fake so the tricky invariants (ordering, retries,
recovery, abort) are asserted precisely rather than over HTTP.

Run with:  python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from datetime import timedelta

from deadman.config import Config
from deadman.engine import (
    PHASE_DONE,
    PHASE_MONITORING,
    PHASE_TEARDOWN,
    STEPS,
    Engine,
    _new_step,
)
from deadman.hetzner import HCloudError
from deadman.log import null_logger
from deadman.storage import StateStore, StorageError
from deadman.util import iso, parse_iso, utcnow


class FakeTelegram:
    enabled = False
    sent_count = 0

    def __init__(self, enabled: bool = False) -> None:
        self.enabled = enabled
        self.messages: list[str] = []

    def send(self, text="", *args, **kwargs):
        self.messages.append(str(text))
        return True

    def send_async(self, text="", *args, **kwargs):
        self.messages.append(str(text))
        return None

    def get_updates(self, *args, **kwargs):
        return []

    @staticmethod
    def parse_command(text, bot_username=""):
        text = (text or "").strip()
        parts = text.split(maxsplit=1)
        return (parts[0].lower() if parts and parts[0].startswith("/") else ""), (
            parts[1] if len(parts) > 1 else ""
        )


class FakeClient:
    """A tiny, deterministic Hetzner project with fault knobs."""

    def __init__(self) -> None:
        self.main_server_id = 1
        self.dry_run = False
        self.state = {}
        self.event_sink = None
        self.poll_hook = None
        self.logger = null_logger()
        self.calls: list[str] = []
        self.events: list[tuple] = []
        self.shutdown_calls: list[int] = []
        self.snapshot_seen_status = None
        self.fail_floating_delete = False
        self.fail_primary_delete = False
        self.fail_auto_delete = False
        self.keep_server_after_delete = False
        self.lb_targets: dict[str, list] = {}
        self.unassign_attempts = 0
        self.resolve_calls = 0
        self.delete_snapshots: list[dict] = []
        self.next_action = 100
        self.on_image_poll = None
        self.servers = {
            1: {
                "id": 1,
                "name": "main",
                "status": "running",
                "protection": {"delete": True, "rebuild": True},
                "public_net": {
                    "ipv4": {"id": 101, "ip": "203.0.113.10"},
                    "ipv6": {"id": 102, "ip": "2001:db8::/64"},
                },
            },
            2: {
                "id": 2,
                "name": "worker",
                "status": "running",
                "protection": {"delete": True, "rebuild": True},
                "public_net": {
                    "ipv4": {"id": 103, "ip": "203.0.113.11"},
                    "ipv6": None,
                },
            },
        }
        self.primary = {
            101: {
                "id": 101,
                "ip": "203.0.113.10",
                "type": "ipv4",
                "assignee_type": "server",
                "assignee_id": 1,
                "auto_delete": True,
                "protection": {"delete": True},
            },
            102: {
                "id": 102,
                "ip": "2001:db8::/64",
                "type": "ipv6",
                "assignee_type": "server",
                "assignee_id": 1,
                "auto_delete": False,
                "protection": {"delete": True},
            },
            103: {
                "id": 103,
                "ip": "203.0.113.11",
                "type": "ipv4",
                "assignee_type": "server",
                "assignee_id": 2,
                "auto_delete": False,
                "protection": {"delete": False},
            },
            104: {
                "id": 104,
                "ip": "203.0.113.12",
                "type": "ipv4",
                "assignee_type": "unassigned",
                "assignee_id": None,
                "auto_delete": False,
                "protection": {"delete": True},
            },
        }
        self.floating = {
            201: {
                "id": 201,
                "ip": "198.51.100.7",
                "server": 1,
                "protection": {"delete": True},
            }
        }
        self.images: dict[int, dict] = {}

    # ------------------------------------------------------------ plumbing
    def _action(self, command: str = "action") -> dict:
        self.next_action += 1
        return {"id": self.next_action, "status": "success", "command": command}

    def wait_action(self, action, timeout=None):
        return {"id": action.get("id") if isinstance(action, dict) else action, "status": "success"}

    def _call_poll_hook(self):
        if self.poll_hook is not None:
            self.poll_hook()
        return None

    # -------------------------------------------------------------- server
    def list_servers(self, label_selector=""):
        return [dict(s) for s in self.servers.values()]

    def get_server(self, server_id, allow_missing=True):
        s = self.servers.get(int(server_id))
        return dict(s) if s else None

    def change_server_protection(self, server_id, delete=False, rebuild=None):
        self.calls.append(f"server_protect:{server_id}:{delete}")
        self.events.append(("server_protection", int(server_id), bool(delete)))
        self.servers[int(server_id)]["protection"] = {"delete": delete, "rebuild": delete}
        return self._action("change_server_protection")

    def delete_server(self, server_id):
        sid = int(server_id)
        self.calls.append(f"server_delete:{sid}")
        server = self.servers.get(sid)
        if not server:
            return True
        attached = self._attached(sid)
        self.delete_snapshots.append(
            {
                "server_id": sid,
                "server_protection": dict(server.get("protection") or {}),
                "primary_ips": {
                    ip_id: {
                        "auto_delete": self.primary[ip_id]["auto_delete"],
                        "protection": dict(self.primary[ip_id]["protection"]),
                        "assignee_id": self.primary[ip_id]["assignee_id"],
                    }
                    for ip_id in attached
                },
            }
        )
        for ip_id in attached:
            ip = self.primary.get(ip_id)
            if ip and (ip.get("protection") or {}).get("delete"):
                raise HCloudError(
                    "server has a protected primary IP", status=422,
                    code="protected", classification="PERMANENT",
                )
        self.events.append(("server_delete", sid))
        if not self.keep_server_after_delete:
            del self.servers[sid]
        self._detach(sid)
        return True

    def _attached(self, server_id: int) -> list[int]:
        server = self.servers.get(server_id) or {}
        out = []
        for key in ("ipv4", "ipv6"):
            net = (server.get("public_net") or {}).get(key) or {}
            if net.get("id"):
                out.append(int(net["id"]))
        return out

    def _detach(self, server_id: int) -> None:
        for ip in list(self.primary.values()):
            if ip.get("assignee_id") == server_id:
                if ip.get("auto_delete"):
                    self.primary.pop(ip["id"], None)
                    self.events.append(("ip_auto_deleted", ip["id"]))
                else:
                    ip["assignee_id"] = None
                    ip["assignee_type"] = "unassigned"

    def ensure_off(self, server_id, allow_main=False):
        sid = int(server_id)
        if sid == self.main_server_id and not allow_main:
            raise HCloudError("refusing", code="refuse_poweroff_main")
        self.shutdown_calls.append(sid)
        if sid in self.servers:
            self.servers[sid]["status"] = "off"
        self.events.append(("power_off", sid))

    # ----------------------------------------------------------- primary ip
    def list_primary_ips(self, label_selector=""):
        return [dict(p) for p in self.primary.values()]

    def get_primary_ip(self, ip_id, allow_missing=True):
        p = self.primary.get(int(ip_id))
        return dict(p) if p else None

    def update_primary_ip(self, ip_id, auto_delete=None, name=None, labels=None):
        ip = self.primary[int(ip_id)]
        if auto_delete is not None:
            if auto_delete and self.fail_auto_delete:
                raise HCloudError("cannot enable auto_delete", status=422, code="invalid_input")
            if auto_delete and (ip.get("protection") or {}).get("delete"):
                raise HCloudError("cannot auto_delete protected ip", status=422, code="invalid_input")
            ip["auto_delete"] = bool(auto_delete)
            self.events.append(("ip_auto_delete", int(ip_id), bool(auto_delete)))
        return dict(ip)

    def change_primary_ip_protection(self, ip_id, delete):
        ip = self.primary[int(ip_id)]
        if delete and ip.get("auto_delete"):
            raise HCloudError(
                "protection requires auto_delete=false", status=422, code="invalid_input"
            )
        ip["protection"] = {"delete": bool(delete)}
        self.events.append(("ip_protection", int(ip_id), bool(delete)))
        return self._action("change_primary_ip_protection")

    def unassign_primary_ip(self, ip_id):
        self.unassign_attempts += 1
        ip = self.primary[int(ip_id)]
        assignee = ip.get("assignee_id")
        server = self.servers.get(assignee)
        if server and server.get("status") != "off":
            raise HCloudError("server not stopped", status=422, code="server_not_stopped")
        if assignee is not None and self._is_lb_target(int(assignee)):
            raise HCloudError(
                "server is a load balancer target", status=422,
                code="server_is_load_balancer_target",
            )
        ip["assignee_id"] = None
        ip["assignee_type"] = "unassigned"
        self.events.append(("ip_unassign", int(ip_id)))
        return self._action("unassign_primary_ip")

    def delete_primary_ip(self, ip_id):
        ip = self.primary[int(ip_id)]
        if self.fail_primary_delete:
            raise HCloudError("boom", status=500, code="unavailable")
        if ip.get("assignee_id") is not None:
            raise HCloudError("must be unassigned", status=422, code="must_be_unassigned")
        if (ip.get("protection") or {}).get("delete"):
            raise HCloudError("protected", status=422, code="protected")
        self.calls.append(f"primary_delete:{ip_id}")
        self.events.append(("ip_delete", int(ip_id)))
        del self.primary[int(ip_id)]
        return True

    # ---------------------------------------------------------- floating ip
    def list_floating_ips(self):
        return [dict(f) for f in self.floating.values()]

    def get_floating_ip(self, ip_id, allow_missing=True):
        f = self.floating.get(int(ip_id))
        return dict(f) if f else None

    def change_floating_ip_protection(self, ip_id, delete):
        self.floating[int(ip_id)]["protection"] = {"delete": delete}
        self.events.append(("fip_protection", int(ip_id), bool(delete)))
        return self._action("change_floating_ip_protection")

    def unassign_floating_ip(self, ip_id):
        self.floating[int(ip_id)]["server"] = None
        return self._action("unassign_floating_ip")

    def delete_floating_ip(self, ip_id):
        if self.fail_floating_delete:
            raise HCloudError("boom", status=500, code="unavailable")
        self.calls.append(f"floating_delete:{ip_id}")
        self.events.append(("fip_delete", int(ip_id)))
        self.floating.pop(int(ip_id), None)
        return True

    # -------------------------------------------------------------- images
    def create_snapshot(self, server_id, description, labels):
        self.snapshot_seen_status = self.servers[int(server_id)]["status"]
        self.calls.append(f"snapshot:{server_id}")
        self.events.append(("snapshot", int(server_id)))
        image = {
            "id": 500,
            "status": "creating",
            "type": "snapshot",
            "protection": {"delete": False},
            "image_size": 5.0,
            "disk_size": 40,
        }
        self.images[500] = image
        return dict(image), self._action("create_image")

    def get_image(self, image_id, allow_missing=True):
        img = self.images.get(int(image_id))
        return dict(img) if img else None

    def wait_image_available(self, image_id, timeout=None):
        # Two poll rounds so tests can inject an /abort mid-snapshot.
        for _ in range(2):
            self._call_poll_hook()
        self.images[int(image_id)]["status"] = "available"
        return dict(self.images[int(image_id)])

    def protect_image(self, image_id, delete=True):
        self.images[int(image_id)]["protection"] = {"delete": delete}
        self.events.append(("image_protection", int(image_id), bool(delete)))
        return self._action("change_image_protection")

    # ----------------------------------------------------------- load balancer
    def list_load_balancers(self):
        return [
            {"id": 77, "targets": list(targets)} for targets in self.lb_targets.values()
        ]

    def _is_lb_target(self, server_id: int) -> bool:
        server = self.servers.get(server_id) or {}
        addresses = set()
        for key in ("ipv4", "ipv6"):
            value = ((server.get("public_net") or {}).get(key) or {}).get("ip")
            if value:
                addresses.add(str(value))
                addresses.add(str(value).split("/")[0])
        for targets in self.lb_targets.values():
            for target in targets:
                if target.get("type") == "server" and (target.get("server") or {}).get("id") == server_id:
                    return True
                if target.get("type") == "ip" and (target.get("ip") or {}).get("ip") in addresses:
                    return True
        return False

    def remove_lb_targets_for_server(self, server_id):
        removed = 0
        server = self.servers.get(int(server_id)) or {}
        addresses = set()
        for key in ("ipv4", "ipv6"):
            value = ((server.get("public_net") or {}).get(key) or {}).get("ip")
            if value:
                addresses.add(str(value))
                addresses.add(str(value).split("/")[0])
        for lb_id in list(self.lb_targets):
            kept = []
            for target in self.lb_targets[lb_id]:
                if target.get("type") == "server" and (target.get("server") or {}).get("id") == int(server_id):
                    removed += 1
                    continue
                if target.get("type") == "ip" and (target.get("ip") or {}).get("ip") in addresses:
                    removed += 1
                    self.events.append(("lb_remove_ip_target", target["ip"]["ip"]))
                    continue
                kept.append(target)
            self.lb_targets[lb_id] = kept
        return removed

    # -------------------------------------------------------------- others
    def detect_main_server(self):
        return self.get_server(self.main_server_id)

    def resolve_protected_ip_ids(self, server, primary_ips=None):
        self.resolve_calls += 1
        ids = list(self.config_ids)
        if not ids and server:
            public_net = server.get("public_net") or {}
            ids = [int(net["id"]) for net in (public_net.get("ipv4"), public_net.get("ipv6")) if net and net.get("id")]
        return ids

    config_ids: list[int] = [101, 102]


class EngineTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="deadman-test-")
        self.cfg = Config.load(
            overrides={
                "token": "test-token",
                "state_dir": self.tmp,
                "main_server_id": 1,
                "main_primary_ip_ids": [101, 102],
                "log_file": os.path.join(self.tmp, "deadman.log"),
                "billing_mode": "none",
                "tick_seconds": 1,
                "action_poll_seconds": 0.01,
            },
            require_token=True,
        )
        self.store = StateStore(self.tmp, enable_block_file=True, logger=null_logger())
        self.addCleanup(self.store.close)
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.client = FakeClient()
        self.telegram = FakeTelegram()
        self.engine = Engine(
            self.cfg, store=self.store, client=self.client,
            telegram=self.telegram, logger=null_logger(),
        )
        self.engine.state = self.engine.new_state()
        st = self.engine.state
        st["phase"] = PHASE_TEARDOWN
        st["main_server_id"] = 1
        st["main_server_name"] = "main"
        st["protected_ip_ids"] = [101, 102]
        st["protected_ips"] = ["203.0.113.10", "2001:db8::/64"]
        st["targets_resolved"] = True
        st["steps"] = [_new_step(name, utcnow()) for name in STEPS]
        st["step_index"] = 0
        self.engine._dirty = True

    def drive(self) -> None:
        for _ in range(40):
            if self.engine.state.get("phase") != PHASE_TEARDOWN:
                return
            self.engine._teardown_tick()
        self.fail(
            f"did not finish: {[(s['name'], s['status'], s.get('last_error')) for s in self.engine.state['steps']]}"
        )


# --------------------------------------------------------------------------- #
# Core flow
# --------------------------------------------------------------------------- #
class TestCriticalFlow(EngineTestBase):
    def test_full_teardown_preserves_main_ip_and_never_powers_off_main(self):
        self.drive()
        st = self.engine.state
        self.assertEqual(st["phase"], PHASE_DONE)
        self.assertTrue(st["main_server_deleted"])

        self.assertNotIn(1, self.client.servers)
        self.assertNotIn(2, self.client.servers)

        for ip_id in (101, 102):
            ip = self.client.primary[ip_id]
            self.assertIsNone(ip["assignee_id"])
            self.assertFalse(ip["auto_delete"])
            self.assertTrue(ip["protection"]["delete"])

        self.assertEqual(self.client.snapshot_seen_status, "running")
        self.assertIn("snapshot:1", self.client.calls)
        self.assertTrue(self.client.images[500]["protection"]["delete"])
        self.assertEqual(self.client.shutdown_calls, [])

        self.assertNotIn(103, self.client.primary)
        self.assertNotIn(104, self.client.primary)
        self.assertEqual(self.client.floating, {})

    def test_prepare_disables_protection_only_while_attached(self):
        details = self.engine._prepare_main_ips(for_deletion=True)
        self.assertFalse(self.client.primary[101]["auto_delete"])
        self.assertFalse(self.client.primary[101]["protection"]["delete"])
        self.assertFalse(self.client.primary[102]["protection"]["delete"])
        self.assertTrue(details["prepared"])
        self.assertEqual(self.client.servers[1]["status"], "running")

    def test_snapshot_is_delete_protected(self):
        self.engine._step_snapshot_main_server({})
        snap = self.engine.state["snapshot"]
        self.assertEqual(snap["status"], "available")
        self.assertEqual(snap["image_id"], 500)
        self.assertTrue(self.client.images[500]["protection"]["delete"])

    def test_other_primary_ip_skip_reserved_and_main_attached(self):
        detail = self.engine._step_delete_other_primary_ips({})
        self.assertIn(101, detail["skipped_attached_to_main"])
        self.assertNotIn(103, self.client.primary)
        self.assertIn(104, detail["deleted"])
        self.assertIn(101, self.client.primary)
        self.assertIn(102, self.client.primary)

    def test_snapshot_gate_keeps_main_server(self):
        self.cfg.require_snapshot_before_main_delete = True
        step = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_MAIN_SERVER"][0]
        self.engine._execute_step(step)
        self.assertEqual(step["status"], "skipped")
        self.assertIn(1, self.client.servers)
        self.assertFalse(self.engine.state.get("main_server_deleted"))

    def test_dry_run_prepare_does_not_require_real_mutation(self):
        class DryFake(FakeClient):
            def update_primary_ip(self, ip_id, auto_delete=None, name=None, labels=None):
                return dict(self.primary[int(ip_id)])

            def change_primary_ip_protection(self, ip_id, delete):
                return self._action()

        self.engine.client = DryFake()
        self.engine.client.dry_run = True
        details = self.engine._prepare_main_ips(for_deletion=True)
        ids = [d["id"] for d in details["prepared"]]
        self.assertEqual(ids, [101, 102])
        for record in details["prepared"]:
            self.assertFalse(record["auto_delete_after"])


class TestStepOrdering(EngineTestBase):
    def test_steps_are_in_the_safe_order(self):
        self.assertEqual(
            list(STEPS),
            [
                "NOTIFY_TEARDOWN_START",
                "DELETE_FLOATING_IPS",
                "DELETE_OTHER_SERVERS",
                "DELETE_OTHER_PRIMARY_IPS",
                "SNAPSHOT_MAIN_SERVER",
                "SETTLE_BILLING",
                "PREPARE_MAIN_PRIMARY_IP",
                "DELETE_MAIN_SERVER",
            ],
        )

    def test_snapshot_precedes_any_protection_change(self):
        self.drive()
        events = self.client.events
        snapshot_at = next(i for i, e in enumerate(events) if e[0] == "snapshot")
        # Only the MAIN resources matter here: other servers' protection is
        # legitimately cleared earlier in the flow.
        first_lowered = next(
            i
            for i, e in enumerate(events)
            if (
                (e[0] == "ip_protection" and e[1] in (101, 102) and e[-1] is False)
                or (e[0] == "server_protection" and e[1] == 1 and e[-1] is False)
            )
        )
        self.assertLess(snapshot_at, first_lowered)

    def test_main_ip_protection_window_is_tiny(self):
        self.drive()
        events = self.client.events
        lowered = next(
            i for i, e in enumerate(events) if e == ("ip_protection", 101, False)
        )
        server_delete = next(i for i, e in enumerate(events) if e == ("server_delete", 1))
        restored = next(
            i for i, e in enumerate(events) if e == ("ip_protection", 101, True) and i > server_delete
        )
        self.assertLess(lowered, server_delete)
        self.assertLess(server_delete, restored)
        # Only the re-assertion of PREPARE and the DELETE itself sit between.
        self.assertLess(server_delete - lowered, 20)


# --------------------------------------------------------------------------- #
# Scenario 3 — maximum protection
# --------------------------------------------------------------------------- #
class TestAllProtected(EngineTestBase):
    def test_every_protection_cleared_before_its_delete(self):
        self.client.primary[103]["protection"] = {"delete": True}
        self.client.primary[104]["protection"] = {"delete": True}
        self.drive()
        events = self.client.events

        def first(*needle):
            return next(i for i, e in enumerate(events) if e == needle)

        self.assertLess(first("fip_protection", 201, False), first("fip_delete", 201))
        self.assertLess(first("server_protection", 2, False), first("server_delete", 2))
        self.assertLess(first("ip_protection", 104, False), first("ip_delete", 104))
        self.assertLess(first("server_protection", 1, False), first("server_delete", 1))
        self.assertLess(first("ip_protection", 101, False), first("server_delete", 1))
        self.assertLess(first("ip_protection", 102, False), first("server_delete", 1))
        self.assertLess(first("server_delete", 1), first("ip_protection", 101, True))

        deleted = [e[1] for e in events if e[0] == "ip_delete"]
        self.assertNotIn(101, deleted)
        self.assertNotIn(102, deleted)
        for ip_id in (101, 102):
            self.assertIsNone(self.client.primary[ip_id]["assignee_id"])
            self.assertTrue(self.client.primary[ip_id]["protection"]["delete"])

    def test_protected_attached_ip_would_block_delete_without_prepare(self):
        """T2: DELETE is refused while the attached IP stays protected."""
        with self.assertRaises(HCloudError) as ctx:
            self.client.delete_server(1)
        self.assertEqual(ctx.exception.status, 422)


# --------------------------------------------------------------------------- #
# Bug 1 — delete ordering
# --------------------------------------------------------------------------- #
class TestBug1DeleteOrdering(EngineTestBase):
    def test_done_and_reprotected_even_if_the_server_never_404s(self):
        self.client.keep_server_after_delete = True
        step = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_MAIN_SERVER"][0]
        self.engine._execute_step(step)

        self.assertEqual(step["status"], "succeeded", step.get("last_error"))
        st = self.engine.state
        self.assertEqual(st["phase"], PHASE_DONE)
        self.assertTrue(st["main_server_deleted"])
        self.assertIsNotNone(st["main_delete_issued_at"])
        # The IPs were detached by the delete and immediately re-protected,
        # even though GET /servers/1 still answers.
        for ip_id in (101, 102):
            self.assertIsNone(self.client.primary[ip_id]["assignee_id"])
            self.assertTrue(self.client.primary[ip_id]["protection"]["delete"])

    def test_state_is_persisted_before_any_post_delete_polling(self):
        self.client.keep_server_after_delete = True
        step = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_MAIN_SERVER"][0]
        self.engine._execute_step(step)
        with open(os.path.join(self.tmp, "state.json")) as fh:
            saved = json.load(fh)
        self.assertEqual(saved["phase"], "DONE")
        self.assertTrue(saved["main_server_deleted"])
        self.assertIsNotNone(saved["main_delete_issued_at"])

    def test_failed_delete_reverts_state(self):
        class Boom(FakeClient):
            def delete_server(self, server_id):
                raise HCloudError("boom", status=500, classification="TRANSIENT")

        self.engine.client = Boom()
        self.engine.client.state = self.engine.state
        step = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_MAIN_SERVER"][0]
        self.engine._execute_step(step)
        self.assertEqual(step["status"], "failed_retryable")
        self.assertEqual(self.engine.state["phase"], PHASE_TEARDOWN)
        self.assertFalse(self.engine.state["main_server_deleted"])
        self.assertIsNone(self.engine.state["main_delete_issued_at"])

    def test_404_on_delete_is_success(self):
        self.client.servers.pop(1)
        for ip_id in (101, 102):
            self.client.primary[ip_id]["assignee_id"] = None
        step = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_MAIN_SERVER"][0]
        self.engine._execute_step(step)
        self.assertEqual(step["status"], "succeeded")
        self.assertTrue(self.engine.state["main_server_deleted"])
        self.assertTrue(self.client.primary[101]["protection"]["delete"])


# --------------------------------------------------------------------------- #
# Bug 2 — partially configured targets
# --------------------------------------------------------------------------- #
class TestBug2PartialTargets(EngineTestBase):
    def _fresh_engine(self, **cfg_overrides):
        cfg = Config.load(
            overrides={
                "token": "test-token",
                "state_dir": self.tmp,
                "main_server_id": 1,
                "log_file": os.path.join(self.tmp, "deadman.log"),
                "billing_mode": "none",
                **cfg_overrides,
            },
            require_token=True,
        )
        store = StateStore(self.tmp, enable_block_file=False, logger=null_logger())
        self.addCleanup(store.close)
        engine = Engine(cfg, store=store, client=self.client, telegram=self.telegram, logger=null_logger())
        engine.state = engine.new_state()
        return engine

    def test_no_explicit_ids_still_resolves_server_ips(self):
        self.client.config_ids = []
        engine = self._fresh_engine()
        engine._ensure_targets()
        self.assertEqual(engine.state["protected_ip_ids"], [101, 102])
        self.assertEqual(engine.state["protected_ips"], ["203.0.113.10", "2001:db8::/64"])
        self.assertTrue(engine.state["targets_resolved"])
        self.assertEqual(engine.state["inventory"]["main_server_id"], 1)

    def test_partial_explicit_id_is_unioned_with_existing_state(self):
        self.client.config_ids = [101]
        engine = self._fresh_engine()
        engine.state["protected_ip_ids"] = [102]
        engine._ensure_targets()
        self.assertEqual(engine.state["protected_ip_ids"], [102, 101])

    def test_resolution_is_cached_in_state(self):
        engine = self._fresh_engine()
        engine._ensure_targets()
        calls = self.client.resolve_calls
        engine._ensure_targets()
        self.assertEqual(self.client.resolve_calls, calls)
        self.assertTrue(engine.state["targets_resolved"])

    def test_empty_set_refuses(self):
        self.client.config_ids = []
        self.client.servers[1]["public_net"] = {"ipv4": None, "ipv6": None}
        engine = self._fresh_engine()
        with self.assertRaises(HCloudError) as ctx:
            engine._ensure_targets()
        self.assertEqual(ctx.exception.code, "preflight_no_protected_ip")


# --------------------------------------------------------------------------- #
# Bug 3 — reserve only the main IPv4
# --------------------------------------------------------------------------- #
class TestBug3ReserveIpv4Only(EngineTestBase):
    def test_non_reserved_attached_ipv6_is_auto_deleted_with_the_server(self):
        self.engine.state["protected_ip_ids"] = [101]
        self.engine.state["protected_ips"] = ["203.0.113.10"]
        self.client.config_ids = [101]

        details = self.engine._prepare_main_ips(for_deletion=True)
        self.assertEqual(details["non_reserved_main_ips"], [102])
        self.assertTrue(self.client.primary[102]["auto_delete"])
        self.assertFalse(self.client.primary[102]["protection"]["delete"])

        step = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_MAIN_SERVER"][0]
        self.engine._execute_step(step)
        self.assertIn(101, self.client.primary)
        self.assertNotIn(102, self.client.primary)
        self.assertTrue(self.client.primary[101]["protection"]["delete"])
        self.assertIsNone(self.client.primary[101]["assignee_id"])

    def test_reserved_ipv6_is_not_auto_deleted(self):
        details = self.engine._prepare_main_ips(for_deletion=True)
        self.assertNotIn(102, details.get("non_reserved_main_ips", []))
        self.assertFalse(self.client.primary[102]["auto_delete"])


# --------------------------------------------------------------------------- #
# Bug 5 / 6 — other servers and Primary IPs
# --------------------------------------------------------------------------- #
class TestBug5AutoDeleteFlip(EngineTestBase):
    def test_delete_succeeds_when_auto_delete_cannot_be_enabled(self):
        self.client.fail_auto_delete = True
        step = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_OTHER_SERVERS"][0]
        self.engine._execute_step(step)
        self.assertEqual(step["status"], "succeeded", step.get("last_error"))
        self.assertNotIn(2, self.client.servers)
        self.assertIn(103, self.client.primary)

        step4 = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_OTHER_PRIMARY_IPS"][0]
        self.engine._execute_step(step4)
        self.assertEqual(step4["status"], "succeeded", step4.get("last_error"))
        self.assertNotIn(103, self.client.primary)


class TestBug6LoadBalancerTargets(EngineTestBase):
    def test_ip_target_is_removed_before_unassign(self):
        self.client.lb_targets["lb"] = [{"type": "ip", "ip": {"ip": "203.0.113.11"}}]
        step = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_OTHER_PRIMARY_IPS"][0]
        self.engine._execute_step(step)
        self.assertEqual(step["status"], "succeeded", step.get("last_error"))
        self.assertIn(("lb_remove_ip_target", "203.0.113.11"), self.client.events)
        self.assertNotIn(103, self.client.primary)
        self.assertEqual(self.client.shutdown_calls, [2])

    def test_server_target_is_removed(self):
        self.client.lb_targets["lb"] = [{"type": "server", "server": {"id": 2}}]
        step = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_OTHER_PRIMARY_IPS"][0]
        self.engine._execute_step(step)
        self.assertEqual(step["status"], "succeeded", step.get("last_error"))
        self.assertNotIn(103, self.client.primary)


# --------------------------------------------------------------------------- #
# Scenario 4 — retries
# --------------------------------------------------------------------------- #
class TestRetryAndRecovery(EngineTestBase):
    def test_retry_then_skip_advances(self):
        self.client.fail_floating_delete = True
        self.engine._teardown_tick()
        step = self.engine.state["steps"][1]
        self.assertEqual(step["name"], "DELETE_FLOATING_IPS")
        self.assertEqual(step["status"], "failed_retryable")
        self.assertIsNotNone(step["next_attempt_at"])
        delay = (parse_iso(step["next_attempt_at"]) - utcnow()).total_seconds()
        self.assertGreater(delay, 3500)
        self.assertLess(delay, 3700)

        step["first_error_at"] = iso(utcnow() - timedelta(hours=7))
        step["next_attempt_at"] = iso(utcnow() - timedelta(seconds=1))
        self.engine._teardown_tick()
        self.assertIn(step["status"], ("skipped", "abandoned"))
        self.assertEqual(self.engine.state["phase"], PHASE_DONE)

    def test_retry_schedule_survives_restart(self):
        self.client.fail_floating_delete = True
        self.engine._teardown_tick()
        scheduled = self.engine.state["steps"][1]["next_attempt_at"]
        self.engine._save()

        store = StateStore(self.tmp, enable_block_file=True, logger=null_logger())
        self.addCleanup(store.close)
        restarted = Engine(self.cfg, store=store, client=self.client, telegram=self.telegram, logger=null_logger())
        state = restarted.load_state()
        self.assertEqual(state["steps"][1]["status"], "failed_retryable")
        self.assertEqual(state["steps"][1]["next_attempt_at"], scheduled)
        self.assertEqual(state["phase"], PHASE_TEARDOWN)

    def test_max_retry_hours_zero_skips_immediately(self):
        self.cfg.max_retry_hours = 0
        self.client.fail_floating_delete = True
        self.drive()
        step = self.engine.state["steps"][1]
        self.assertEqual(step["status"], "skipped")
        self.assertEqual(self.engine.state["phase"], PHASE_DONE)

    def test_primary_ip_delete_failure_is_retryable(self):
        self.client.fail_primary_delete = True
        step = [s for s in self.engine.state["steps"] if s["name"] == "DELETE_OTHER_PRIMARY_IPS"][0]
        self.engine._execute_step(step)
        self.assertEqual(step["status"], "failed_retryable")
        self.assertEqual(step["last_error"]["code"], "primary_ip_delete_incomplete")

    def test_block_file_recovers_when_json_is_corrupt(self):
        st = self.engine.state
        st["phase"] = PHASE_MONITORING
        st["marker"] = "from-block"
        self.store.save(st)
        generation = st["generation"]

        with open(os.path.join(self.tmp, "state.json"), "w") as fh:
            fh.write("{ this is not json")
        with open(os.path.join(self.tmp, "state.json.bak"), "w") as fh:
            fh.write("also broken")

        recovered = self.store.load()
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered["marker"], "from-block")
        self.assertGreaterEqual(recovered.get("generation", 0), generation)

    def test_generation_increases_and_both_slots_used(self):
        self.store.save({"phase": "MONITORING", "generation": 0})
        self.store.save({"phase": "MONITORING", "generation": 0})
        report = self.store.block.verify()
        slots = [s for s in report["slots"] if s.get("valid")]
        self.assertEqual(len(slots), 2)
        self.assertNotEqual(slots[0]["slot"], slots[1]["slot"])

    def test_running_step_is_reverified(self):
        self.engine.state["steps"][2]["status"] = "running"
        self.engine.state["step_index"] = 2
        self.engine._save()
        store = StateStore(self.tmp, enable_block_file=True, logger=null_logger())
        self.addCleanup(store.close)
        restarted = Engine(self.cfg, store=store, client=self.client, telegram=self.telegram, logger=null_logger())
        state = restarted.load_state()
        self.assertEqual(state["steps"][2]["status"], "failed_retryable")
        self.assertEqual(state["steps"][2]["last_error"]["code"], "process_restarted")

    def test_clock_skew_clamps_retry_times(self):
        self.engine.state["steps"][1]["status"] = "failed_retryable"
        self.engine.state["steps"][1]["next_attempt_at"] = iso(utcnow() + timedelta(hours=3))
        self.engine.state["last_tick"] = iso(utcnow() + timedelta(hours=4))
        self.engine._save()
        store = StateStore(self.tmp, enable_block_file=True, logger=null_logger())
        self.addCleanup(store.close)
        restarted = Engine(self.cfg, store=store, client=self.client, telegram=self.telegram, logger=null_logger())
        state = restarted.load_state()
        self.assertLessEqual(
            parse_iso(state["steps"][1]["next_attempt_at"]), utcnow() + timedelta(seconds=5)
        )

    def test_corrupt_artefacts_refuse_to_start_fresh(self):
        with open(os.path.join(self.tmp, "state.json"), "w") as fh:
            fh.write("{ broken")
        with self.assertRaises(StorageError):
            self.engine.load_state()

    def test_flock_excludes_a_second_daemon(self):
        other = StateStore(self.tmp, enable_block_file=False, logger=null_logger())
        self.addCleanup(other.close)
        self.assertTrue(self.store.acquire_lock())
        self.assertFalse(other.acquire_lock())


# --------------------------------------------------------------------------- #
# Scenario 6 — abort
# --------------------------------------------------------------------------- #
class TestAbort(EngineTestBase):
    def test_abort_during_snapshot_poll(self):
        def _abort() -> None:
            self.engine._handle_message("/abort", {})

        self.client.poll_hook = _abort
        step = [s for s in self.engine.state["steps"] if s["name"] == "SNAPSHOT_MAIN_SERVER"][0]
        self.engine.state["step_index"] = STEPS.index("SNAPSHOT_MAIN_SERVER")
        self.engine._teardown_tick()

        st = self.engine.state
        self.assertEqual(st["phase"], PHASE_MONITORING)
        self.assertIn(1, self.client.servers)
        self.assertTrue(self.client.primary[101]["protection"]["delete"])
        self.assertTrue(self.client.primary[102]["protection"]["delete"])
        remaining = (parse_iso(st["deadline_at"]) - utcnow()).total_seconds()
        self.assertGreater(remaining, 6.9 * 86400)
        self.assertTrue(any("ABORTED" in m for m in self.telegram.messages))

    def test_any_message_during_teardown_aborts(self):
        self.engine.state["step_index"] = 2
        self.engine._handle_message("I am back, stop", {})
        self.assertEqual(self.engine.state["phase"], PHASE_MONITORING)
        self.assertIn(1, self.client.servers)

    def test_abort_after_delete_is_too_late(self):
        self.telegram.enabled = True
        self.engine.state["main_delete_issued_at"] = iso()
        self.engine._handle_message("/abort", {})
        self.assertEqual(self.engine.state["phase"], PHASE_TEARDOWN)
        self.assertTrue(any("Too late" in m for m in self.telegram.messages))

    def test_abort_restores_protection_after_prepare(self):
        # PREPARE has already lowered protection; an abort must restore it.
        self.engine._prepare_main_ips(for_deletion=True)
        self.assertFalse(self.client.primary[101]["protection"]["delete"])
        self.engine._abort_teardown("operator returned")
        self.assertTrue(self.client.primary[101]["protection"]["delete"])
        self.assertTrue(self.client.primary[102]["protection"]["delete"])
        self.assertEqual(self.engine.state["phase"], PHASE_MONITORING)

    def test_abort_does_not_auto_delete_non_reserved_ips(self):
        self.engine.state["protected_ip_ids"] = [101]
        self.engine._abort_teardown("operator returned")
        self.assertFalse(self.client.primary[102]["auto_delete"])


# --------------------------------------------------------------------------- #
# Monitoring
# --------------------------------------------------------------------------- #
class TestMonitoring(EngineTestBase):
    def setUp(self):
        super().setUp()
        self.telegram.enabled = True

    def test_deadline_requires_two_ticks_then_teardown(self):
        st = self.engine.state
        st["phase"] = PHASE_MONITORING
        st["steps"] = []
        st["last_confirmed_online_at"] = iso(utcnow() - timedelta(days=8))
        st["deadline_at"] = iso(utcnow() - timedelta(seconds=5))
        self.engine._monitoring_tick()
        self.assertEqual(st["phase"], PHASE_MONITORING)
        self.assertIsNotNone(st.get("deadline_seen_at"))
        self.engine._monitoring_tick()
        self.assertEqual(st["phase"], PHASE_TEARDOWN)
        self.assertEqual([s["name"] for s in st["steps"]], list(STEPS))

    def test_checkin_message_is_sent_and_any_text_resets(self):
        st = self.engine.state
        st["phase"] = PHASE_MONITORING
        st["last_checkin_sent_at"] = None
        self.engine._monitoring_tick()
        self.assertTrue(any("check-in" in m for m in self.telegram.messages))

        st["deadline_at"] = iso(utcnow() - timedelta(seconds=10))
        self.engine._handle_message("still here", {})
        remaining = (parse_iso(st["deadline_at"]) - utcnow()).total_seconds()
        self.assertGreater(remaining, 6.9 * 86400)
        self.assertTrue(any("Check-in received" in m for m in self.telegram.messages))

    def test_status_command_does_not_crash(self):
        self.engine.state["phase"] = PHASE_MONITORING
        self.engine._handle_message("/status", {})
        self.assertTrue(any("hetzner-deadman status" in m for m in self.telegram.messages))


class TestConfig(unittest.TestCase):
    def test_grace_period_alias(self):
        os.environ["DEADMAN_DAYS"] = "3"
        try:
            cfg = Config.load(overrides={"token": "x"}, require_token=False)
            self.assertEqual(cfg.grace_period_days, 3.0)
        finally:
            del os.environ["DEADMAN_DAYS"]

    def test_state_json_roundtrip(self):
        tmp = tempfile.mkdtemp(prefix="deadman-config-")
        try:
            cfg = Config.load(overrides={"token": "x", "state_dir": tmp})
            self.assertEqual(cfg.billing_mode, "none")
            self.assertTrue(cfg.state_dir.endswith(tmp))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
