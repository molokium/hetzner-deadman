"""End-to-end QA over a **real HTTP stack** against local API mocks.

Nothing here uses ``FakeClient``: every test drives ``HCloudClient``,
``TelegramClient`` and ``BillingStep`` through ``urllib`` to a real
``ThreadingHTTPServer`` speaking Hetzner Cloud v1 / Telegram Bot API, with
status codes, async actions, protection rules and fault injection.

Scenarios covered (manager QA brief):
 1. daily check-in + operator reply
 2. full self-destruction with every resource and every hook
 3. maximum protection everywhere
 4. API outage / 500 / 422 / timeout / malformed JSON / action error + retry,
    persistence across restart, skip after the retry budget
 5. crash / container-restart recovery at every persistence layer
 6. operator regains connectivity mid-teardown and aborts during a snapshot
plus the eight regressions found in review (bugs 1-7; bug 8 is docs).

Run with:  python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import tempfile
import threading
import time
import unittest
from datetime import timedelta

from deadman.config import Config, ConfigError
from deadman.engine import PHASE_DONE, PHASE_MONITORING, PHASE_TEARDOWN, Engine, STEPS, _new_step
from deadman.hetzner import HCloudError
from deadman.log import null_logger
from deadman.storage import StateStore
from deadman.util import iso, parse_iso, utcnow

from tests.http_mock import BillingMock, Fault, HetznerMock, TelegramMock


# --------------------------------------------------------------------------- #
# harness
# --------------------------------------------------------------------------- #
class HttpHarness(unittest.TestCase):
    """Spins up the three mock APIs and a temp state directory."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="deadman-http-")
        self.hetzner = HetznerMock()
        self.telegram = TelegramMock()
        self.billing = BillingMock()
        self.addCleanup(self._teardown_mocks)

        self.hetzner.add_primary_ip(101, "203.0.113.10", "ipv4", assignee_id=1, auto_delete=True, protected=True)
        self.hetzner.add_primary_ip(102, "2001:db8::/64", "ipv6", assignee_id=1, auto_delete=False, protected=True)
        self.hetzner.add_primary_ip(103, "203.0.113.11", "ipv4", assignee_id=2, auto_delete=False, protected=False)
        self.hetzner.add_primary_ip(104, "203.0.113.12", "ipv4", assignee_id=None, auto_delete=False, protected=True)
        self.hetzner.add_server(1, "main", ipv4_id=101, ipv6_id=102, protected=True)
        self.hetzner.add_server(2, "worker", ipv4_id=103, protected=True)
        self.hetzner.add_floating_ip(201, "198.51.100.7", server=1, protected=True)
        self.hetzner.add_image(900, "pre-existing-snapshot", protected=True)

    def _teardown_mocks(self) -> None:
        for mock in (self.hetzner, self.telegram, self.billing):
            try:
                mock.close()
            except Exception:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------- factories
    def make_config(self, **overrides):
        base = {
            "token": "test-token",
            "base_url": self.hetzner.base_url + "/v1",
            "state_dir": self.tmp,
            "log_file": os.path.join(self.tmp, "deadman.log"),
            "main_server_id": 1,
            "main_primary_ip_ids": [101, 102],
            "telegram_bot_token": "123456:TEST-TOKEN",
            "telegram_chat_id": self.telegram.chat_id,
            "telegram_user_id": self.telegram.user_id,
            "telegram_api_base": self.telegram.base_url,
            "telegram_poll_timeout": 0,
            "tick_seconds": 0,
            "action_poll_seconds": 0.02,
            "action_timeout_seconds": 5.0,
            "image_timeout_seconds": 10.0,
            "shutdown_grace_seconds": 3.0,
            "poweroff_timeout_seconds": 3.0,
            "request_timeout_seconds": 2.0,
            "retry_interval_seconds": 3600,
            "billing_mode": "none",
            "enable_block_file": True,
        }
        base.update(overrides)
        return Config.load(overrides=base, require_token=True)

    def make_engine(self, config=None, store=None, start_state=True):
        cfg = config or self.make_config()
        if store is None:
            store = StateStore(
                cfg.state_dir,
                block_size_bytes=cfg.block_file_bytes,
                enable_block_file=cfg.enable_block_file,
                logger=null_logger(),
            )
            self.addCleanup(store.close)
        engine = Engine(cfg, store=store, logger=null_logger())
        if start_state:
            engine.state = engine.load_state()
        return engine

    # --------------------------------------------------------------- driving
    def start_teardown(self, engine) -> None:
        engine.state = engine.load_state() if not engine.state else engine.state
        engine._begin_teardown("qa test")
        self.assertEqual(engine.state["phase"], PHASE_TEARDOWN)

    def drive_to_completion(self, engine, max_ticks: int = 60, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        for _ in range(max_ticks):
            if engine.state.get("phase") != PHASE_TEARDOWN:
                return
            engine._teardown_tick()
            if engine.state.get("phase") != PHASE_TEARDOWN:
                return
            if time.time() > deadline:
                break
            time.sleep(0.02)
        self.fail(
            f"teardown did not finish: phase={engine.state.get('phase')} "
            f"step_index={engine.state.get('step_index')} "
            f"steps={[(s['name'], s['status'], s.get('last_error')) for s in engine.state.get('steps', [])]}"
        )

    def assert_protection_cleared_before(self, kind: str, ident: int) -> None:
        """Assert a ``change_*_protection {delete:false}`` preceded the delete."""
        cleared_at = None
        deleted_at = None
        for index, event in enumerate(self.hetzner.actions_performed):
            if event.get("delete") is False:
                if kind == "server" and event.get("command") == "change_server_protection" and event.get("server_id") == ident:
                    cleared_at = index
                if kind == "primary" and event.get("command") == "change_primary_ip_protection" and event.get("primary_ip_id") == ident:
                    cleared_at = index
                if kind == "floating" and event.get("command") == "change_floating_ip_protection" and event.get("floating_ip_id") == ident:
                    cleared_at = index
            if kind == "server" and event.get("command") == "delete_server" and event.get("server_id") == ident:
                deleted_at = index
            if kind == "primary" and event.get("command") == "delete_primary_ip" and event.get("primary_ip_id") == ident:
                deleted_at = index
            if kind == "floating" and event.get("command") == "delete_floating_ip" and event.get("floating_ip_id") == ident:
                deleted_at = index
        self.assertIsNotNone(cleared_at, f"no protection was cleared for {kind} {ident}")
        self.assertIsNotNone(deleted_at, f"{kind} {ident} was never deleted")
        self.assertLess(cleared_at, deleted_at, f"{kind} {ident} protection cleared too late")

    @staticmethod
    def free_port() -> int:
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        return port


# --------------------------------------------------------------------------- #
# Scenario 1 — daily check-in and reply
# --------------------------------------------------------------------------- #
class TestScenario1Checkin(HttpHarness):
    def test_daily_checkin_is_sent_and_reply_resets_timer(self):
        engine = self.make_engine()
        self.assertEqual(engine.state["phase"], PHASE_MONITORING)

        engine._monitoring_tick()
        checkins = self.telegram.sent_containing("check-in")
        self.assertTrue(checkins, f"no check-in sent; messages={self.telegram.texts()}")
        self.assertIn("Deadline:", checkins[0])

        # The user replies with free text over the real Telegram HTTP API.
        engine.state["deadline_at"] = iso(utcnow() - timedelta(hours=1))
        engine.state["last_confirmed_online_at"] = iso(utcnow() - timedelta(days=8))
        engine._save()
        old_deadline = parse_iso(engine.state["deadline_at"])
        self.telegram.enqueue("yes I am here")
        engine._poll_telegram(block=False)

        new_deadline = parse_iso(engine.state["deadline_at"])
        self.assertGreater(new_deadline, old_deadline)
        confirmed = parse_iso(engine.state["last_confirmed_online_at"])
        self.assertGreater(confirmed, utcnow() - timedelta(minutes=2))
        remaining = (new_deadline - utcnow()).total_seconds()
        self.assertGreater(remaining, 6.9 * 86400)
        self.assertLess(remaining, 7.1 * 86400)
        self.assertEqual(engine.state["phase"], PHASE_MONITORING)
        self.assertEqual(engine.state["telegram_offset"], 2)
        self.assertTrue(
            self.telegram.sent_containing("Check-in received"),
            f"no confirmation sent; messages={self.telegram.texts()}",
        )

    def test_unauthorized_chat_cannot_reset_timer(self):
        engine = self.make_engine()
        old_deadline = engine.state["deadline_at"]
        self.telegram.enqueue("let me in", chat_id="999999", user_id="111")
        engine._poll_telegram(block=False)
        self.assertEqual(engine.state["deadline_at"], old_deadline)

    def test_send_failure_does_not_crash_monitoring(self):
        self.telegram.faults.append(
            Fault(method="POST", path_contains="sendMessage", action="http_500", times=-1)
        )
        engine = self.make_engine()
        engine._monitoring_tick()  # must not raise
        self.assertEqual(engine.state["phase"], PHASE_MONITORING)
        self.assertIsNone(engine.state.get("last_checkin_sent_at"))

    def test_two_tick_confirmation_before_self_destruction(self):
        engine = self.make_engine()
        engine.state["deadline_at"] = iso(utcnow() - timedelta(seconds=5))
        engine._save()

        engine._monitoring_tick()
        self.assertEqual(engine.state["phase"], PHASE_MONITORING, "one tick must not destroy anything")
        self.assertIsNotNone(engine.state.get("deadline_seen_at"))

        engine._monitoring_tick()
        self.assertEqual(engine.state["phase"], PHASE_TEARDOWN)
        self.assertEqual([s["name"] for s in engine.state["steps"]], list(STEPS))


# --------------------------------------------------------------------------- #
# Scenario 2 — full teardown
# --------------------------------------------------------------------------- #
class TestScenario2FullTeardown(HttpHarness):
    def test_full_teardown_end_to_end(self):
        self.billing.status = 200
        cfg = self.make_config(billing_mode="webhook", billing_webhook_url=self.billing.base_url + "/billing")
        engine = self.make_engine(cfg)
        self.start_teardown(engine)
        self.drive_to_completion(engine)

        st = engine.state
        self.assertEqual(st["phase"], PHASE_DONE)
        self.assertTrue(st["main_server_deleted"])
        self.assertIsNotNone(st["main_server_deleted"] and st["main_delete_issued_at"])
        self.assertIsNotNone(st["teardown_finished_at"])

        # --- Telegram notifications -------------------------------------
        self.assertTrue(self.telegram.sent_containing("SELF-DESTRUCTION STARTED"))
        self.assertTrue(self.telegram.sent_containing("deleting the main server now"))
        self.assertTrue(self.telegram.sent_containing("Main server deleted"))

        # --- floating IPs ------------------------------------------------
        self.assertEqual(self.hetzner.floating_ips, {})
        self.assert_protection_cleared_before("floating", 201)

        # --- other servers and their primary IPs -------------------------
        self.assertNotIn(2, self.hetzner.servers)
        self.assert_protection_cleared_before("server", 2)
        self.assertNotIn(103, self.hetzner.primary_ips)
        self.assertNotIn(104, self.hetzner.primary_ips)
        self.assert_protection_cleared_before("primary", 104)

        # --- live snapshot, creating -> available, protected -------------
        self.assertEqual(self.hetzner.snapshot_seen_status, "running")
        snap = st["snapshot"]
        self.assertTrue(snap.get("image_id"))
        self.assertEqual(snap.get("status"), "available")
        self.assertTrue(snap.get("live"))
        self.assertTrue(self.hetzner.images[snap["image_id"]]["protection"]["delete"])
        # Existing snapshots are untouched.
        self.assertIn(900, self.hetzner.images)
        self.assertTrue(self.hetzner.images[900]["protection"]["delete"])
        create_calls = [a for a in self.hetzner.actions_performed if a["command"] == "create_snapshot"]
        self.assertEqual(len(create_calls), 1)

        # --- billing ran before the main server was deleted --------------
        self.assertTrue(self.billing.payloads)
        self.assertEqual(self.billing.payloads[0]["event"], "deadman.teardown")
        billing_at = self.hetzner.requests.index(
            next(r for r in self.hetzner.requests if "/primary_ips/101/actions/change_protection" in r["path"])
        )
        delete_at = next(
            i
            for i, r in enumerate(self.hetzner.requests)
            if r["method"] == "DELETE" and r["path"].endswith("/servers/1")
        )
        self.assertLess(billing_at, delete_at)
        self.assertEqual(st["billing"]["status"], "succeeded")

        # --- exact state at the moment of DELETE /servers/1 --------------
        self.assertTrue(self.hetzner.server_delete_snapshots)
        at_delete = self.hetzner.server_delete_snapshots[-1]
        self.assertFalse(at_delete["server_protection"]["delete"])
        for ip_id in (101, 102):
            record = at_delete["primary_ips"][ip_id]
            self.assertFalse(record["auto_delete"], f"IP {ip_id} auto_delete must be false at delete")
            self.assertFalse(record["protection"]["delete"], f"IP {ip_id} must be unprotected at delete")
            # ... and it was never unassigned before the delete (T4 + on-host).
            self.assertEqual(record["assignee_id"], 1)

        # --- main server gone, main IPs survived protected ---------------
        self.assertNotIn(1, self.hetzner.servers)
        for ip_id in (101, 102):
            ip = self.hetzner.primary_ips[ip_id]
            self.assertIsNone(ip["assignee_id"])
            self.assertFalse(ip["auto_delete"])
            self.assertTrue(ip["protection"]["delete"])
        self.assertEqual(st["main_ip_prepared"]["prepared"][0]["auto_delete_after"], False)

        # --- never powered off / unassigned while running ---------------
        self.assertFalse(
            [a for a in self.hetzner.actions_performed if a.get("server_id") == 1 and a["command"] in ("shutdown", "poweroff")]
        )
        self.assertFalse(
            [
                a
                for a in self.hetzner.actions_performed
                if a.get("command") == "unassign_primary_ip" and a.get("primary_ip_id") in (101, 102)
            ]
        )

        # --- post-run invariant -----------------------------------------
        report = engine.verify_postconditions()
        self.assertEqual(report["violations"], [], report)

    def test_billing_webhook_500_does_not_stop_teardown(self):
        self.billing.status = 500
        cfg = self.make_config(billing_mode="webhook", billing_webhook_url=self.billing.base_url + "/billing")
        engine = self.make_engine(cfg)
        self.start_teardown(engine)
        self.drive_to_completion(engine)
        self.assertEqual(engine.state["phase"], PHASE_DONE)
        self.assertNotIn(1, self.hetzner.servers)
        self.assertIn(engine.state["billing"]["status"], ("skipped", "succeeded"))

    def test_billing_webhook_timeout_does_not_stop_teardown(self):
        self.billing.delay = 1.5
        cfg = self.make_config(
            billing_mode="webhook",
            billing_webhook_url=self.billing.base_url + "/billing",
            billing_timeout_seconds=1,
        )
        engine = self.make_engine(cfg)
        self.start_teardown(engine)
        self.drive_to_completion(engine)
        self.assertEqual(engine.state["phase"], PHASE_DONE)
        self.assertNotIn(1, self.hetzner.servers)
        self.assertEqual(engine.state["billing"]["status"], "skipped")

    def test_billing_command_nonzero_does_not_stop_teardown(self):
        cfg = self.make_config(
            billing_mode="command",
            billing_command=["/bin/sh", "-c", "exit 3"],
            billing_timeout_seconds=5,
        )
        engine = self.make_engine(cfg)
        self.start_teardown(engine)
        self.drive_to_completion(engine)
        self.assertEqual(engine.state["phase"], PHASE_DONE)
        self.assertNotIn(1, self.hetzner.servers)
        self.assertEqual(engine.state["billing"]["status"], "skipped")
        self.assertEqual(engine.state["billing"]["returncode"], 3)

    def test_billing_command_missing_binary_does_not_stop_teardown(self):
        cfg = self.make_config(
            billing_mode="command",
            billing_command=["/nonexistent/definitely-not-here"],
        )
        engine = self.make_engine(cfg)
        self.start_teardown(engine)
        self.drive_to_completion(engine)
        self.assertEqual(engine.state["phase"], PHASE_DONE)
        self.assertNotIn(1, self.hetzner.servers)

    def test_second_run_after_done_does_not_redelete(self):
        engine = self.make_engine()
        self.start_teardown(engine)
        self.drive_to_completion(engine)
        deletes = [r for r in self.hetzner.requests if r["method"] == "DELETE" and r["path"].endswith("/servers/1")]
        self.assertEqual(len(deletes), 1)

        # Simulate a container restart after DONE.
        restarted = self.make_engine()
        self.assertEqual(restarted.state["phase"], PHASE_DONE)
        restarted._done_tick()
        self.assertEqual(
            [r for r in self.hetzner.requests if r["method"] == "DELETE" and r["path"].endswith("/servers/1")],
            deletes,
        )
        self.assertEqual(restarted.state["phase"], PHASE_DONE)


# --------------------------------------------------------------------------- #
# Scenario 3 — everything protected
# --------------------------------------------------------------------------- #
class TestScenario3AllProtected(HttpHarness):
    def test_every_protection_is_cleared_before_the_corresponding_delete(self):
        self.hetzner.servers[2]["protection"] = {"delete": True, "rebuild": True}
        self.hetzner.primary_ips[103]["protection"] = {"delete": True}
        self.hetzner.primary_ips[104]["protection"] = {"delete": True}
        self.hetzner.primary_ips[101]["protection"] = {"delete": True}
        self.hetzner.primary_ips[102]["protection"] = {"delete": True}
        self.hetzner.floating_ips[201]["protection"] = {"delete": True}

        engine = self.make_engine()
        self.start_teardown(engine)
        self.drive_to_completion(engine)

        self.assertEqual(engine.state["phase"], PHASE_DONE)
        self.assert_protection_cleared_before("floating", 201)
        self.assert_protection_cleared_before("server", 2)
        self.assert_protection_cleared_before("primary", 104)
        # The main server itself is protected too; DELETE works only after its
        # protection (and its attached IP protection) is cleared.
        self.assert_protection_cleared_before("server", 1)

        # The reserved IPs are never deleted: protection is dropped just before
        # the server delete and restored immediately after the detach.
        sequence = [
            (a.get("command"), a.get("delete"), a.get("primary_ip_id"), a.get("server_id"))
            for a in self.hetzner.actions_performed
        ]
        for ip_id in (101, 102):
            dropped = sequence.index(("change_primary_ip_protection", False, ip_id, None))
            restored = sequence.index(("change_primary_ip_protection", True, ip_id, None))
            server_delete = sequence.index(("delete_server", None, None, 1))
            self.assertLess(dropped, server_delete, f"IP {ip_id} unprotected only after the delete")
            self.assertLess(server_delete, restored, f"IP {ip_id} was not re-protected after the delete")

        # The main Primary IP is never deleted, and ends protected + unassigned.
        deleted_ips = [a["primary_ip_id"] for a in self.hetzner.actions_performed if a["command"] == "delete_primary_ip"]
        self.assertNotIn(101, deleted_ips)
        self.assertNotIn(102, deleted_ips)
        for ip_id in (101, 102):
            ip = self.hetzner.primary_ips[ip_id]
            self.assertIsNone(ip["assignee_id"])
            self.assertTrue(ip["protection"]["delete"])
            self.assertFalse(ip["auto_delete"])

        report = engine.verify_postconditions()
        self.assertEqual(report["violations"], [], report)

    def test_protected_main_ip_blocks_server_delete_until_prepare(self):
        """T2 regression: if protection were left on, the mock refuses the DELETE."""
        self.hetzner.servers[1]["protection"] = {"delete": False, "rebuild": False}
        engine = self.make_engine()
        self.start_teardown(engine)
        # Snapshot the invariant: PREPARE must clear IP protection, and only
        # then may DELETE succeed. If the engine tried DELETE earlier it would
        # get a 422 and have to correct itself.
        engine._teardown_tick()
        self.assertEqual(engine.state["phase"], PHASE_DONE)
        cleared = [
            a
            for a in self.hetzner.actions_performed
            if a["command"] == "change_primary_ip_protection" and a["delete"] is False
        ]
        self.assertTrue(cleared)


# --------------------------------------------------------------------------- #
# Scenario 4 — outage, retries, persistence, skip
# --------------------------------------------------------------------------- #
class TestScenario4Faults(HttpHarness):
    def test_http_500_schedules_one_hour_retry_and_persists_across_restart(self):
        self.hetzner.faults.append(
            Fault(method="DELETE", path_contains="/floating_ips/201", action="http_500", times=2)
        )
        engine = self.make_engine()
        self.start_teardown(engine)
        engine._teardown_tick()

        step = engine.state["steps"][1]
        self.assertEqual(step["name"], "DELETE_FLOATING_IPS")
        self.assertEqual(step["status"], "failed_retryable")
        self.assertEqual(step["last_error"]["class"], "TRANSIENT")
        self.assertEqual(step["last_error"]["code"], "floating_ip_delete_incomplete")
        next_at = parse_iso(step["next_attempt_at"])
        delay = (next_at - utcnow()).total_seconds()
        self.assertGreater(delay, 3500)
        self.assertLess(delay, 3700)
        self.assertEqual(engine.state["phase"], PHASE_TEARDOWN)

        # --- process restart: the retry schedule must survive ------------
        restarted = self.make_engine()
        step2 = restarted.state["steps"][1]
        self.assertEqual(step2["status"], "failed_retryable")
        self.assertEqual(step2["attempts"], 1)
        self.assertEqual(step2["next_attempt_at"], step["next_attempt_at"])
        self.assertEqual(restarted.state["phase"], PHASE_TEARDOWN)

        # Not due yet -> a tick must not hammer the API.
        before = len(self.hetzner.requests)
        restarted._teardown_tick()
        self.assertEqual(len(self.hetzner.requests), before)

        # Once due (fault cleared) the flow completes.
        self.hetzner.clear_faults()
        restarted.state["steps"][1]["next_attempt_at"] = iso(utcnow() - timedelta(seconds=1))
        self.drive_to_completion(restarted)
        self.assertEqual(restarted.state["phase"], PHASE_DONE)
        self.assertNotIn(1, self.hetzner.servers)

    def test_retry_budget_exhausted_skips_step_and_advances(self):
        self.hetzner.faults.append(
            Fault(method="DELETE", path_contains="/floating_ips/201", action="http_500", times=-1)
        )
        engine = self.make_engine(self.make_config(max_retry_hours=0))
        self.start_teardown(engine)
        engine._teardown_tick()

        step = engine.state["steps"][1]
        self.assertEqual(step["status"], "skipped", step)
        self.assertIn("retry window exhausted", step["abandoned_reason"])
        # The flow advanced anyway and deleted the main server.
        self.assertEqual(engine.state["phase"], PHASE_DONE)
        self.assertNotIn(1, self.hetzner.servers)
        # ... and a CRITICAL alert was raised (it goes out on a daemon thread).
        deadline = time.time() + 3
        while time.time() < deadline and not self.telegram.sent_containing("step_abandoned"):
            time.sleep(0.02)
        self.assertTrue(self.telegram.sent_containing("step_abandoned"))

    def test_422_server_not_stopped_triggers_poweroff_then_retry(self):
        self.hetzner.faults.append(
            Fault(
                method="POST",
                path_contains="/primary_ips/103/actions/unassign",
                action="http_422",
                times=1,
                code="server_not_stopped",
                message="server must be powered off",
            )
        )
        engine = self.make_engine()
        self.start_teardown(engine)
        step = [s for s in engine.state["steps"] if s["name"] == "DELETE_OTHER_PRIMARY_IPS"][0]
        engine._execute_step(step)
        self.assertEqual(step["status"], "succeeded", step.get("last_error"))
        self.assertNotIn(103, self.hetzner.primary_ips)
        # The server was powered off before the retry, never the main server.
        self.assertIn(
            {"command": "shutdown", "server_id": 2},
            [a for a in self.hetzner.actions_performed if a.get("command") == "shutdown"],
        )

    def test_unreachable_api_marks_step_retryable(self):
        dead_port = self.free_port()
        cfg = self.make_config(base_url=f"http://127.0.0.1:{dead_port}/v1", request_timeout_seconds=0.5)
        engine = self.make_engine(cfg)
        self.start_teardown(engine)
        engine._teardown_tick()
        # _begin_teardown's inventory resolution failed too, so the first API
        # step fails; either way the flow must be alive and retryable.
        self.assertEqual(engine.state["phase"], PHASE_TEARDOWN)
        failed = [s for s in engine.state["steps"] if s["status"] == "failed_retryable"]
        self.assertTrue(failed, engine.state["steps"])
        self.assertEqual(failed[0]["last_error"]["class"], "TRANSIENT")
        self.assertIn("network error", failed[0]["last_error"]["message"].lower())

    def test_timeout_is_transient_and_retryable(self):
        self.hetzner.faults.append(
            Fault(
                method="DELETE",
                path_contains="/floating_ips/201",
                action="timeout",
                times=1,
                delay=1.0,
            )
        )
        engine = self.make_engine(self.make_config(request_timeout_seconds=0.3))
        self.start_teardown(engine)
        engine._teardown_tick()
        step = engine.state["steps"][1]
        self.assertEqual(step["status"], "failed_retryable")
        self.assertEqual(step["last_error"]["class"], "TRANSIENT")

    def test_malformed_json_is_a_retryable_error_not_a_crash(self):
        self.hetzner.faults.append(
            Fault(method="GET", path_contains="/primary_ips/101", action="malformed", times=-1)
        )
        engine = self.make_engine()
        self.start_teardown(engine)
        step = [s for s in engine.state["steps"] if s["name"] == "PREPARE_MAIN_PRIMARY_IP"][0]
        engine._execute_step(step)
        self.assertEqual(step["status"], "failed_retryable")
        self.assertEqual(step["last_error"]["code"], "main_ip_missing")
        # And nothing irreversible happened.
        self.assertIn(1, self.hetzner.servers)
        self.assertFalse(engine.state.get("main_delete_issued_at"))

    def test_hetzner_action_error_is_retryable(self):
        self.hetzner.faults.append(
            Fault(
                method="POST",
                path_contains="/servers/1/actions/create_image",
                action="action_error",
                times=1,
                code="server_error",
                message="image build exploded",
            )
        )
        engine = self.make_engine()
        self.start_teardown(engine)
        step = [s for s in engine.state["steps"] if s["name"] == "SNAPSHOT_MAIN_SERVER"][0]
        engine._execute_step(step)
        self.assertEqual(step["status"], "failed_retryable")
        self.assertEqual(step["last_error"]["code"], "server_error")
        self.assertIn(1, self.hetzner.servers)

    def test_http_500_on_ip_delete_is_retryable_and_keeps_the_ip(self):
        self.hetzner.faults.append(
            Fault(
                method="DELETE",
                path_contains="/primary_ips/104",
                action="http_500",
                times=-1,
            )
        )
        engine = self.make_engine()
        self.start_teardown(engine)
        step = [s for s in engine.state["steps"] if s["name"] == "DELETE_OTHER_PRIMARY_IPS"][0]
        engine._execute_step(step)
        self.assertEqual(step["status"], "failed_retryable")
        self.assertEqual(step["last_error"]["code"], "primary_ip_delete_incomplete")
        self.assertIn(104, self.hetzner.primary_ips)
        self.assertIn(1, self.hetzner.servers)


# --------------------------------------------------------------------------- #
# Scenario 5 — crash / restart recovery
# --------------------------------------------------------------------------- #
class TestScenario5Recovery(HttpHarness):
    def test_corrupt_json_recovers_from_block_slot(self):
        engine = self.make_engine()
        engine.state["phase"] = PHASE_TEARDOWN
        engine.state["marker"] = "from-block"
        engine.state["step_index"] = 3
        engine._save()
        generation = engine.state["generation"]

        with open(os.path.join(self.tmp, "state.json"), "w") as fh:
            fh.write("{ this is not json")
        with open(os.path.join(self.tmp, "state.json.bak"), "w") as fh:
            fh.write("also broken")

        restarted = self.make_engine()
        self.assertEqual(restarted.state["marker"], "from-block")
        self.assertEqual(restarted.state["phase"], PHASE_TEARDOWN)
        self.assertEqual(restarted.state["step_index"], 3)
        self.assertGreaterEqual(restarted.state["generation"], generation)
        self.assertTrue(str(restarted.state.get("_recovered_from", "")).startswith("state.block"))

    def test_missing_json_recovers_from_backup(self):
        cfg = self.make_config(enable_block_file=False)
        engine = self.make_engine(cfg)
        engine.state["marker"] = "from-bak"
        engine._save()
        engine.state["marker"] = "newer"
        engine._save()
        os.unlink(os.path.join(self.tmp, "state.json"))
        restarted = self.make_engine(cfg)
        # state.json is gone; whichever secondary store holds the newest
        # generation wins (bak and the SQLite mirror both carry the document).
        self.assertEqual(restarted.state["marker"], "newer")
        self.assertIn(
            restarted.state.get("_recovered_from"), ("state.json.bak", "state.db:kv")
        )

    def test_missing_all_files_recovers_from_sqlite(self):
        cfg = self.make_config(enable_block_file=False)
        engine = self.make_engine(cfg)
        engine.state["marker"] = "from-db"
        engine.state["step_index"] = 2
        engine._save()
        for name in ("state.json", "state.json.bak"):
            path = os.path.join(self.tmp, name)
            if os.path.exists(path):
                os.unlink(path)
        restarted = self.make_engine(cfg)
        self.assertEqual(restarted.state["marker"], "from-db")
        self.assertEqual(restarted.state["step_index"], 2)
        self.assertEqual(restarted.state.get("_recovered_from"), "state.db:kv")

    def test_missing_everything_starts_fresh(self):
        empty = tempfile.mkdtemp(prefix="deadman-empty-")
        self.addCleanup(shutil.rmtree, empty, True)
        cfg = self.make_config(state_dir=empty, log_file=os.path.join(empty, "x.log"))
        engine = self.make_engine(cfg)
        self.assertEqual(engine.state["phase"], PHASE_MONITORING)
        self.assertTrue(engine.state["run_id"])

    def test_running_step_is_reverified_on_restart(self):
        engine = self.make_engine()
        self.start_teardown(engine)
        engine.state["steps"][2]["status"] = "running"
        engine.state["steps"][2]["attempts"] = 4
        engine.state["step_index"] = 2
        engine._save()

        restarted = self.make_engine()
        step = restarted.state["steps"][2]
        self.assertEqual(step["status"], "failed_retryable")
        self.assertEqual(step["last_error"]["code"], "process_restarted")
        self.assertLessEqual(parse_iso(step["next_attempt_at"]), utcnow() + timedelta(seconds=2))
        # ... and the flow can be resumed to completion from there.
        self.drive_to_completion(restarted)
        self.assertEqual(restarted.state["phase"], PHASE_DONE)

    def test_backwards_clock_jump_clamps_retry_times(self):
        engine = self.make_engine()
        self.start_teardown(engine)
        step = engine.state["steps"][1]
        step["status"] = "failed_retryable"
        step["next_attempt_at"] = iso(utcnow() + timedelta(hours=1))
        engine.state["last_tick"] = iso(utcnow() + timedelta(hours=2))
        engine._save()

        restarted = self.make_engine()
        clamped = parse_iso(restarted.state["steps"][1]["next_attempt_at"])
        self.assertIsNotNone(clamped)
        self.assertLessEqual(clamped, utcnow() + timedelta(seconds=5))

    def test_flock_prevents_two_daemons(self):
        cfg = self.make_config()
        first = StateStore(cfg.state_dir, enable_block_file=False, logger=null_logger())
        second = StateStore(cfg.state_dir, enable_block_file=False, logger=null_logger())
        try:
            self.assertTrue(first.acquire_lock())
            self.assertFalse(second.acquire_lock())
            first.release_lock()
            self.assertTrue(second.acquire_lock())
        finally:
            first.close()
            second.close()

    def test_state_artefacts_that_are_unreadable_refuse_to_start_fresh(self):
        from deadman.storage import StorageError

        path = os.path.join(self.tmp, "state.json")
        with open(path, "w") as fh:
            fh.write("{ broken")
        engine = self.make_engine(start_state=False)
        with self.assertRaises(StorageError):
            engine.load_state()

    def test_done_state_survives_restart_without_repeating_deletes(self):
        engine = self.make_engine()
        self.start_teardown(engine)
        self.drive_to_completion(engine)
        restarted = self.make_engine()
        self.assertEqual(restarted.state["phase"], PHASE_DONE)
        self.assertTrue(restarted.state["main_server_deleted"])
        self.assertTrue(restarted.state.get("finalized") or restarted.state.get("step_index", 0) >= len(STEPS))


# --------------------------------------------------------------------------- #
# Scenario 6 — operator returns mid-teardown
# --------------------------------------------------------------------------- #
class TestScenario6Abort(HttpHarness):
    def test_abort_during_snapshot_poll_stops_before_main_delete(self):
        self.hetzner.image_ready_delay = 30.0  # long poll window
        engine = self.make_engine()
        self.start_teardown(engine)

        errors: list[BaseException] = []

        def _run() -> None:
            try:
                engine._teardown_tick()
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        worker = threading.Thread(target=_run, name="teardown", daemon=True)
        worker.start()

        # Wait until the snapshot step is actually polling for the image.
        deadline = time.time() + 15
        while time.time() < deadline:
            snap_step = next(
                (s for s in engine.state.get("steps", []) if s["name"] == "SNAPSHOT_MAIN_SERVER"),
                None,
            )
            if snap_step and snap_step["status"] == "running" and self.hetzner.snapshot_seen_status:
                break
            time.sleep(0.02)

        engine._last_telegram_poll = 0.0
        self.telegram.enqueue("/abort")
        worker.join(timeout=20)
        self.assertFalse(worker.is_alive(), "abort did not unwind the snapshot poll")
        self.assertEqual(errors, [])

        st = engine.state
        self.assertEqual(st["phase"], PHASE_MONITORING)
        self.assertFalse(st.get("main_server_deleted"))
        self.assertIsNone(st.get("main_delete_issued_at"))
        # Main server still alive; nothing irreversible happened.
        self.assertIn(1, self.hetzner.servers)
        self.assertFalse(
            [r for r in self.hetzner.requests if r["method"] == "DELETE" and r["path"].endswith("/servers/1")]
        )
        # Main Primary IP is still assigned and still protected.
        self.assertEqual(self.hetzner.primary_ips[101]["assignee_id"], 1)
        self.assertTrue(self.hetzner.primary_ips[101]["protection"]["delete"])
        self.assertTrue(self.hetzner.primary_ips[102]["protection"]["delete"])
        # Timer reset.
        remaining = (parse_iso(st["deadline_at"]) - utcnow()).total_seconds()
        self.assertGreater(remaining, 6.9 * 86400)
        self.assertTrue(self.telegram.sent_containing("ABORTED"))

    def test_any_message_during_teardown_aborts_after_prepare(self):
        engine = self.make_engine()
        self.start_teardown(engine)
        # Run the first six steps so PREPARE has (not yet) run; abort now.
        for name in (
            "NOTIFY_TEARDOWN_START",
            "DELETE_FLOATING_IPS",
            "DELETE_OTHER_SERVERS",
            "DELETE_OTHER_PRIMARY_IPS",
        ):
            step = next(s for s in engine.state["steps"] if s["name"] == name)
            engine._execute_step(step)
            engine.state["step_index"] += 1

        self.telegram.enqueue("wait, I'm back!")
        engine._poll_telegram(block=False)
        self.assertEqual(engine.state["phase"], PHASE_MONITORING)
        self.assertIn(1, self.hetzner.servers)
        self.assertTrue(self.hetzner.primary_ips[101]["protection"]["delete"])
        self.assertEqual(engine.state["abort_requested"], False)

    def test_abort_after_delete_is_refused(self):
        engine = self.make_engine()
        self.start_teardown(engine)
        engine.state["main_delete_issued_at"] = iso()
        self.telegram.enqueue("/abort")
        engine._poll_telegram(block=False)
        self.assertEqual(engine.state["phase"], PHASE_TEARDOWN)
        self.assertTrue(self.telegram.sent_containing("Too late"))


# --------------------------------------------------------------------------- #
# Bug-specific regressions (1-7)
# --------------------------------------------------------------------------- #
class TestBug1DeleteOrdering(HttpHarness):
    def test_state_is_done_and_ip_reprotected_even_if_server_never_404s(self):
        """The VM dies before GET /servers/{main} can ever return 404."""
        self.hetzner.keep_server_after_delete = True
        engine = self.make_engine()
        self.start_teardown(engine)
        step = next(s for s in engine.state["steps"] if s["name"] == "DELETE_MAIN_SERVER")

        started = time.time()
        engine._execute_step(step)
        elapsed = time.time() - started
        self.assertEqual(step["status"], "succeeded", step)
        self.assertLess(elapsed, 10.0, "delete step blocked waiting for a 404 that cannot happen")

        st = engine.state
        self.assertEqual(st["phase"], PHASE_DONE)
        self.assertTrue(st["main_server_deleted"])
        self.assertIsNotNone(st["main_delete_issued_at"])
        self.assertIsNotNone(st["teardown_finished_at"])

        # Persisted immediately, not after the poll.
        with open(os.path.join(self.tmp, "state.json")) as fh:
            saved = json.load(fh)
        self.assertEqual(saved["phase"], "DONE")
        self.assertTrue(saved["main_server_deleted"])
        self.assertIsNotNone(saved["main_delete_issued_at"])

        # The IP is detached by the server delete and was re-protected at once.
        self.assertIsNone(self.hetzner.primary_ips[101]["assignee_id"])
        self.assertTrue(self.hetzner.primary_ips[101]["protection"]["delete"])
        self.assertTrue(self.hetzner.primary_ips[102]["protection"]["delete"])

    def test_failed_delete_reverts_to_teardown(self):
        self.hetzner.faults.append(
            Fault(method="DELETE", path_contains="/servers/1", action="http_500", times=-1)
        )
        engine = self.make_engine()
        self.start_teardown(engine)
        step = next(s for s in engine.state["steps"] if s["name"] == "DELETE_MAIN_SERVER")
        engine._execute_step(step)

        self.assertEqual(step["status"], "failed_retryable")
        self.assertEqual(engine.state["phase"], PHASE_TEARDOWN)
        self.assertFalse(engine.state["main_server_deleted"])
        self.assertIsNone(engine.state["main_delete_issued_at"])
        self.assertIn(1, self.hetzner.servers)
        # A retry is scheduled, not an infinite loop.
        self.assertIsNotNone(step["next_attempt_at"])

    def test_delete_404_is_success(self):
        engine = self.make_engine()
        self.start_teardown(engine)
        # The server vanished out-of-band; Hetzner detaches its IPs as part of
        # that deletion, so mirror that in the fixture.
        self.hetzner.servers.pop(1)
        for ip_id in (101, 102):
            self.hetzner.primary_ips[ip_id]["assignee_id"] = None
            self.hetzner.primary_ips[ip_id]["assignee_type"] = "unassigned"
        step = next(s for s in engine.state["steps"] if s["name"] == "DELETE_MAIN_SERVER")
        engine._execute_step(step)
        self.assertEqual(step["status"], "succeeded")
        self.assertTrue(engine.state["main_server_deleted"])
        self.assertEqual(engine.state["phase"], PHASE_DONE)
        self.assertTrue(self.hetzner.primary_ips[101]["protection"]["delete"])


class TestBug2PartialTargetConfig(HttpHarness):
    def test_address_form_main_ip_resolves_and_populates_state(self):
        cfg = self.make_config(main_primary_ip_ids=[], main_primary_ip="203.0.113.10")
        engine = self.make_engine(cfg)
        self.assertEqual(engine.state["protected_ip_ids"], [])
        engine._ensure_targets()
        self.assertEqual(engine.state["protected_ip_ids"], [101])
        self.assertEqual(engine.state["protected_ips"], ["203.0.113.10"])
        self.assertTrue(engine.state["targets_resolved"])
        self.assertEqual(engine.state["main_server_id"], 1)
        self.assertEqual(engine.state["inventory"]["main_server_id"], 1)
        self.assertEqual(engine.state["inventory"]["servers"][0]["name"], "main")

    def test_partial_id_config_unions_server_ips(self):
        cfg = self.make_config(main_primary_ip_ids=[], reserve_main_ipv6=True)
        engine = self.make_engine(cfg)
        engine._ensure_targets()
        self.assertEqual(engine.state["protected_ip_ids"], [101, 102])
        self.assertIn("2001:db8::/64", engine.state["protected_ips"])

    def test_resolution_is_idempotent_and_cached(self):
        engine = self.make_engine()
        engine._ensure_targets()
        before = len(self.hetzner.requests)
        engine._ensure_targets()
        self.assertEqual(len(self.hetzner.requests), before, "targets re-resolved on every step")

    def test_empty_protected_set_refuses_to_teardown(self):
        cfg = self.make_config(main_primary_ip_ids=[], main_primary_ip="198.51.100.99")
        engine = self.make_engine(cfg)
        with self.assertRaises(HCloudError) as ctx:
            engine._ensure_targets()
        self.assertEqual(ctx.exception.code, "preflight_no_protected_ip")
        self.assertIn(1, self.hetzner.servers)


class TestBug3ReserveOnlyMainIpv4(HttpHarness):
    def test_explicit_ipv4_only_deletes_ipv6_with_the_server(self):
        cfg = self.make_config(main_primary_ip_ids=[101], reserve_main_ipv6=False)
        engine = self.make_engine(cfg)
        self.start_teardown(engine)
        self.drive_to_completion(engine)

        self.assertEqual(engine.state["phase"], PHASE_DONE)
        # The chosen IPv4 survives, unassigned and protected.
        self.assertIn(101, self.hetzner.primary_ips)
        self.assertIsNone(self.hetzner.primary_ips[101]["assignee_id"])
        self.assertTrue(self.hetzner.primary_ips[101]["protection"]["delete"])
        # The non-reserved IPv6 was explicitly marked auto_delete and vanished
        # together with the server.
        self.assertNotIn(102, self.hetzner.primary_ips)
        auto_deleted = [
            a for a in self.hetzner.actions_performed if a["command"] == "auto_delete_primary_ip"
        ]
        self.assertIn(102, [a["primary_ip_id"] for a in auto_deleted])
        # ... and it was never passed to an explicit DELETE while attached.
        explicit_deletes = [
            a["primary_ip_id"] for a in self.hetzner.actions_performed if a["command"] == "delete_primary_ip"
        ]
        self.assertNotIn(102, explicit_deletes)

    def test_address_only_config_reserves_exactly_that_ip(self):
        cfg = self.make_config(main_primary_ip_ids=[], main_primary_ip="203.0.113.10")
        engine = self.make_engine(cfg)
        self.start_teardown(engine)
        self.drive_to_completion(engine)
        self.assertIn(101, self.hetzner.primary_ips)
        self.assertNotIn(102, self.hetzner.primary_ips)
        self.assertFalse(engine.state["main_server_deleted"] is False)


class TestBug4SnapshotBeforeProtectionDrop(HttpHarness):
    def test_snapshot_happens_while_main_ip_is_still_protected(self):
        engine = self.make_engine()
        self.start_teardown(engine)
        engine._teardown_tick()
        self.assertEqual(engine.state["phase"], PHASE_DONE)

        # Index in the ordered request log where protection was first lowered.
        lowered = next(
            i
            for i, r in enumerate(self.hetzner.requests)
            if r["method"] == "POST" and r["path"].endswith("/primary_ips/101/actions/change_protection")
        )
        snapshotted = next(
            i
            for i, r in enumerate(self.hetzner.requests)
            if r["method"] == "POST" and r["path"].endswith("/servers/1/actions/create_image")
        )
        delete = next(
            i
            for i, r in enumerate(self.hetzner.requests)
            if r["method"] == "DELETE" and r["path"].endswith("/servers/1")
        )
        self.assertLess(snapshotted, lowered, "snapshot must precede lowering main IP protection")
        self.assertLess(lowered, delete)
        # Measure the unprotected window in wall-clock time, which is what
        # actually matters (it used to be hours: snapshot + billing).
        window = self.hetzner.requests[delete]["at"] - self.hetzner.requests[lowered]["at"]
        self.assertLess(window, 3.0, f"main IP was unprotected for {window:.2f}s")

    def test_step_order_is_documented_order(self):
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


class TestBug5AutoDeleteFlipFailure(HttpHarness):
    def test_server_is_deleted_even_if_auto_delete_cannot_be_enabled(self):
        self.hetzner.reject_auto_delete_true = True
        engine = self.make_engine()
        self.start_teardown(engine)

        step = next(s for s in engine.state["steps"] if s["name"] == "DELETE_OTHER_SERVERS")
        engine._execute_step(step)
        self.assertEqual(step["status"], "succeeded", step["last_error"])
        self.assertNotIn(2, self.hetzner.servers)
        # Its IP was detached instead of auto-deleted, and remained for step 4.
        self.assertIn(103, self.hetzner.primary_ips)

        step4 = next(s for s in engine.state["steps"] if s["name"] == "DELETE_OTHER_PRIMARY_IPS")
        engine._execute_step(step4)
        self.assertEqual(step4["status"], "succeeded", step4["last_error"])
        self.assertNotIn(103, self.hetzner.primary_ips)


class TestBug6LoadBalancerIpTarget(HttpHarness):
    def _run_ip_step(self, targets):
        self.hetzner.add_load_balancer(77, targets)
        engine = self.make_engine()
        self.start_teardown(engine)
        step = next(s for s in engine.state["steps"] if s["name"] == "DELETE_OTHER_PRIMARY_IPS")
        engine._execute_step(step)
        return engine, step

    def test_ip_target_is_removed_then_unassign_succeeds(self):
        engine, step = self._run_ip_step([{"type": "ip", "ip": {"ip": "203.0.113.11"}}])
        self.assertEqual(step["status"], "succeeded", step.get("last_error"))
        self.assertNotIn(103, self.hetzner.primary_ips)
        removals = [a for a in self.hetzner.actions_performed if a["command"] == "remove_lb_target"]
        self.assertTrue(removals)
        self.assertEqual(removals[0]["target"]["type"], "ip")
        self.assertEqual(removals[0]["target"]["ip"]["ip"], "203.0.113.11")

    def test_server_target_still_works(self):
        engine, step = self._run_ip_step([{"type": "server", "server": {"id": 2, "ip": "203.0.113.11"}}])
        self.assertEqual(step["status"], "succeeded", step.get("last_error"))
        self.assertNotIn(103, self.hetzner.primary_ips)

    def test_label_selector_target_still_works(self):
        engine, step = self._run_ip_step(
            [
                {
                    "type": "label_selector",
                    "label_selector": {"selector": "role=worker"},
                    "targets": [{"type": "server", "server": {"id": 2, "ip": "203.0.113.11"}}],
                }
            ]
        )
        self.assertEqual(step["status"], "succeeded", step.get("last_error"))
        self.assertNotIn(103, self.hetzner.primary_ips)


class TestBug7ConfigAliases(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="deadman-cfg-")
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def _clean_env(self, *names: str) -> None:
        for name in names:
            os.environ.pop(name, None)

    def test_grace_period_env_aliases(self):
        for alias in ("X_DAYS", "DAYS_BEFORE_SELF_DESTRUCT", "INACTIVITY_DAYS"):
            with self.subTest(alias=alias):
                os.environ[alias] = "4"
                try:
                    cfg = Config.load(
                        overrides={"token": "x", "state_dir": self.tmp},
                        require_token=False,
                    )
                    self.assertEqual(cfg.grace_period_days, 4.0)
                finally:
                    self._clean_env(alias)

    def test_retry_hours_alias(self):
        os.environ["RETRY_HOURS"] = "2.5"
        try:
            cfg = Config.load(overrides={"token": "x", "state_dir": self.tmp}, require_token=False)
            self.assertEqual(cfg.max_retry_hours, 2.5)
        finally:
            self._clean_env("RETRY_HOURS")

    def test_lease_timeout_hours_is_converted_to_days(self):
        path = os.path.join(self.tmp, "config.json")
        with open(path, "w") as fh:
            json.dump({"lease": {"timeout_hours": 72}}, fh)
        cfg = Config.load(config_file=path, overrides={"token": "x", "state_dir": self.tmp})
        self.assertEqual(cfg.grace_period_days, 3.0)
        self.assertEqual(cfg.unknown_file_keys, [])

    def test_lease_grace_period_days_is_not_converted(self):
        path = os.path.join(self.tmp, "config.json")
        with open(path, "w") as fh:
            json.dump({"lease": {"grace_period_days": 3}}, fh)
        cfg = Config.load(config_file=path, overrides={"token": "x", "state_dir": self.tmp})
        self.assertEqual(cfg.grace_period_days, 3.0)

    def test_bad_lease_hours_is_rejected(self):
        path = os.path.join(self.tmp, "config.json")
        with open(path, "w") as fh:
            json.dump({"lease": {"timeout_hours": "soon"}}, fh)
        with self.assertRaises(ConfigError):
            Config.load(config_file=path, overrides={"token": "x", "state_dir": self.tmp})


if __name__ == "__main__":
    unittest.main()
