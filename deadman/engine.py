"""The dead man's switch state machine.

Phases
------
``MONITORING``
    Wait for Telegram check-ins.  Every ``CHECKIN_INTERVAL_HOURS`` send a
    reminder; when ``now >= deadline_at`` (last check-in + grace period) begin
    teardown.

``TEARDOWN``
    Run the fixed step list in order.  Every step is idempotent.  Any API error
    — 4xx, 5xx, timeout, DNS, garbage JSON — schedules a retry one hour later.
    After ``MAX_RETRY_HOURS`` without success the step is skipped and the flow
    advances, so it can never wedge.  Any Telegram message from the operator
    aborts before the main server is deleted.

``DONE``
    The main server has been deleted (or deletion was gated).  If the process
    still lives (external host), keep replying to /status and re-assert the
    main Primary IP invariant periodically.

Critical ordering (this is what makes running *on* the main server safe):

1. notify, 2. delete floating IPs, 3. delete other servers, 4. delete other
Primary IPs, 5. **live** snapshot of the still-running main server (taken while
the main server *and* its Primary IP still carry full protection),
6. billing hook, 7. prepare the main Primary IP (`auto_delete=false`; disable
delete protection while it is attached so the server delete is permitted),
8. delete the main server.  Never power off the main server.  Never unassign
its Primary IP before deletion.

Lowering the main IP's protection is deliberately the *second to last* thing
that happens: the window in which `protection.delete` is `false` is the few
milliseconds between `PREPARE_MAIN_PRIMARY_IP` and `DELETE_MAIN_SERVER`, not
the hours a snapshot or a billing hook can take.
"""

from __future__ import annotations

import signal
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from .billing import BillingStep
from .config import Config
from .hetzner import PERMANENT, TRANSIENT, HCloudClient, HCloudError
from .log import JsonLogger
from .storage import StateStore, StorageError
from .telegram import TelegramClient
from .util import (
    humanize_duration,
    iso,
    new_run_id,
    parse_iso,
    seconds_until,
    utcnow,
)

PHASE_MONITORING = "MONITORING"
PHASE_TEARDOWN = "TEARDOWN"
PHASE_DONE = "DONE"
PHASE_ABORTED = "ABORTED"

STEPS: tuple[str, ...] = (
    "NOTIFY_TEARDOWN_START",
    "DELETE_FLOATING_IPS",
    "DELETE_OTHER_SERVERS",
    "DELETE_OTHER_PRIMARY_IPS",
    "SNAPSHOT_MAIN_SERVER",
    "SETTLE_BILLING",
    "PREPARE_MAIN_PRIMARY_IP",
    "DELETE_MAIN_SERVER",
)

STEP_LABELS = {
    "NOTIFY_TEARDOWN_START": "notify",
    "DELETE_FLOATING_IPS": "floating IPs",
    "DELETE_OTHER_SERVERS": "other servers",
    "DELETE_OTHER_PRIMARY_IPS": "other Primary IPs",
    "SNAPSHOT_MAIN_SERVER": "snapshot main",
    "SETTLE_BILLING": "billing",
    "PREPARE_MAIN_PRIMARY_IP": "preserve main IP",
    "DELETE_MAIN_SERVER": "delete main server",
}

MILESTONES = (
    (48 * 3600, "48h"),
    (24 * 3600, "24h"),
    (6 * 3600, "6h"),
    (3600, "1h"),
    (600, "10m"),
)


class TeardownAborted(Exception):
    """Raised from a poll hook when the operator sends /abort mid-step."""


class SnapshotGateError(RuntimeError):
    """Snapshot is required but missing; the main server must not be deleted."""


def _new_step(name: str, now: datetime) -> dict[str, Any]:
    return {
        "name": name,
        "status": "pending",
        "attempts": 0,
        "first_attempt_at": None,
        "first_error_at": None,
        "last_attempt_at": None,
        "next_attempt_at": iso(now),
        "last_error": None,
        "abandoned_reason": None,
        "started_at": None,
        "finished_at": None,
        "detail": {},
    }


class Engine:
    def __init__(
        self,
        config: Config,
        store: Optional[StateStore] = None,
        client: Optional[HCloudClient] = None,
        telegram: Optional[TelegramClient] = None,
        logger: Optional[JsonLogger] = None,
    ) -> None:
        self.config = config
        self.logger = logger or JsonLogger(config.log_file, level=config.log_level)
        self.store = store or StateStore(
            config.state_dir,
            block_size_bytes=config.block_file_bytes,
            enable_block_file=config.enable_block_file,
            logger=self.logger,
        )
        self.client = client or HCloudClient(config, logger=self.logger)
        self.telegram = telegram or TelegramClient(
            config.telegram_bot_token,
            config.telegram_chat_id,
            config.telegram_user_id,
            api_base=config.telegram_api_base,
            logger=self.logger,
        )
        self.client.logger = self.logger
        self.client.event_sink = self._api_event
        self.client.poll_hook = self._poll_during_long_operation
        self.billing = BillingStep(config, self.logger)
        self.state: dict[str, Any] = {}
        self._stop = threading.Event()
        self._dirty = False
        self._in_step = False
        self._last_telegram_poll = 0.0

    # ================================================================== state
    def new_state(self) -> dict[str, Any]:
        now = utcnow()
        state = {
            "schema_version": 2,
            "run_id": new_run_id(),
            "created_at": iso(now),
            "updated_at": iso(now),
            "phase": PHASE_MONITORING,
            "last_confirmed_online_at": iso(now),
            "deadline_at": iso(now + timedelta(days=self.config.grace_period_days)),
            "grace_period_days": self.config.grace_period_days,
            "checkin_interval_hours": self.config.checkin_interval_hours,
            "last_checkin_sent_at": None,
            "last_checkin_attempt_at": None,
            "warnings_sent": 0,
            "milestones_sent": [],
            "deadline_seen_at": None,
            "telegram_offset": 0,
            "step_index": 0,
            "steps": [],
            "teardown_started_at": None,
            "teardown_finished_at": None,
            "main_server_id": self.config.main_server_id,
            "main_server_name": self.config.main_server_name or None,
            "protected_ip_ids": list(self.config.main_primary_ip_ids),
            "protected_ips": list(self.config.protected_ips),
            "targets_resolved": False,
            "inventory": {},
            "snapshot": {},
            "snapshot_history": [],
            "main_ip_prepared": {},
            "billing": {},
            "counters": {"api_calls": 0, "errors": 0, "telegram_sent": 0},
            "abort_requested": False,
            "main_delete_issued_at": None,
            "main_server_deleted": False,
            "last_tick": iso(now),
            "errors": [],
        }
        return state

    def load_state(self) -> dict[str, Any]:
        state = self.store.load()
        if state is None:
            # Never silently start a fresh MONITORING run if state artefacts
            # exist but none could be parsed: that could re-arm a dead man's
            # switch whose teardown already started.
            artefacts = self.store.json_path.exists() or self.store.bak_path.exists()
            if not artefacts and self.store.block is not None:
                slot = self.store.block.read_best()
                artefacts = bool(slot and b'"phase"' in slot.get("payload", b""))
            if artefacts:
                raise StorageError(
                    "state artefacts exist but no valid state could be recovered; "
                    "refusing to start fresh (inspect state.json/state.block/state.db)"
                )
            state = self.new_state()
            self.store.save(state)
            self.logger.info("state_initialised", "fresh state created", run_id=state["run_id"])
            return state

        # Backfill keys added by newer versions without losing old progress.
        defaults = self.new_state()
        for key, value in defaults.items():
            state.setdefault(key, value)
        state.setdefault("inventory", {})
        state.setdefault("snapshot", {})
        state.setdefault("counters", {"api_calls": 0, "errors": 0, "telegram_sent": 0})
        state.setdefault("milestones_sent", [])
        state.setdefault("errors", [])

        # A step that was mid-flight when the process died must be re-verified.
        for step in state.get("steps") or []:
            if step.get("status") == "running":
                step["status"] = "failed_retryable"
                step["next_attempt_at"] = iso(utcnow())
                step["last_error"] = {
                    "code": "process_restarted",
                    "message": "step was running when the process died; re-verifying",
                    "class": "TRANSIENT",
                }

        # Clock sanity: a backwards jump must not park retries in the future.
        last_tick = parse_iso(state.get("last_tick"))
        now = utcnow()
        if last_tick and now < last_tick - timedelta(seconds=120):
            self.logger.warning(
                "clock_skew", "system clock moved backwards", last_tick=state.get("last_tick")
            )
            self._alert("clock_skew", "System clock moved backwards; retry times clamped.", critical=False)
            for step in state.get("steps") or []:
                nxt = parse_iso(step.get("next_attempt_at"))
                if nxt and nxt > now:
                    step["next_attempt_at"] = iso(now)
        return state

    def _save(self) -> None:
        self.store.save(self.state)
        self._dirty = False

    def _api_event(self, event: str, payload: dict) -> None:
        try:
            self.store.event(
                event,
                run_id=self.state.get("run_id", ""),
                step=self._current_step_name(),
                payload=payload,
            )
        except Exception:
            pass

    def _current_step_name(self) -> Optional[str]:
        try:
            idx = int(self.state.get("step_index", 0))
            steps = self.state.get("steps") or []
            if 0 <= idx < len(steps):
                return steps[idx].get("name")
        except Exception:
            pass
        return None

    # ==================================================================== run
    def run(self) -> None:
        self.state = self.load_state()
        self.client.state = self.state
        self._install_signal_handlers()
        self.logger.info(
            "engine_start",
            "hetzner-deadman started",
            phase=self.state.get("phase"),
            dry_run=self.config.dry_run,
            telegram=self.telegram.enabled,
        )
        if not self.telegram.enabled:
            self.logger.warning(
                "telegram_disabled",
                "Telegram is not configured: check-ins via chat are impossible; "
                "set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID",
            )
        while not self._stop.is_set():
            try:
                self.tick()
            except StorageError as exc:
                self.logger.critical("storage_failure", f"{exc}; stopping to avoid unpersisted destruction")
                self._alert("storage_failure", f"State persistence failed: {exc}", critical=True)
                self._stop.set()
            except Exception as exc:  # keep the daemon alive no matter what
                self.logger.error("tick_failed", f"unhandled error: {exc}", exc_info=str(exc))
                self._stop.wait(30)
        try:
            self._save()
        except Exception:
            pass
        self.logger.info("engine_stop", "engine stopped")

    def stop(self) -> None:
        self._stop.set()

    def _install_signal_handlers(self) -> None:
        def _handler(signum, frame):  # noqa: ANN001
            self.logger.info("signal", f"received signal {signum}; shutting down")
            self._stop.set()

        try:
            signal.signal(signal.SIGTERM, _handler)
            signal.signal(signal.SIGINT, _handler)
        except (ValueError, OSError):
            # not in the main thread (tests / embedded use)
            pass

    def tick(self) -> None:
        now = utcnow()
        self.state["last_tick"] = iso(now)
        self.state["counters"] = self.state.get("counters") or {}
        if self.telegram.enabled:
            self.telegram.sent_count = int(self.state["counters"].get("telegram_sent", 0))
        self._poll_telegram(block=True)
        self.state["counters"]["telegram_sent"] = self.telegram.sent_count
        self._check_heartbeat_file()

        phase = self.state.get("phase")
        if self.state.get("abort_requested") and phase == PHASE_TEARDOWN:
            self._abort_teardown("abort requested")
        elif phase == PHASE_MONITORING:
            self._monitoring_tick()
        elif phase == PHASE_TEARDOWN:
            self._teardown_tick()
        elif phase == PHASE_DONE:
            self._done_tick()
        elif phase == PHASE_ABORTED:
            self.state["phase"] = PHASE_MONITORING
            self._monitoring_tick()

        self._save()
        if not self.telegram.enabled:
            self._stop.wait(max(1, min(self.config.tick_seconds, 10)))

    # ============================================================== telegram
    def _poll_telegram(self, block: bool = True, timeout: Optional[int] = None) -> bool:
        if not self.telegram.enabled:
            return False
        offset = int(self.state.get("telegram_offset", 0) or 0)
        poll_timeout = self.config.telegram_poll_timeout if block else 0
        if timeout is not None:
            poll_timeout = timeout
        updates = self.telegram.get_updates(offset=offset, timeout=poll_timeout)
        changed = False
        for update in updates:
            update_id = update.get("update_id")
            try:
                if update_id is not None and int(update_id) >= offset:
                    offset = int(update_id) + 1
                    changed = True
            except (TypeError, ValueError):
                pass
            message = update.get("message") or update.get("edited_message") or {}
            if not message:
                continue
            if not self.telegram.is_authorized(message):
                self.logger.warning(
                    "telegram_unauthorized",
                    "ignoring message from unauthorized chat",
                    chat_id=(message.get("chat") or {}).get("id"),
                )
                continue
            text = self.telegram.extract_text(update) or ""
            try:
                self._handle_message(text, message)
            except TeardownAborted:
                raise
            except Exception as exc:
                self.logger.error("telegram_handler_failed", str(exc))
        if changed:
            self.state["telegram_offset"] = offset
            self._dirty = True
        self._last_telegram_poll = time.time()
        return changed

    def _poll_during_long_operation(self) -> None:
        """Called from API poll loops so /abort works during image builds."""
        if not self.telegram.enabled:
            if self.state.get("abort_requested"):
                raise TeardownAborted()
            return
        # Throttle: at most once every 3 seconds.
        if time.time() - self._last_telegram_poll < 3:
            return
        self._poll_telegram(block=False, timeout=0)
        if self.state.get("abort_requested"):
            raise TeardownAborted()

    def _handle_message(self, text: str, message: dict) -> None:
        command, argument = TelegramClient.parse_command(text)
        phase = self.state.get("phase")

        if command in ("/abort", "/cancel", "/stop"):
            if phase == PHASE_TEARDOWN:
                if self.state.get("main_delete_issued_at"):
                    self._reply("Too late — the delete of the main server has already been issued.")
                else:
                    self._request_abort("operator requested /abort")
            else:
                self._reply("Nothing to abort; teardown is not running.")
                self._confirm_online("abort command")
            return

        if phase == PHASE_TEARDOWN:
            if self.config.abort_on_message_during_teardown:
                self._request_abort("operator messaged during teardown")
                return
            # Otherwise still treat it as proof of life, but keep tearing down?
            # Safer default is handled above; explicit config can keep going.
            self._confirm_online("message during teardown")
            self._reply("Message received — but teardown is already in progress.")
            return

        if command == "/status":
            self._reply(self.status_text())
            self._confirm_online("/status", reply=False)
            return
        if command == "/help":
            self._reply(self.help_text())
            self._confirm_online("/help", reply=False)
            return
        if command == "/dryrun":
            self._reply("Running pre-flight against Hetzner Cloud (read-only)…")
            self._reply(self.dry_run_report())
            self._confirm_online("/dryrun", reply=False)
            return
        if command in ("/alive", "/ping", "/yes", "/ok", "/reset"):
            self._confirm_online(command)
            return

        # Any other text is also proof of life (the user was told: just reply).
        self._confirm_online("message")

    def _reply(self, text: str) -> None:
        if self.telegram.enabled:
            self.telegram.send(text, retries=1)

    def _confirm_online(self, reason: str, reply: bool = True) -> None:
        now = utcnow()
        self.state["last_confirmed_online_at"] = iso(now)
        self.state["deadline_at"] = iso(now + timedelta(days=self.config.grace_period_days))
        self.state["last_checkin_sent_at"] = iso(now)
        self.state["last_checkin_attempt_at"] = iso(now)
        self.state["warnings_sent"] = 0
        self.state["milestones_sent"] = []
        self.state["deadline_seen_at"] = None
        self.state["phase"] = PHASE_MONITORING if self.state.get("phase") in (PHASE_ABORTED,) else self.state.get("phase")
        self._dirty = True
        self.logger.info("checkin_received", f"check-in received ({reason})", reason=reason)
        if reply and self.state.get("phase") == PHASE_MONITORING:
            days = self.config.grace_period_days
            self._reply(
                "✅ Check-in received. Timer reset.\n"
                f"Next deadline: {self._fmt_dt(self.state['deadline_at'])} "
                f"({self._days_label(days)} from now).\n"
                "You're all set — no news is good news."
            )
        self.store.event(
            "checkin",
            run_id=self.state.get("run_id", ""),
            payload={"reason": reason, "deadline_at": self.state["deadline_at"]},
        )

    def _check_heartbeat_file(self) -> None:
        path = self.config.heartbeat_file
        if not path:
            return
        try:
            p = Path(path)
            if not p.exists():
                return
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=utcnow().tzinfo)
            last = parse_iso(self.state.get("last_confirmed_online_at"))
            if last is None or mtime > last + timedelta(seconds=1):
                self.state["last_confirmed_online_at"] = iso(mtime)
                self.state["deadline_at"] = iso(mtime + timedelta(days=self.config.grace_period_days))
                self.state["milestones_sent"] = []
                self.logger.info("heartbeat_file", "heartbeat file touched; timer reset")
                self._dirty = True
        except OSError:
            pass

    # ============================================================ monitoring
    def _monitoring_tick(self) -> None:
        now = utcnow()
        deadline = parse_iso(self.state.get("deadline_at"))
        if deadline is None:
            deadline = now + timedelta(days=self.config.grace_period_days)
            self.state["deadline_at"] = iso(deadline)

        self._maybe_send_checkin(now, deadline)
        self._maybe_send_milestones(now, deadline)

        trigger = Path(self.config.state_dir) / "trigger"
        if trigger.exists():
            self.logger.warning("manual_trigger", "trigger file found; starting teardown")
            try:
                trigger.unlink()
            except OSError:
                pass
            self._begin_teardown("manual trigger file")
            return

        if now >= deadline:
            seen = parse_iso(self.state.get("deadline_seen_at"))
            if seen is None:
                # Require two consecutive ticks past the deadline so a single
                # clock jump cannot destroy the project.
                self.state["deadline_seen_at"] = iso(now)
                self._dirty = True
                self.logger.warning("deadline_seen", "deadline passed; confirming next tick")
                return
            self._begin_teardown(
                f"no check-in since {self.state.get('last_confirmed_online_at')}"
            )

    def _maybe_send_checkin(self, now: datetime, deadline: datetime) -> None:
        if not self.telegram.enabled or now >= deadline:
            return
        interval = float(self.config.checkin_interval_hours) * 3600
        last_sent = parse_iso(self.state.get("last_checkin_sent_at"))
        last_attempt = parse_iso(self.state.get("last_checkin_attempt_at"))
        due = last_sent is None or (now - last_sent).total_seconds() >= interval
        if not due:
            return
        # If sending fails, retry at most every 5 minutes rather than every tick.
        if last_attempt and (now - last_attempt).total_seconds() < 300:
            return
        remaining = (deadline - now).total_seconds()
        urgency = "⚠️ FINAL WARNING\n" if remaining < 24 * 3600 else ""
        text = (
            f"{urgency}🫀 hetzner-deadman check-in\n\n"
            "Reply to this message with anything (for example /alive) to confirm "
            "you are online.\n\n"
            f"Deadline: {self._fmt_dt(iso(deadline))}\n"
            f"Time left: {humanize_duration(remaining)}\n\n"
            "If no check-in is received, the rest of the project is cleaned up, a "
            "final live snapshot is taken, and this server is deleted last."
        )
        self.state["last_checkin_attempt_at"] = iso(now)
        ok = self.telegram.send(text, retries=1)
        if ok:
            self.state["last_checkin_sent_at"] = iso(now)
            self.state["warnings_sent"] = int(self.state.get("warnings_sent", 0)) + 1
            self.logger.info("checkin_sent", "daily check-in sent", remaining=int(remaining))
        self._dirty = True

    def _maybe_send_milestones(self, now: datetime, deadline: datetime) -> None:
        if not self.telegram.enabled or now >= deadline:
            return
        remaining = (deadline - now).total_seconds()
        sent = set(self.state.get("milestones_sent") or [])
        for threshold, label in MILESTONES:
            if remaining <= threshold and label not in sent:
                self.telegram.send_async(
                    f"⏳ hetzner-deadman: {humanize_duration(remaining)} until self-destruction.\n"
                    "Reply with anything to cancel the countdown."
                )
                sent.add(label)
                self.state["milestones_sent"] = sorted(sent)
                self._dirty = True
                self.logger.warning("milestone", f"{label} warning sent", remaining=int(remaining))
                break

    # ============================================================== teardown
    def _begin_teardown(self, reason: str) -> None:
        now = utcnow()
        self.state["phase"] = PHASE_TEARDOWN
        self.state["teardown_started_at"] = iso(now)
        self.state["teardown_finished_at"] = None
        self.state["step_index"] = 0
        self.state["steps"] = [_new_step(name, now) for name in STEPS]
        self.state["main_server_deleted"] = False
        self.state["abort_requested"] = False
        self.state["deadline_seen_at"] = None
        self._dirty = True
        try:
            self._ensure_targets()
        except Exception as exc:  # API may be down; the notify step still runs
            self.state["inventory_error"] = str(exc)
            self.logger.warning("inventory_at_trigger_failed", str(exc))
        self.logger.critical(
            "teardown_triggered", f"TEARDOWN TRIGGERED: {reason}", reason=reason
        )
        self._save()
        self._alert("teardown_triggered", f"Teardown triggered: {reason}", critical=True)
        # First step will send the detailed Telegram notice.

    def _teardown_tick(self) -> None:
        if self.state.get("abort_requested"):
            self._abort_teardown("abort requested")
            return
        steps = self.state.get("steps") or []
        # Execute as many due steps as possible per tick, bounded.
        for _ in range(len(STEPS) + 2):
            idx = int(self.state.get("step_index", 0))
            if idx >= len(steps):
                self._finish_teardown()
                return
            step = steps[idx]
            status = step.get("status")
            if status in ("succeeded", "skipped", "abandoned"):
                self.state["step_index"] = idx + 1
                self._dirty = True
                continue
            next_at = parse_iso(step.get("next_attempt_at")) or utcnow()
            if utcnow() < next_at:
                return
            try:
                self._execute_step(step)
            except TeardownAborted:
                self._abort_teardown("aborted by operator")
                return
            if step.get("status") in ("succeeded", "skipped"):
                self.state["step_index"] = idx + 1
                self._dirty = True
                self._save()
                continue
            # Retryable failure: wait for the next hourly attempt.
            self._save()
            return

    def _request_abort(self, reason: str) -> None:
        """Request an abort; unwinds the current step if one is running."""
        if self.state.get("main_delete_issued_at"):
            self._reply("Too late — the main server deletion has already been issued.")
            return
        self.state["abort_requested"] = True
        self._dirty = True
        if self._in_step:
            # Raising here unwinds wait_action/wait_image_available immediately;
            # the outer tick loop performs the actual abort.
            raise TeardownAborted()
        self._abort_teardown(reason)

    def _execute_step(self, step: dict) -> None:
        name = step["name"]
        now = utcnow()
        step["status"] = "running"
        step["attempts"] = int(step.get("attempts", 0)) + 1
        step["last_attempt_at"] = iso(now)
        step["started_at"] = step.get("started_at") or iso(now)
        if step.get("first_attempt_at") is None:
            step["first_attempt_at"] = iso(now)
        self._in_step = True
        self.logger.info(
            "step_start", f"step {name} starting", step=name, attempt=step["attempts"]
        )
        self.store.event(
            "step_start",
            run_id=self.state.get("run_id", ""),
            step=name,
            attempt=step["attempts"],
        )
        try:
            handler = getattr(self, f"_step_{name.lower()}")
            detail = handler(step) or {}
            step["detail"] = detail
            step["status"] = "succeeded"
            step["finished_at"] = iso(utcnow())
            step["next_attempt_at"] = None
            step["last_error"] = None
            self.logger.info(
                "step_success", f"step {name} succeeded", step=name, attempt=step["attempts"]
            )
            self.store.event(
                "step_result",
                run_id=self.state.get("run_id", ""),
                step=name,
                attempt=step["attempts"],
                payload={"status": "succeeded", "detail": detail},
            )
        except SnapshotGateError as exc:
            step["status"] = "skipped"
            step["abandoned_reason"] = str(exc)
            step["finished_at"] = iso(utcnow())
            step["next_attempt_at"] = None
            self.logger.critical("step_gated", f"step {name} skipped: {exc}", step=name)
            self._alert("snapshot_gate", str(exc), critical=True)
        except TeardownAborted:
            step["status"] = "failed_retryable"
            step["next_attempt_at"] = iso(utcnow())
            step["last_error"] = {
                "code": "aborted",
                "message": "aborted by operator",
                "class": "TRANSIENT",
            }
            raise
        except StorageError:
            # Never continue destructing when state cannot be persisted.
            raise
        except Exception as exc:  # noqa: BLE001 - every error is a retryable error
            self._step_failed(step, exc)
        finally:
            self._in_step = False

    def _step_failed(self, step: dict, exc: Exception) -> None:
        now = utcnow()
        if isinstance(exc, HCloudError):
            err = exc.to_dict()
        else:
            err = {
                "code": exc.__class__.__name__,
                "message": str(exc),
                "class": "UNKNOWN",
            }
        step["last_error"] = err
        if step.get("first_error_at") is None:
            step["first_error_at"] = iso(now)
            step["first_failure_at"] = iso(now)
        first_error = parse_iso(step.get("first_error_at")) or now
        elapsed = (now - first_error).total_seconds()
        budget = float(self.config.max_retry_hours) * 3600
        exhausted = elapsed >= budget
        too_many = int(step.get("attempts", 0)) >= int(self.config.max_step_attempts)
        self.logger.error(
            "step_failed",
            f"step {step['name']} failed: {err.get('code')}: {err.get('message')}",
            step=step["name"],
            attempt=step.get("attempts"),
            elapsed_seconds=int(elapsed),
            class_=err.get("class"),
        )
        self.store.event(
            "step_result",
            run_id=self.state.get("run_id", ""),
            step=step["name"],
            attempt=step.get("attempts"),
            payload={"status": "failed", "error": err, "elapsed_seconds": int(elapsed)},
        )
        self.state.setdefault("errors", []).append(
            {"ts": iso(now), "step": step["name"], "error": err}
        )
        self.state["errors"] = self.state["errors"][-50:]

        if exhausted or too_many:
            reason = (
                f"{int(elapsed // 3600)}h retry window exhausted "
                f"({step.get('attempts')} attempts): {err.get('code')}"
            )
            step["status"] = "skipped" if self.config.skip_step_after_max_retry_hours else "abandoned"
            step["abandoned_reason"] = reason
            step["finished_at"] = iso(now)
            step["next_attempt_at"] = None
            self.logger.critical(
                "step_skipped",
                f"step {step['name']} skipped after retry budget: {reason}",
                step=step["name"],
            )
            self._alert(
                "step_abandoned",
                f"Step {step['name']} gave up after {int(elapsed // 3600)}h: "
                f"{err.get('code')} — {err.get('message')}. Advancing to next step.",
                critical=True,
            )
            # step_index advances in _teardown_tick because status is skipped.
        else:
            step["status"] = "failed_retryable"
            step["next_attempt_at"] = iso(now + timedelta(seconds=self.config.retry_interval_seconds))
            self.logger.warning(
                "step_retry_scheduled",
                f"step {step['name']} will retry in {self.config.retry_interval_seconds}s",
                step=step["name"],
                next_attempt_at=step["next_attempt_at"],
            )
            self._alert(
                "step_retry",
                f"Step {step['name']} failed ({err.get('code')}); retrying in "
                f"{humanize_duration(self.config.retry_interval_seconds)}.",
                critical=False,
            )
        self._dirty = True

    def _finish_teardown(self) -> None:
        if self.state.get("phase") == PHASE_DONE and self.state.get("finalized"):
            return
        now = utcnow()
        self.state["phase"] = PHASE_DONE
        self.state["finalized"] = True
        self.state["teardown_finished_at"] = iso(now)
        self._dirty = True
        self._save()
        deleted = self.state.get("main_server_deleted")
        self.logger.critical(
            "teardown_complete",
            "teardown complete" if deleted else "teardown finished without deleting main server",
            main_server_deleted=deleted,
        )
        self._alert(
            "teardown_complete",
            "Teardown complete. Main server deleted." if deleted else
            "Teardown finished, but the main server was NOT deleted (see step details).",
            critical=True,
        )
        self.telegram.send_async(
            "☠️ hetzner-deadman: teardown finished.\n"
            + ("Main server deleted. Goodbye." if deleted else "Main server NOT deleted; see /status.")
        )

    # ------------------------------------------------------------- step impl
    def _step_notify_teardown_start(self, step: dict) -> dict:
        summary = self._inventory_summary()
        text = (
            "☠️ hetzner-deadman: SELF-DESTRUCTION STARTED\n\n"
            f"Triggered: {self._fmt_dt(iso())}\n"
            f"Reason: no check-in for {self.config.grace_period_days:g} days.\n\n"
            f"{summary}\n\n"
            "Order: floating IPs → other servers → other Primary IPs → live "
            "snapshot of main → billing → preserve main IP → delete main server.\n"
            "Send /abort NOW to stop before the main server is deleted."
        )
        # Brief retry, then move on regardless: Telegram must never block teardown.
        if self.telegram.enabled:
            self.telegram.send(text, retries=2)
        self.logger.critical("teardown_notify", "teardown start notification sent")
        return {"telegram_sent": self.telegram.enabled}

    def _step_delete_floating_ips(self, step: dict) -> dict:
        self._ensure_targets()
        floating_ips = self.client.list_floating_ips()
        deleted: list[int] = []
        errors: list[dict] = []
        for fip in floating_ips:
            try:
                if self._delete_one_floating_ip(fip):
                    deleted.append(int(fip["id"]))
            except TeardownAborted:
                raise
            except Exception as exc:
                errors.append({"id": fip.get("id"), "error": str(exc)})
                self.logger.error("fip_delete_failed", str(exc), floating_ip_id=fip.get("id"))
        if errors:
            raise HCloudError(
                f"{len(errors)} floating IP(s) could not be deleted: {errors[:3]}",
                code="floating_ip_delete_incomplete",
                classification="TRANSIENT",
            )
        return {"deleted": deleted, "count": len(deleted)}

    def _delete_one_floating_ip(self, fip: dict) -> bool:
        ip_id = int(fip["id"])
        current = self.client.get_floating_ip(ip_id)
        if current is None:
            return False
        if (current.get("protection") or {}).get("delete"):
            action = self.client.change_floating_ip_protection(ip_id, delete=False)
            self.client.wait_action(action)
        if current.get("server") is not None:
            action = self.client.unassign_floating_ip(ip_id)
            self.client.wait_action(action)
        try:
            self.client.delete_floating_ip(ip_id)
        except HCloudError as exc:
            if exc.code == "must_be_unassigned" or exc.status == 422:
                action = self.client.unassign_floating_ip(ip_id)
                self.client.wait_action(action)
                self.client.delete_floating_ip(ip_id)
            else:
                raise
        if not self.client.dry_run and self.client.get_floating_ip(ip_id) is not None:
            raise HCloudError(
                f"floating IP {ip_id} still exists after delete",
                code="floating_ip_still_exists",
                classification="TRANSIENT",
            )
        self.logger.info("fip_deleted", "floating IP deleted", floating_ip_id=ip_id)
        return True

    def _step_delete_other_servers(self, step: dict) -> dict:
        self._ensure_targets()
        main_id = self._main_id()
        servers = self.client.list_servers(self.config.label_selector)
        deleted: list[int] = []
        errors: list[dict] = []
        for server in servers:
            sid = int(server["id"])
            if sid == main_id:
                continue
            try:
                if self._delete_one_other_server(sid):
                    deleted.append(sid)
            except TeardownAborted:
                raise
            except Exception as exc:
                errors.append({"id": sid, "error": str(exc)})
                self.logger.error("server_delete_failed", str(exc), server_id=sid)
        if errors:
            raise HCloudError(
                f"{len(errors)} server(s) could not be deleted: {errors[:3]}",
                code="server_delete_incomplete",
                classification="TRANSIENT",
            )
        return {"deleted": deleted, "count": len(deleted)}

    def _delete_one_other_server(self, server_id: int) -> bool:
        main_id = self._main_id()
        assert server_id != main_id, "refusing to delete the main server in the other-server step"
        current = self.client.get_server(server_id)
        if current is None:
            return False
        attached = self._server_primary_ip_ids(current)
        reserved = set(int(i) for i in self.state.get("protected_ip_ids") or [])
        reserved_addrs = set(str(a) for a in self.state.get("protected_ips") or [])
        re_protect: list[int] = []
        for ip_id in attached:
            ip = self.client.get_primary_ip(ip_id)
            if ip is None:
                continue
            protection = bool((ip.get("protection") or {}).get("delete"))
            if ip_id in reserved or str(ip.get("ip", "")) in reserved_addrs:
                # The IP must survive while the server it is attached to dies.
                # Protection blocks the delete (T2), so disable it now and
                # re-enable it once the server is gone.
                if protection:
                    self.client.wait_action(self.client.change_primary_ip_protection(ip_id, False))
                if ip.get("auto_delete"):
                    self.client.update_primary_ip(ip_id, auto_delete=False)
                re_protect.append(ip_id)
            else:
                if protection:
                    self.client.wait_action(self.client.change_primary_ip_protection(ip_id, False))
                if not ip.get("auto_delete"):
                    # Flipping auto_delete is only an optimisation: it lets the
                    # IP die with the server in this single call.  A 422 here
                    # (e.g. the IP is in a state that forbids auto_delete) must
                    # NEVER block deleting the server — the very next step,
                    # DELETE_OTHER_PRIMARY_IPS, deletes leftovers explicitly.
                    try:
                        self.client.update_primary_ip(ip_id, auto_delete=True)
                    except HCloudError as exc:
                        self.logger.warning(
                            "other_ip_auto_delete_failed",
                            f"could not set auto_delete=true on primary IP {ip_id} ({exc}); "
                            "it will be deleted explicitly by DELETE_OTHER_PRIMARY_IPS",
                            primary_ip_id=ip_id,
                        )

        server_protection = current.get("protection") or {}
        if server_protection.get("delete") or server_protection.get("rebuild"):
            self.client.wait_action(
                self.client.change_server_protection(server_id, delete=False, rebuild=False)
            )
        try:
            self.client.delete_server(server_id)
        except HCloudError as exc:
            if exc.status == 404:
                return False
            # Any 4xx on server delete: clear protection on the server and its
            # attached IPs, then retry once (SPEC residual-risk #1).
            if exc.status and 400 <= exc.status < 500:
                self.logger.warning(
                    "server_delete_corrective", f"delete failed ({exc}); clearing protections",
                    server_id=server_id,
                )
                try:
                    self.client.wait_action(
                        self.client.change_server_protection(server_id, delete=False, rebuild=False)
                    )
                except HCloudError:
                    pass
                try:
                    self.client.ensure_off(server_id)
                except HCloudError:
                    pass
                for ip_id in attached:
                    try:
                        self.client.wait_action(
                            self.client.change_primary_ip_protection(ip_id, False)
                        )
                        self.client.update_primary_ip(
                            ip_id, auto_delete=(ip_id not in reserved)
                        )
                        if ip_id in reserved:
                            # Now that the server is off, detach the reserved IP
                            # rather than relying on the server-delete detach.
                            self.client.wait_action(self.client.unassign_primary_ip(ip_id))
                    except HCloudError:
                        pass
                self.client.delete_server(server_id)
            else:
                raise
        if not self.client.dry_run:
            self._wait_server_gone(server_id)
        for ip_id in re_protect:
            try:
                self._protect_if_unassigned(ip_id)
            except HCloudError as exc:
                self.logger.warning("reprotect_failed", str(exc), primary_ip_id=ip_id)
        self.logger.info("other_server_deleted", "server deleted", server_id=server_id)
        return True

    def _wait_server_gone(self, server_id: int, timeout: float = 180.0) -> None:
        deadline = time.time() + timeout
        delay = max(0.05, min(float(self.config.action_poll_seconds), 10.0))
        while time.time() < deadline:
            if self.client.get_server(server_id) is None:
                return
            self.client._call_poll_hook()
            time.sleep(delay)
        raise HCloudError(
            f"server {server_id} still exists after delete",
            code="server_still_exists",
            classification="TRANSIENT",
        )

    def _step_delete_other_primary_ips(self, step: dict) -> dict:
        self._ensure_targets()
        main_id = self._main_id()
        reserved_ids = set(int(i) for i in self.state.get("protected_ip_ids") or [])
        reserved_addrs = set(str(a) for a in self.state.get("protected_ips") or [])
        primary_ips = self.client.list_primary_ips()
        main_server = self.client.get_server(main_id)
        main_attached_ids: set[int] = set(
            self._server_primary_ip_ids(main_server) if main_server else []
        )
        main_attached_ips: set[str] = set()
        if main_server:
            public_net = main_server.get("public_net") or {}
            for key in ("ipv4", "ipv6"):
                net = public_net.get(key) or {}
                if net.get("ip"):
                    main_attached_ips.add(str(net["ip"]))
        deleted: list[int] = []
        skipped: list[int] = []
        errors: list[dict] = []
        for ip in primary_ips:
            ip_id = int(ip["id"])
            # Hard denylist: never, under any circumstance, delete a reserved IP
            # or one that belongs to the live main server.
            if ip_id in reserved_ids or ip_id in main_attached_ids:
                skipped.append(ip_id)
                continue
            assert ip_id not in reserved_ids, f"BUG: attempted to delete protected primary IP {ip_id}"
            if str(ip.get("ip", "")) in reserved_addrs or str(ip.get("ip", "")) in main_attached_ips:
                skipped.append(ip_id)
                continue
            if ip.get("assignee_id") is not None and int(ip["assignee_id"]) == main_id:
                # Attached to the main server.  Unassigning requires powering it
                # off, which would kill this software.  It is handled by the
                # main server deletion (auto_delete is set in PREPARE).
                skipped.append(ip_id)
                continue
            try:
                self._delete_one_primary_ip(ip)
                deleted.append(ip_id)
            except TeardownAborted:
                raise
            except Exception as exc:
                errors.append({"id": ip_id, "error": str(exc)})
                self.logger.error("primary_ip_delete_failed", str(exc), primary_ip_id=ip_id)
        if errors:
            raise HCloudError(
                f"{len(errors)} primary IP(s) could not be deleted: {errors[:3]}",
                code="primary_ip_delete_incomplete",
                classification="TRANSIENT",
            )
        return {"deleted": deleted, "skipped_attached_to_main": skipped, "count": len(deleted)}

    def _delete_one_primary_ip(self, ip: dict) -> bool:
        ip_id = int(ip["id"])
        if ip_id in set(int(i) for i in self.state.get("protected_ip_ids") or []):
            raise HCloudError(
                f"refusing to delete reserved primary IP {ip_id}",
                code="refuse_delete_protected_ip",
                classification="PERMANENT",
            )
        current = self.client.get_primary_ip(ip_id)
        if current is None:
            return False
        if (current.get("protection") or {}).get("delete"):
            self.client.wait_action(self.client.change_primary_ip_protection(ip_id, False))
        assignee = current.get("assignee_id")
        if assignee is not None:
            if int(assignee) == self._main_id():
                return False
            self._ensure_off_with_lb_recovery(int(assignee))
            try:
                self.client.wait_action(self.client.unassign_primary_ip(ip_id))
            except HCloudError as exc:
                if exc.code == "server_is_load_balancer_target":
                    self.client.remove_lb_targets_for_server(int(assignee))
                    self.client.wait_action(self.client.unassign_primary_ip(ip_id))
                elif exc.code == "server_not_stopped":
                    self.client.ensure_off(int(assignee))
                    self.client.wait_action(self.client.unassign_primary_ip(ip_id))
                else:
                    raise
        try:
            self.client.delete_primary_ip(ip_id)
        except HCloudError as exc:
            if exc.code == "must_be_unassigned" or exc.status == 422:
                self.client.wait_action(self.client.unassign_primary_ip(ip_id))
                self.client.delete_primary_ip(ip_id)
            else:
                raise
        if not self.client.dry_run and self.client.get_primary_ip(ip_id) is not None:
            raise HCloudError(
                f"primary IP {ip_id} still exists after delete",
                code="primary_ip_still_exists",
                classification="TRANSIENT",
            )
        self.logger.info("primary_ip_deleted", "primary IP deleted", primary_ip_id=ip_id)
        return True

    def _ensure_off_with_lb_recovery(self, server_id: int) -> None:
        if server_id == self._main_id():
            raise HCloudError(
                "refusing to power off main server", code="refuse_poweroff_main", classification=PERMANENT
            )
        try:
            self.client.ensure_off(server_id)
        except HCloudError as exc:
            if exc.code == "server_is_load_balancer_target":
                self.client.remove_lb_targets_for_server(server_id)
                self.client.ensure_off(server_id)
            else:
                raise

    def _step_prepare_main_primary_ip(self, step: dict) -> dict:
        self._ensure_targets()
        details = self._prepare_main_ips(for_deletion=True)
        self.state["main_ip_prepared"] = details
        self._dirty = True
        return details

    def _prepare_main_ips(self, for_deletion: bool = True) -> dict[str, Any]:
        """Ensure reserved Primary IPs survive the deletion of the main server.

        While a Primary IP is attached to the main server, delete protection
        must be OFF: Hetzner refuses to delete a server that has a protected
        Primary IP (T2).  ``auto_delete=false`` is what makes the IP survive
        the server deletion; protection is re-enabled immediately afterwards
        (and by the post-delete loop if this process survives).
        """
        main_id = self._main_id()
        reserved = set(int(i) for i in self.state.get("protected_ip_ids") or [])
        if not reserved:
            # Refuse *before* mutating anything (e.g. before flipping
            # auto_delete on the server's other Primary IPs).
            raise HCloudError(
                "no protected Primary IP IDs resolved; refusing destructive teardown",
                code="preflight_no_protected_ip",
                classification=PERMANENT,
            )
        server = self.client.get_server(main_id)
        details: dict[str, Any] = {"prepared": [], "for_deletion": for_deletion, "missing": []}
        for ip_id in sorted(reserved):
            ip = self.client.get_primary_ip(ip_id)
            if ip is None:
                details["missing"].append(ip_id)
                self.logger.critical(
                    "main_ip_missing", f"reserved primary IP {ip_id} does not exist", primary_ip_id=ip_id
                )
                # Do not delete the main server while a reserved IP cannot be
                # verified: if it actually still exists with auto_delete=true,
                # the server deletion would destroy it.
                raise HCloudError(
                    f"reserved primary IP {ip_id} could not be read; refusing to continue",
                    code="main_ip_missing",
                    classification="TRANSIENT",
                )
            record = {
                "id": ip_id,
                "ip": ip.get("ip"),
                "assignee_id": ip.get("assignee_id"),
                "auto_delete_before": ip.get("auto_delete"),
                "protected_before": bool((ip.get("protection") or {}).get("delete")),
            }
            if ip.get("auto_delete"):
                self.client.update_primary_ip(ip_id, auto_delete=False)
                record["auto_delete_set_false"] = True
            assigned_to_main = ip.get("assignee_id") is not None and int(ip["assignee_id"]) == main_id
            protection = bool((ip.get("protection") or {}).get("delete"))
            if for_deletion:
                if assigned_to_main and protection:
                    # MUST disable: otherwise DELETE /servers/{main} is refused.
                    self.client.wait_action(
                        self.client.change_primary_ip_protection(ip_id, False)
                    )
                    record["protection_disabled_for_delete"] = True
                elif not assigned_to_main and not protection:
                    # Already detached: safe to protect the reserved IP now.
                    self.client.wait_action(
                        self.client.change_primary_ip_protection(ip_id, True)
                    )
                    record["protection_enabled"] = True
            else:
                # Abort path: restore protection when possible.
                if not protection:
                    try:
                        self.client.wait_action(
                            self.client.change_primary_ip_protection(ip_id, True)
                        )
                        record["protection_restored"] = True
                    except HCloudError as exc:
                        record["protection_restore_error"] = str(exc)
            if self.client.dry_run:
                record["auto_delete_after"] = False
                record["protected_after"] = bool(protection and not assigned_to_main)
                record["assignee_after"] = ip.get("assignee_id")
                details["prepared"].append(record)
                continue
            verified = self.client.get_primary_ip(ip_id) or {}
            record["auto_delete_after"] = verified.get("auto_delete")
            record["protected_after"] = bool((verified.get("protection") or {}).get("delete"))
            record["assignee_after"] = verified.get("assignee_id")
            if verified.get("auto_delete"):
                raise HCloudError(
                    f"primary IP {ip_id} still has auto_delete=true after update",
                    code="auto_delete_not_cleared",
                    classification="TRANSIENT",
                )
            details["prepared"].append(record)
            self.logger.info(
                "main_ip_prepared",
                f"reserved primary IP {ip_id} prepared",
                primary_ip_id=ip_id,
                **{k: v for k, v in record.items() if k not in ("id", "ip")},
            )

        # Any non-reserved Primary IP attached to the main server must die with
        # it; unassigning is impossible without powering the machine off.  This
        # is a *destructive* preparation, so it must not run on the abort path.
        if server and for_deletion:
            for ip_id in self._server_primary_ip_ids(server):
                if ip_id in reserved:
                    continue
                ip = self.client.get_primary_ip(ip_id)
                if ip is None:
                    continue
                if (ip.get("protection") or {}).get("delete"):
                    self.client.wait_action(
                        self.client.change_primary_ip_protection(ip_id, False)
                    )
                if not ip.get("auto_delete"):
                    self.client.update_primary_ip(ip_id, auto_delete=True)
                details.setdefault("non_reserved_main_ips", []).append(ip_id)
        details["snapshot_gate_required"] = bool(self.config.require_snapshot_before_main_delete)
        details["main_server_exists"] = server is not None
        return details

    def _step_snapshot_main_server(self, step: dict) -> dict:
        self._ensure_targets()
        main_id = self._main_id()
        server = self.client.get_server(main_id)
        if server is None:
            self.logger.warning("snapshot_skipped", "main server no longer exists")
            return {"skipped": "main_server_gone"}
        if server.get("status") == "deleting":
            self.logger.warning("snapshot_skipped", "main server is already deleting")
            return {"skipped": "main_server_deleting"}

        snap = self.state.setdefault("snapshot", {})
        image_id = snap.get("image_id")
        if image_id:
            image = self.client.get_image(int(image_id))
            if image is not None:
                status = image.get("status")
                if status == "available":
                    self._protect_snapshot(int(image_id), snap)
                    snap["status"] = "available"
                    return {"image_id": image_id, "reused": True, "status": status}
                if status == "creating":
                    self.client.wait_image_available(int(image_id))
                    self._protect_snapshot(int(image_id), snap)
                    snap["status"] = "available"
                    return {"image_id": image_id, "reused": True, "status": "available"}
            self.logger.warning(
                "snapshot_previous_failed",
                f"previous snapshot {image_id} unusable ({image.get('status') if image else 'gone'}); creating a new one",
            )
            self.state.setdefault("snapshot_history", []).append(dict(snap))
            self.state["snapshot"] = {}
            snap = self.state["snapshot"]

        description = f"{self.config.snapshot_description_prefix}-{utcnow().strftime('%Y%m%dT%H%M%SZ')}"
        image, action = self.client.create_snapshot(
            main_id, description, {"deadman": "preserved", "deadman-run": self.state["run_id"].lower()}
        )
        image_id = int(image.get("id") or 0)
        snap.update(
            {
                "image_id": image_id,
                "action_id": (action or {}).get("id"),
                "description": description,
                "status": image.get("status") or "creating",
                "created_at": iso(),
                "live": True,
                "server_id": main_id,
            }
        )
        self.state["snapshot"] = snap
        self._dirty = True
        self._save()  # persist the image id before polling, so a crash cannot orphan it
        self.logger.critical(
            "snapshot_created",
            f"live snapshot {image_id} of running main server {main_id} started",
            image_id=image_id,
            server_id=main_id,
        )
        if self.client.dry_run:
            snap["status"] = "available"
            return {"image_id": image_id, "dry_run": True}
        if (action or {}).get("id"):
            self.client.wait_action(action["id"], timeout=self.config.image_timeout_seconds)
        image = self.client.wait_image_available(image_id)
        snap["status"] = image.get("status", "available")
        snap["image_size_gb"] = image.get("image_size")
        snap["disk_size_gb"] = image.get("disk_size")
        self._protect_snapshot(image_id, snap)
        self._dirty = True
        self._save()
        return {"image_id": image_id, "status": snap["status"], "description": description}

    def _protect_snapshot(self, image_id: int, snap: dict) -> None:
        if not self.config.protect_snapshot or self.client.dry_run:
            return
        action = self.client.protect_image(image_id, delete=True)
        if action:
            self.client.wait_action(action)
        image = self.client.get_image(image_id)
        protected = bool(((image or {}).get("protection") or {}).get("delete"))
        if not protected:
            # One retry, then surface as a retryable step failure.
            action = self.client.protect_image(image_id, delete=True)
            if action:
                self.client.wait_action(action)
            image = self.client.get_image(image_id)
            protected = bool(((image or {}).get("protection") or {}).get("delete"))
        if not protected:
            raise HCloudError(
                f"snapshot {image_id} is not delete-protected",
                code="snapshot_not_protected",
                classification="TRANSIENT",
            )
        snap["protected"] = True

    def _step_settle_billing(self, step: dict) -> dict:
        try:
            result = self.billing.run(self.state)
        except Exception as exc:  # BillingStep already guards; belt and braces.
            self.logger.warning("billing_engine_guard", str(exc))
            result = {"status": "skipped", "mode": self.config.billing_mode, "error": str(exc)}
        self.state["billing"] = result
        self._dirty = True
        try:
            self.telegram.send_async(
                "💳 Billing step: no Hetzner payment API exists; account credit is "
                "applied automatically. Proceeding to delete the main server."
            )
        except Exception:
            pass
        return result

    def _step_delete_main_server(self, step: dict) -> dict:
        self._ensure_targets()
        main_id = self._main_id()
        server = self.client.get_server(main_id)
        if server is None:
            # Already gone (perhaps a previous process died right after the
            # DELETE was accepted).  The server is deleted; mark it as such.
            self.state["main_server_deleted"] = True
            self.state["main_delete_issued_at"] = self.state.get("main_delete_issued_at") or iso()
            self.state["phase"] = PHASE_DONE
            self.state["teardown_finished_at"] = self.state.get("teardown_finished_at") or iso()
            self._dirty = True
            self._save()
            self._post_delete_reprotect(server_id=main_id)
            return {"already_deleted": True}

        snap = self.state.get("snapshot") or {}
        snapshot_ok = snap.get("status") == "available" and bool(snap.get("image_id"))
        if snapshot_ok and not self.client.dry_run:
            image = self.client.get_image(int(snap["image_id"]))
            if image is None or image.get("status") != "available":
                snapshot_ok = False
                self.logger.critical(
                    "snapshot_missing_before_delete",
                    "recorded snapshot is no longer available",
                    image_id=snap.get("image_id"),
                )
        force_file = Path(self.config.state_dir) / "force_proceed"
        if self.config.require_snapshot_before_main_delete and not snapshot_ok and not force_file.exists():
            raise SnapshotGateError(
                "SNAPSHOT_MAIN_SERVER did not produce an available snapshot and "
                "REQUIRE_SNAPSHOT_BEFORE_MAIN_DELETE=true; refusing to delete the main server. "
                f"Create {force_file} to override."
            )
        if not snapshot_ok:
            self.logger.critical(
                "deleting_without_snapshot",
                "no available snapshot; deleting main server anyway (configured default)",
            )

        # Re-assert the IP preparation right before the irreversible call.
        self.state["main_ip_prepared"] = self._prepare_main_ips(for_deletion=True)

        protection = server.get("protection") or {}
        if protection.get("delete") or protection.get("rebuild"):
            self.client.wait_action(
                self.client.change_server_protection(main_id, delete=False, rebuild=False)
            )

        ips = ", ".join(
            f"{p.get('ip')} (id {p.get('id')})"
            for p in (self.state.get("main_ip_prepared", {}).get("prepared") or [])
        ) or "n/a"
        goodbye = (
            "💀 hetzner-deadman: deleting the main server now.\n\n"
            f"Snapshot: {snap.get('image_id', 'none')} ({snap.get('status', 'n/a')})\n"
            f"Preserved Primary IP(s) (auto_delete=false): {ips}\n"
            f"Main server: {server.get('name')} (id {main_id})\n\n"
            "Goodbye."
        )
        # Last message before the plug is pulled: give it a short, bounded try.
        if self.telegram.enabled:
            self.telegram.send(goodbye, retries=0, timeout=8)

        self.state["main_delete_attempted_at"] = iso()
        self.state["phase"] = PHASE_TEARDOWN
        self._dirty = True
        self._save()

        try:
            self.client.delete_server(main_id)
        except HCloudError as exc:
            if exc.status == 404:
                pass  # already gone: success
            elif exc.status and 400 <= exc.status < 500:
                # Most likely a protected Primary IP or protected server. Clear
                # both and retry once; if it still fails, the step retries in an
                # hour and PREPARE will run again.
                self.logger.warning(
                    "main_delete_corrective", f"delete failed ({exc}); clearing protections"
                )
                try:
                    self.client.wait_action(
                        self.client.change_server_protection(main_id, delete=False, rebuild=False)
                    )
                except HCloudError:
                    pass
                for ip_id in self.state.get("protected_ip_ids") or []:
                    try:
                        self.client.update_primary_ip(int(ip_id), auto_delete=False)
                        self.client.wait_action(
                            self.client.change_primary_ip_protection(int(ip_id), False)
                        )
                    except HCloudError:
                        pass
                try:
                    self.client.delete_server(main_id)
                except HCloudError:
                    self._revert_failed_main_delete()
                    raise
            else:
                self._revert_failed_main_delete()
                raise

        # ------------------------------------------------------------------
        # The DELETE was accepted.  Record the irreversible fact FIRST and
        # persist it before any further network round-trip: this process runs
        # ON the server that is now being destroyed, and Hetzner detaches the
        # Primary IP and powers the VM off within seconds.  Waiting for
        # GET /servers/{main} to return 404 before writing state (or before
        # re-protecting the IP) means neither ever happens.
        # ------------------------------------------------------------------
        self.state["main_delete_issued_at"] = self.state.get("main_delete_issued_at") or iso()
        self.state["main_server_deleted"] = True
        self.state["phase"] = PHASE_DONE
        self.state["teardown_finished_at"] = iso()
        self._dirty = True
        self._save()
        self.logger.critical(
            "main_server_delete_issued", "DELETE /servers accepted; state persisted", server_id=main_id
        )

        # Combined fast poll (1 s, max 120 s): watch the server disappear AND
        # re-enable delete protection on each reserved Primary IP the instant
        # Hetzner detaches it.  auto_delete is already false, so the IP is safe
        # even if this process is killed before the protection call lands.
        self._post_delete_reprotect(server_id=main_id)
        self.telegram.send_async("✅ Main server deleted. State: DONE.")
        return {"deleted": True, "snapshot": snap.get("image_id")}

    def _revert_failed_main_delete(self) -> None:
        """Roll the state back when DELETE /servers/{main} was refused."""
        self.state["phase"] = PHASE_TEARDOWN
        self.state["main_server_deleted"] = False
        self.state["main_delete_issued_at"] = None
        self._dirty = True
        self.logger.warning(
            "main_delete_reverted",
            "main server delete failed; reverted to TEARDOWN for a retry",
        )

    def _post_delete_reprotect(self, timeout: float = 120.0, server_id: Optional[int] = None) -> None:
        """Re-enable delete protection on reserved IPs once they detach.

        Runs a single combined poll loop: every second it checks
        ``GET /primary_ips/{id}`` for every reserved IP *and*
        ``GET /servers/{main}``.  As soon as an IP reports
        ``assignee_id is None`` its protection is restored immediately — the
        loop does not wait for the server to reach 404 first (it never will
        while this process is alive: the VM dies first).
        """
        if self.client.dry_run:
            return
        ids = [int(i) for i in self.state.get("protected_ip_ids") or []]
        if not ids and server_id is None:
            return
        deadline = time.time() + float(timeout)
        pending = set(ids)
        watch_server = server_id is not None
        server_gone_at: Optional[float] = None
        if not watch_server and not pending:
            return

        while True:
            if watch_server:
                try:
                    if self.client.get_server(int(server_id)) is None:  # type: ignore[arg-type]
                        self.state["main_delete_confirmed_at"] = (
                            self.state.get("main_delete_confirmed_at") or iso()
                        )
                        watch_server = False
                        server_gone_at = time.time()
                except HCloudError as exc:
                    self.logger.warning("main_delete_confirm_failed", str(exc))

            for ip_id in list(pending):
                try:
                    ip = self.client.get_primary_ip(ip_id)
                except HCloudError as exc:
                    self.logger.warning("reprotect_poll_failed", str(exc), primary_ip_id=ip_id)
                    continue
                if ip is None:
                    self.logger.critical(
                        "main_ip_missing_after_delete",
                        f"reserved primary IP {ip_id} disappeared after the server delete",
                        primary_ip_id=ip_id,
                    )
                    pending.discard(ip_id)
                    continue
                if ip.get("assignee_id") is not None:
                    # If the server itself is already gone, an assignee that
                    # still points at it is a stale read: Hetzner detaches
                    # Primary IPs as part of the deletion, so this cannot last.
                    stale = (
                        server_gone_at is not None
                        and int(ip["assignee_id"]) == int(server_id)  # type: ignore[arg-type]
                        and time.time() - server_gone_at > 5.0
                    )
                    if stale:
                        self.logger.critical(
                            "main_ip_stale_assignee",
                            f"primary IP {ip_id} still reports the deleted server as assignee; "
                            "leaving it to the DONE-tick invariant repair",
                            primary_ip_id=ip_id,
                        )
                        pending.discard(ip_id)
                    continue
                try:
                    if ip.get("auto_delete"):
                        self.client.update_primary_ip(ip_id, auto_delete=False)
                    if not (ip.get("protection") or {}).get("delete"):
                        self.client.wait_action(
                            self.client.change_primary_ip_protection(ip_id, True)
                        )
                    verified = self.client.get_primary_ip(ip_id) or {}
                    if (verified.get("protection") or {}).get("delete") and not verified.get(
                        "auto_delete"
                    ):
                        self.logger.info(
                            "main_ip_reprotected",
                            "reserved primary IP re-protected after detach",
                            primary_ip_id=ip_id,
                        )
                        pending.discard(ip_id)
                except HCloudError as exc:
                    self.logger.warning("reprotect_failed", str(exc), primary_ip_id=ip_id)

            if not pending:
                # Every reserved IP is verifiably protected and unassigned.
                # Confirming the server's own disappearance is best effort: the
                # VM is being destroyed underneath us right now, so waiting for
                # a 404 that may never be observable only delays DONE.
                return
            if time.time() >= deadline:
                break
            time.sleep(1.0)

        if pending:
            self.logger.warning(
                "reprotect_timeout",
                f"re-protection still pending for primary IP(s) {sorted(pending)}; "
                "auto_delete=false keeps them safe and DONE-tick repair will retry",
                primary_ip_ids=sorted(pending),
            )
        elif watch_server:
            self.logger.warning(
                "main_delete_unconfirmed",
                "server still reports as existing after the poll window; state is DONE",
            )

    # ================================================================== done
    def _done_tick(self) -> None:
        if not self.state.get("finalized"):
            self._finish_teardown()
        # Verify the invariant at most every 6 hours.
        last = parse_iso(self.state.get("last_verify_at"))
        if last and (utcnow() - last).total_seconds() < 6 * 3600:
            return
        self.state["last_verify_at"] = iso()
        if self.state.get("main_server_deleted"):
            self._repair_main_ip_invariant()
        report = self.verify_postconditions()
        self.state["last_verify"] = report
        self._dirty = True
        if report.get("violations"):
            self.logger.critical(
                "invariant_violation", "post-run invariant violated", report=report
            )
            self._alert(
                "invariant_violation",
                f"Post-run invariant violated: {report['violations']}",
                critical=True,
            )
        else:
            self.logger.info("invariant_ok", "post-run invariant holds", report=report)

    def _repair_main_ip_invariant(self) -> None:
        """Best-effort repair after DONE: auto_delete=false + delete protection."""
        if self.client.dry_run or not self.config.token:
            return
        for ip_id in self.state.get("protected_ip_ids") or []:
            try:
                ip = self.client.get_primary_ip(int(ip_id))
                if ip is None:
                    continue
                if ip.get("auto_delete"):
                    self.client.update_primary_ip(int(ip_id), auto_delete=False)
                if ip.get("assignee_id") is None and not (ip.get("protection") or {}).get("delete"):
                    self.client.wait_action(
                        self.client.change_primary_ip_protection(int(ip_id), True)
                    )
                    self.logger.warning(
                        "invariant_repaired",
                        "re-enabled delete protection on reserved primary IP",
                        primary_ip_id=ip_id,
                    )
            except HCloudError as exc:
                self.logger.warning("invariant_repair_failed", str(exc), primary_ip_id=ip_id)

    def verify_postconditions(self) -> dict[str, Any]:
        report: dict[str, Any] = {"checked_at": iso(), "violations": [], "main_ips": []}
        if self.client.dry_run or not self.config.token:
            report["skipped"] = "dry_run_or_no_token"
            return report
        main_id = self.state.get("main_server_id")
        if self.state.get("main_server_deleted") and main_id:
            try:
                if self.client.get_server(int(main_id)) is not None:
                    report["violations"].append(f"main server {main_id} still exists")
            except HCloudError as exc:
                report["violations"].append(f"main server check failed: {exc}")
        for ip_id in self.state.get("protected_ip_ids") or []:
            try:
                ip = self.client.get_primary_ip(int(ip_id), allow_missing=True)
            except HCloudError as exc:
                report["violations"].append(f"primary IP {ip_id}: {exc}")
                continue
            if ip is None:
                report["violations"].append(f"primary IP {ip_id} is MISSING")
                continue
            item = {
                "id": ip_id,
                "ip": ip.get("ip"),
                "assignee_id": ip.get("assignee_id"),
                "auto_delete": ip.get("auto_delete"),
                "protected": bool((ip.get("protection") or {}).get("delete")),
            }
            report["main_ips"].append(item)
            if item["auto_delete"]:
                report["violations"].append(f"primary IP {ip_id} has auto_delete=true")
            # While the main server still exists (snapshot gate), protection is
            # intentionally off so the server remains deletable.
            if not self.state.get("main_server_deleted"):
                continue
            if item["assignee_id"] is not None:
                report["violations"].append(f"primary IP {ip_id} still assigned")
            if not item["protected"]:
                report["violations"].append(f"primary IP {ip_id} is not delete-protected")
        return report

    # ============================================================== helpers
    def _main_id(self) -> int:
        main_id = self.state.get("main_server_id") or self.config.main_server_id
        if not main_id:
            raise HCloudError(
                "main server id is not resolved",
                code="main_server_unresolved",
                classification=PERMANENT,
            )
        return int(main_id)

    @staticmethod
    def _server_primary_ip_ids(server: dict) -> list[int]:
        public_net = server.get("public_net") or {}
        ids: list[int] = []
        for key in ("ipv4", "ipv6"):
            net = public_net.get(key) or {}
            if net.get("id"):
                ids.append(int(net["id"]))
        return list(dict.fromkeys(ids))

    def _ensure_targets(self) -> None:
        """Resolve and persist the main server and the reserved Primary IPs.

        This must be robust against *partially* configured deployments: a user
        may set only ``MAIN_SERVER_ID``, only ``MAIN_PRIMARY_IP`` (an address,
        not an ID), only ``MAIN_PRIMARY_IP_ID``, or a subset of the project.
        Previously an early return when ``main_id and reserved`` were both set
        skipped ``resolve_protected_ip_ids`` entirely, so:

        * an address-form ``MAIN_PRIMARY_IP`` never got mapped to an ID,
        * the server's own ``public_net`` IP IDs were never unioned in,
        * ``state["protected_ips"]`` stayed empty (so address denylists failed),
        * ``state["targets_resolved"]`` was never set (so every step re-resolved).

        It now always resolves once, unions with anything already in state, and
        refuses to continue if the protected set is empty: silently deleting an
        IP that cannot be recreated is the one unrecoverable bug.
        """
        main_id = self.state.get("main_server_id") or self.config.main_server_id
        if self.config.main_server_id:
            self.client.main_server_id = int(self.config.main_server_id)
        elif main_id:
            self.client.main_server_id = int(main_id)

        already = (
            bool(self.state.get("targets_resolved"))
            and bool(main_id)
            and bool(self.state.get("protected_ip_ids"))
        )
        if already:
            self.state["main_server_id"] = int(main_id)
            self.config.main_server_id = int(main_id)
            self.client.main_server_id = int(main_id)
            return

        try:
            server = self.client.detect_main_server()
        except HCloudError as exc:
            # Crash-safety: if the main server is already gone (a previous
            # process got the DELETE accepted and died before persisting DONE)
            # but the reserved Primary IP IDs are known, keep going with the
            # known targets instead of wedging forever on a 404.
            if exc.code == "main_server_not_found" and main_id and self.state.get(
                "protected_ip_ids"
            ):
                self.logger.warning(
                    "main_server_gone_targets_kept",
                    "main server is already gone; using persisted protected IP set",
                    main_server_id=int(main_id),
                )
                self.state["main_server_id"] = int(main_id)
                self.state["targets_resolved"] = True
                self.config.main_server_id = int(main_id)
                self.client.main_server_id = int(main_id)
                self._dirty = True
                return
            raise
        if server is None:
            raise HCloudError(
                "no servers found in this project (wrong token or empty project?)",
                code="no_servers",
                classification=TRANSIENT,
            )
        main_id = int(server["id"])
        primary_ips = self.client.list_primary_ips(self.config.label_selector)
        resolved_ids = [int(i) for i in self.client.resolve_protected_ip_ids(server, primary_ips)]
        existing_ids = [int(i) for i in (self.state.get("protected_ip_ids") or [])]
        protected = list(dict.fromkeys(existing_ids + resolved_ids))
        if not protected:
            raise HCloudError(
                "could not resolve any main Primary IP to preserve; refusing to delete anything",
                code="preflight_no_protected_ip",
                classification=PERMANENT,
            )

        protected_set = set(protected)
        addresses = [
            str(ip.get("ip")) for ip in primary_ips if int(ip.get("id", 0)) in protected_set
        ]
        extra = [str(a) for a in (self.state.get("protected_ips") or [])]
        self.state["protected_ip_ids"] = protected
        self.state["protected_ips"] = list(dict.fromkeys(addresses + extra))
        self.state["main_server_id"] = main_id
        self.state["main_server_name"] = server.get("name")
        self.state["targets_resolved"] = True
        self.config.main_server_id = main_id
        self.client.main_server_id = main_id
        self._capture_inventory(server, primary_ips)
        self._dirty = True
        self._save()
        self.logger.info(
            "targets_resolved",
            "main server and protected Primary IPs resolved",
            main_server_id=main_id,
            main_server_name=server.get("name"),
            protected_ip_ids=protected,
        )

    def _capture_inventory(self, server: dict, primary_ips: list[dict]) -> None:
        try:
            servers = self.client.list_servers(self.config.label_selector)
            floating = self.client.list_floating_ips()
        except HCloudError as exc:
            self.logger.warning("inventory_incomplete", str(exc))
            servers = [server]
            floating = []
        self.state["inventory"] = {
            "captured_at": iso(),
            "main_server_id": int(server["id"]),
            "main_server_name": server.get("name"),
            "protected_ip_ids": list(self.state.get("protected_ip_ids") or []),
            "protected_ips": list(self.state.get("protected_ips") or []),
            "servers": [
                {
                    "id": s.get("id"),
                    "name": s.get("name"),
                    "status": s.get("status"),
                    "server_type": (s.get("server_type") or {}).get("name"),
                    "primary_ip_ids": self._server_primary_ip_ids(s),
                }
                for s in servers
            ],
            "primary_ips": [
                {
                    "id": p.get("id"),
                    "ip": p.get("ip"),
                    "type": p.get("type"),
                    "assignee_id": p.get("assignee_id"),
                    "auto_delete": p.get("auto_delete"),
                    "protected": bool((p.get("protection") or {}).get("delete")),
                }
                for p in primary_ips
            ],
            "floating_ips": [
                {
                    "id": f.get("id"),
                    "ip": f.get("ip"),
                    "server": f.get("server"),
                    "protected": bool((f.get("protection") or {}).get("delete")),
                }
                for f in floating
            ],
        }

    def _inventory_summary(self) -> str:
        inv = self.state.get("inventory") or {}
        if not inv:
            return "Inventory not yet captured (Hetzner API unavailable at trigger time)."
        servers = inv.get("servers") or []
        ips = inv.get("primary_ips") or []
        fips = inv.get("floating_ips") or []
        protected = inv.get("protected_ip_ids") or []
        others = [s for s in servers if s.get("id") != inv.get("main_server_id")]
        return (
            f"Main server: {inv.get('main_server_name')} (id {inv.get('main_server_id')})\n"
            f"Preserved Primary IPs: {', '.join(str(i) for i in protected) or 'none'}\n"
            f"Other servers: {len(others)} | Other Primary IPs: "
            f"{max(0, len(ips) - len(protected))} | Floating IPs: {len(fips)}"
        )

    def _protect_if_unassigned(self, ip_id: int) -> None:
        ip = self.client.get_primary_ip(ip_id)
        if ip is None:
            return
        if ip.get("assignee_id") is None and not (ip.get("protection") or {}).get("delete"):
            if ip.get("auto_delete"):
                self.client.update_primary_ip(ip_id, auto_delete=False)
            self.client.wait_action(self.client.change_primary_ip_protection(ip_id, True))

    def _abort_teardown(self, reason: str) -> None:
        if self.state.get("main_delete_issued_at"):
            self.logger.critical("abort_too_late", "abort requested after main delete was issued")
            self._reply("Too late: the main server deletion has already been issued.")
            return
        now = utcnow()
        self.logger.critical("teardown_aborted", f"teardown aborted: {reason}", reason=reason)
        self.state["phase"] = PHASE_MONITORING
        self.state["abort_requested"] = False
        self.state["teardown_aborted_at"] = iso(now)
        self.state["step_index"] = 0
        self.state["steps"] = []
        self.state["deadline_seen_at"] = None
        # Restore protection on the main IP where possible.
        try:
            if self.client and self.config.token and not self.client.dry_run:
                self._prepare_main_ips(for_deletion=False)
        except Exception as exc:
            self.logger.warning("abort_reprotect_failed", str(exc))
        self._confirm_online("teardown aborted", reply=False)
        self._save()
        self.telegram.send(
            "🛑 hetzner-deadman: teardown ABORTED before the main server was deleted.\n"
            f"Reason: {reason}\n"
            "Timer reset. Resources already deleted stay deleted.",
            retries=1,
        )
        self.store.event("teardown_aborted", run_id=self.state.get("run_id", ""), payload={"reason": reason})

    def _alert(self, event: str, message: str, critical: bool = False) -> None:
        level = "CRITICAL" if critical else "WARNING"
        self.logger.log(level, f"alert:{event}", message)
        self.telegram.send_async(f"{'🚨' if critical else '⚠️'} {event}: {message}")

    # -------------------------------------------------------------- status
    def status_text(self) -> str:
        s = self.state
        phase = s.get("phase", "?")
        deadline = s.get("deadline_at")
        remaining = seconds_until(deadline) if deadline else None
        lines = [
            "🫀 hetzner-deadman status",
            f"Phase: {phase}",
            f"Last check-in: {s.get('last_confirmed_online_at') or 'never'}",
        ]
        if remaining is not None:
            lines.append(
                f"Deadline: {deadline} ({humanize_duration(remaining)} left)"
                if remaining > 0
                else f"Deadline: {deadline} (PASSED)"
            )
        lines += [
            f"Check-in every: {self.config.checkin_interval_hours:g}h",
            f"Grace period: {self.config.grace_period_days:g}d",
            f"Dry run: {self.config.dry_run}",
            f"Main server: {s.get('main_server_name') or 'auto'} "
            f"(id {s.get('main_server_id') or self.config.main_server_id or 'auto'})",
            "Protected Primary IPs: "
            + (", ".join(str(i) for i in s.get("protected_ip_ids") or []) or "resolved at teardown")
            + (
                f" ({', '.join(str(a) for a in s.get('protected_ips') or [])})"
                if s.get("protected_ips")
                else ""
            ),
        ]
        snap = s.get("snapshot") or {}
        if snap.get("image_id"):
            lines.append(f"Snapshot: {snap.get('image_id')} ({snap.get('status')})")
        if phase == PHASE_TEARDOWN:
            idx = int(s.get("step_index", 0))
            steps = s.get("steps") or []
            if 0 <= idx < len(steps):
                step = steps[idx]
                lines.append(
                    f"Current step: {step.get('name')} (attempt {step.get('attempts')}, "
                    f"status {step.get('status')}, next {step.get('next_attempt_at')})"
                )
                if step.get("last_error"):
                    err = step["last_error"]
                    lines.append(f"Last error: {err.get('code')}: {err.get('message')}")
            lines.append(f"Abort window open: {not bool(s.get('main_delete_issued_at'))}")
        if s.get("main_server_deleted"):
            lines.append("Main server deleted: yes")
        return "\n".join(lines)

    def help_text(self) -> str:
        return (
            "hetzner-deadman commands:\n"
            "/alive — confirm you are online and reset the timer (any message works)\n"
            "/status — show phase, deadline and teardown progress\n"
            "/dryrun — read-only pre-flight: what would be preserved / deleted\n"
            "/abort — stop a running teardown before the main server is deleted\n"
            "/help — this message\n\n"
            f"Grace period: {self.config.grace_period_days:g} days. "
            "No check-in → final live snapshot, then the server deletes itself."
        )

    def dry_run_report(self) -> str:
        try:
            return self._build_dry_run_report()
        except HCloudError as exc:
            return f"Pre-flight failed: {exc.code or exc.status}: {exc}"
        except Exception as exc:  # noqa: BLE001
            return f"Pre-flight failed: {exc}"

    def _build_dry_run_report(self) -> str:
        if not self.config.token:
            return "Pre-flight failed: HCLOUD_TOKEN is not set."
        server = self.client.detect_main_server()
        if server is None:
            return "Pre-flight failed: no servers in this project."
        primary_ips = self.client.list_primary_ips(self.config.label_selector)
        servers = self.client.list_servers(self.config.label_selector)
        floating = self.client.list_floating_ips()
        protected = self.client.resolve_protected_ip_ids(server, primary_ips)
        protected_set = set(protected)
        main_id = int(server["id"])
        lines = [
            "🧪 hetzner-deadman DRY RUN (read-only, nothing will change)",
            f"Main server: {server.get('name')} (id {main_id}), status {server.get('status')}",
            f"Main server protection: {server.get('protection')}",
            "",
            "Would PRESERVE:",
        ]
        if not protected_set:
            lines.append("  ⚠️  NONE — teardown will refuse to delete anything!")
        for ip in primary_ips:
            if int(ip["id"]) in protected_set:
                lines.append(
                    f"  • Primary IP {ip.get('ip')} (id {ip.get('id')}, {ip.get('type')}, "
                    f"auto_delete={ip.get('auto_delete')}, protected="
                    f"{(ip.get('protection') or {}).get('delete')})"
                )
        lines.append("  • snapshot of main server (live, delete-protected)")
        lines.append("")
        lines.append("Would DELETE:")
        others = [s for s in servers if int(s["id"]) != main_id]
        if others:
            for s in others:
                lines.append(f"  • server {s.get('name')} (id {s.get('id')}, {s.get('status')})")
        else:
            lines.append("  • (no other servers)")
        other_ips = [p for p in primary_ips if int(p["id"]) not in protected_set]
        if other_ips:
            for p in other_ips:
                lines.append(
                    f"  • Primary IP {p.get('ip')} (id {p.get('id')}, assignee {p.get('assignee_id')})"
                )
        else:
            lines.append("  • (no other Primary IPs)")
        if floating:
            for f in floating:
                lines.append(f"  • Floating IP {f.get('ip')} (id {f.get('id')}, server {f.get('server')})")
        else:
            lines.append("  • (no Floating IPs)")
        lines += [
            "",
            f"Billing: mode={self.config.billing_mode} (Hetzner applies credit automatically)",
            f"Retry: every {self.config.retry_interval_seconds}s for up to {self.config.max_retry_hours:g}h per step",
            f"Snapshot required before delete: {self.config.require_snapshot_before_main_delete}",
        ]
        return "\n".join(lines)

    @staticmethod
    def _fmt_dt(value: Optional[str]) -> str:
        dt = parse_iso(value)
        if dt is None:
            return "unknown"
        return dt.strftime("%Y-%m-%d %H:%M UTC")

    @staticmethod
    def _days_label(days: float) -> str:
        if float(days) == 1:
            return "1 day"
        return f"{days:g} days"
