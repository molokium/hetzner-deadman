"""Command line interface.

Subcommands
-----------
``run``             run the daemon (the normal entrypoint)
``status``          print current phase, deadline, teardown progress
``dry-run``         read-only pre-flight: what would be preserved / deleted
``trigger-now``     create <state_dir>/trigger so the daemon starts teardown
``reset-timer``     reset the check-in deadline to now (+ optionally notify)
``verify-storage``  verify JSON / block-file / SQLite persistence
``config``          print the effective, redacted configuration
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

from . import __version__
from .config import Config, ConfigError
from .engine import Engine, PHASE_DONE, PHASE_TEARDOWN
from .log import JsonLogger
from .storage import StateStore
from .telegram import TelegramClient
from .util import humanize_duration, iso, seconds_until, utcnow


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", help="path to config.json / config.yaml")
    parser.add_argument("--state-dir", help="override STATE_DIR (default /data)")
    parser.add_argument("--log-level", help="DEBUG|INFO|WARNING|ERROR|CRITICAL")
    parser.add_argument("--version", action="version", version=f"hetzner-deadman {__version__}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="deadman",
        description=(
            "Hetzner Cloud dead man's switch: daily Telegram check-in, and if you "
            "stop answering, a final live snapshot plus a full project teardown."
        ),
    )
    parser.add_argument("--version", action="version", version=f"hetzner-deadman {__version__}")
    sub = parser.add_subparsers(dest="command", metavar="command")

    run = sub.add_parser("run", help="run the daemon")
    _add_common(run)
    run.add_argument("--dry-run", dest="dry_run", action="store_true", default=None,
                     help="simulate every destructive API call")
    run.add_argument("--live", dest="dry_run", action="store_false",
                     help="force real API calls even if DRY_RUN=true")

    status = sub.add_parser("status", help="show current state")
    _add_common(status)
    status.add_argument("--json", action="store_true", help="print the raw state document")

    dry = sub.add_parser("dry-run", help="read-only Hetzner pre-flight report")
    _add_common(dry)

    trigger = sub.add_parser("trigger-now", help="start teardown on the running daemon")
    _add_common(trigger)
    trigger.add_argument("--wait", action="store_true", help="wait until teardown starts")

    reset = sub.add_parser("reset-timer", help="reset the check-in deadline")
    _add_common(reset)
    reset.add_argument("--notify", action="store_true", help="send a Telegram confirmation")

    verify = sub.add_parser("verify-storage", help="verify the three persistence layers")
    _add_common(verify)

    cfg = sub.add_parser("config", help="print effective configuration")
    _add_common(cfg)

    return parser


def _overrides(args: argparse.Namespace) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if getattr(args, "state_dir", None):
        out["state_dir"] = args.state_dir
    if getattr(args, "log_level", None):
        out["log_level"] = args.log_level
    if getattr(args, "dry_run", None) is not None:
        out["dry_run"] = args.dry_run
    return out


def _load(args: argparse.Namespace, require_token: bool) -> Config:
    return Config.load(
        config_file=getattr(args, "config", None),
        overrides=_overrides(args),
        require_token=require_token,
    )


def _store(cfg: Config) -> StateStore:
    return StateStore(
        cfg.state_dir,
        block_size_bytes=cfg.block_file_bytes,
        enable_block_file=cfg.enable_block_file,
        logger=JsonLogger(cfg.log_file, level=cfg.log_level),
    )


# --------------------------------------------------------------------- run
def cmd_run(args: argparse.Namespace) -> int:
    cfg = _load(args, require_token=True)
    logger = JsonLogger(cfg.log_file, level=cfg.log_level)
    store = StateStore(
        cfg.state_dir,
        block_size_bytes=cfg.block_file_bytes,
        enable_block_file=cfg.enable_block_file,
        logger=logger,
    )
    if not store.acquire_lock():
        logger.warning("already_running", "another hetzner-deadman instance holds the lock; exiting")
        print("already running", file=sys.stderr)
        return 0
    engine = Engine(cfg, store=store, logger=logger)
    logger.info(
        "startup",
        "hetzner-deadman starting",
        version=__version__,
        dry_run=cfg.dry_run,
        state_dir=cfg.state_dir,
    )
    try:
        engine.run()
    finally:
        store.close()
    return 0


# ------------------------------------------------------------------ status
def cmd_status(args: argparse.Namespace) -> int:
    cfg = _load(args, require_token=False)
    store = _store(cfg)
    state = store.load()
    if state is None:
        print("No state found in", cfg.state_dir)
        return 0
    if args.json:
        print(json.dumps(state, indent=2, default=str))
        store.close()
        return 0
    phase = state.get("phase", "?")
    deadline = state.get("deadline_at")
    remaining = seconds_until(deadline) if deadline else None
    print(f"hetzner-deadman {__version__}")
    print(f"phase:            {phase}")
    print(f"last check-in:    {state.get('last_confirmed_online_at') or 'never'}")
    if remaining is None:
        print("deadline:         unknown")
    elif remaining > 0:
        print(f"deadline:         {deadline}  ({humanize_duration(remaining)} left)")
    else:
        print(f"deadline:         {deadline}  (PASSED {humanize_duration(-remaining)} ago)")
    print(f"dry run:          {cfg.dry_run}")
    print(f"main server:      {state.get('main_server_name') or 'auto'} "
          f"(id {state.get('main_server_id') or cfg.main_server_id or 'auto'})")
    print(f"protected IPs:    {state.get('protected_ip_ids') or 'resolved at teardown'}")
    snap = state.get("snapshot") or {}
    if snap:
        print(f"snapshot:         {snap.get('image_id')} ({snap.get('status')})")
    if phase == PHASE_TEARDOWN:
        idx = int(state.get("step_index", 0))
        steps = state.get("steps") or []
        for pos, step in enumerate(steps):
            marker = "->" if pos == idx else "  "
            err = step.get("last_error") or {}
            suffix = f"  last_error={err.get('code')}" if err else ""
            print(
                f"{marker} {step.get('name'):<30} {step.get('status'):<16} "
                f"attempts={step.get('attempts', 0)}{suffix}"
            )
    if state.get("main_server_deleted"):
        print("main deleted:     yes")
    print(f"state dir:        {cfg.state_dir}")
    store.close()
    return 0


# ----------------------------------------------------------------- dry run
def cmd_dry_run(args: argparse.Namespace) -> int:
    cfg = _load(args, require_token=True)
    store = _store(cfg)
    if not store.load():
        # A dry run is read-only, but having an initial state makes /status and
        # the report reproducible; do not overwrite an existing state.
        pass
    engine = Engine(cfg, store=store)
    report = engine.dry_run_report()
    print(report)
    store.close()
    return 0 if not report.startswith("Pre-flight failed") else 1


# -------------------------------------------------------------- trigger-now
def cmd_trigger_now(args: argparse.Namespace) -> int:
    cfg = _load(args, require_token=False)
    Path(cfg.state_dir).mkdir(parents=True, exist_ok=True)
    trigger = Path(cfg.state_dir) / "trigger"
    trigger.write_text(iso() + "\n", encoding="utf-8")
    print(f"trigger file created: {trigger}")
    print("The running daemon will start the teardown on its next tick.")
    if args.wait:
        store = _store(cfg)
        deadline = time.time() + 120
        while time.time() < deadline:
            state = store.load()
            if state and state.get("phase") in (PHASE_TEARDOWN, PHASE_DONE):
                print(f"teardown phase: {state.get('phase')}")
                store.close()
                return 0
            time.sleep(2)
        store.close()
        print("timed out waiting for teardown to start", file=sys.stderr)
        return 1
    return 0


# -------------------------------------------------------------- reset-timer
def cmd_reset_timer(args: argparse.Namespace) -> int:
    cfg = _load(args, require_token=False)
    store = _store(cfg)
    if not store.acquire_lock():
        print("daemon is running; use Telegram /alive or touch the heartbeat file", file=sys.stderr)
        store.close()
        return 1
    try:
        engine = Engine(cfg, store=store)
        state = engine.load_state()
        engine.state = state
        now = utcnow()
        state["last_confirmed_online_at"] = iso(now)
        state["deadline_at"] = iso(now + timedelta(days=cfg.grace_period_days))
        state["last_checkin_sent_at"] = iso(now)
        state["last_checkin_attempt_at"] = iso(now)
        state["milestones_sent"] = []
        state["warnings_sent"] = 0
        state["deadline_seen_at"] = None
        if state.get("phase") in (PHASE_TEARDOWN, PHASE_DONE):
            print(f"note: phase is {state.get('phase')}; teardown is not reset", file=sys.stderr)
        store.save(state)
        print(f"timer reset; next deadline {state['deadline_at']} ({cfg.grace_period_days:g} days)")
        if args.notify:
            tg = TelegramClient(
                cfg.telegram_bot_token, cfg.telegram_chat_id, cfg.telegram_user_id,
                api_base=cfg.telegram_api_base,
            )
            if tg.send("✅ Timer reset manually on the host. You're all set.", retries=1):
                print("telegram notification sent")
            else:
                print("telegram notification failed (or not configured)", file=sys.stderr)
    finally:
        store.close()
    return 0


# ----------------------------------------------------------- verify-storage
def cmd_verify_storage(args: argparse.Namespace) -> int:
    cfg = _load(args, require_token=False)
    store = _store(cfg)
    acquired = store.acquire_lock()
    try:
        report = store.verify_storage(deep=acquired)
        report["exclusive_lock"] = acquired
        if not acquired:
            report["note"] = "daemon holds the lock; skipped destructive round-trip probe"
        print(json.dumps(report, indent=2, default=str))
        return 0 if report.get("ok") else 1
    finally:
        if acquired:
            store.release_lock()
        store.close()


# -------------------------------------------------------------------- config
def cmd_config(args: argparse.Namespace) -> int:
    cfg = _load(args, require_token=False)
    print(json.dumps(cfg.describe(), indent=2, default=str))
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 0
    handlers = {
        "run": cmd_run,
        "status": cmd_status,
        "dry-run": cmd_dry_run,
        "trigger-now": cmd_trigger_now,
        "reset-timer": cmd_reset_timer,
        "verify-storage": cmd_verify_storage,
        "config": cmd_config,
    }
    try:
        return handlers[args.command](args)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except BrokenPipeError:
        return 0
    except Exception as exc:  # keep CLI failures readable
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
