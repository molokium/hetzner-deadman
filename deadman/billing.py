"""Billing step — deliberately boring, deliberately unable to fail the run.

Hetzner Cloud and Robot expose **no** invoice/payment/credit endpoints
(verified: zero matches for invoice|payment|billing|credit in the 152-path
Cloud OpenAPI spec).  Outstanding invoices are settled from account credit
automatically, once per day.  There is therefore nothing this program can call
to "pay" a bill; the only options are a user-supplied webhook or command hook.

Whatever happens here — timeout, DNS failure, non-zero exit, HTTP 500, even a
bug in this module — the step is recorded as ``succeeded`` or ``skipped`` and
the teardown proceeds to deleting the main server.  This is the one step that
must never gate the flow.
"""

from __future__ import annotations

import json
import shlex
import subprocess
import urllib.error
import urllib.request
from typing import Any, Optional

from .config import Config
from .log import JsonLogger, null_logger
from .util import iso, truncate

BILLING_EXPLANATION = (
    "Hetzner Cloud has no public API endpoint for paying invoices or topping up "
    "credit. Existing account credit is applied to future invoices automatically, "
    "and outstanding balances are reconciled about once a day. No manual payment "
    "call is possible from this software."
)


class BillingStep:
    def __init__(self, config: Config, logger: Optional[JsonLogger] = None) -> None:
        self.config = config
        self.logger = logger or null_logger()

    def run(self, state: Optional[dict] = None) -> dict[str, Any]:
        state = state or {}
        started = iso()
        result: dict[str, Any] = {
            "status": "skipped",
            "mode": self.config.billing_mode,
            "started_at": started,
            "note": BILLING_EXPLANATION,
            "error": None,
        }
        self.logger.info(
            "billing_start",
            "billing step: no Hetzner payment API exists; credit is applied automatically",
            mode=self.config.billing_mode,
        )
        try:
            if self.config.billing_mode == "webhook":
                result.update(self._webhook(state))
            elif self.config.billing_mode == "command":
                result.update(self._command(state))
            else:
                result["status"] = "skipped"
                result["reason"] = "no_billing_api"
        except Exception as exc:  # noqa: BLE001 - the whole point of this module
            self.logger.warning("billing_hook_failed", str(exc))
            result.update({"status": "skipped", "error": str(exc), "reason": "billing_hook_error"})
        finally:
            result["finished_at"] = iso()
        self.logger.info(
            "billing_done", f"billing step {result['status']}", status=result["status"], mode=result["mode"]
        )
        return result

    # ---------------------------------------------------------------- webhook
    def _webhook(self, state: dict) -> dict[str, Any]:
        url = self.config.billing_webhook_url
        if not url:
            return {"status": "skipped", "reason": "no_webhook_url"}
        payload = {
            "event": "deadman.teardown",
            "run_id": state.get("run_id", ""),
            "phase": "billing",
            "server_deleted": False,
            "timestamp": iso(),
            "note": BILLING_EXPLANATION,
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "hetzner-deadman/1.0"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.config.billing_timeout_seconds) as resp:
                status = resp.status
            ok = 200 <= int(status) < 300
            return {
                "status": "succeeded" if ok else "skipped",
                "http_status": status,
                "reason": None if ok else "webhook_non_2xx",
            }
        except urllib.error.HTTPError as exc:
            try:
                status = exc.code
            finally:
                try:
                    exc.close()
                except Exception:
                    pass
            return {
                "status": "skipped",
                "http_status": status,
                "error": f"HTTP {status}",
                "reason": "webhook_non_2xx",
            }
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return {"status": "skipped", "error": str(exc), "reason": "webhook_failed"}

    # ---------------------------------------------------------------- command
    def _command(self, state: dict) -> dict[str, Any]:
        raw = self.config.billing_command
        if isinstance(raw, str):
            argv = shlex.split(raw)
        else:
            argv = [str(part) for part in raw]
        if not argv:
            return {"status": "skipped", "reason": "no_command"}
        try:
            proc = subprocess.run(
                argv,
                timeout=self.config.billing_timeout_seconds,
                check=False,
                capture_output=True,
                text=True,
                shell=False,
                env=None,
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            return {"status": "skipped", "error": str(exc), "reason": "command_failed"}
        ok = proc.returncode == 0
        return {
            "status": "succeeded" if ok else "skipped",
            "returncode": proc.returncode,
            "stdout": truncate(proc.stdout or "", 1000),
            "stderr": truncate(proc.stderr or "", 1000),
            "reason": None if ok else "command_nonzero",
        }
