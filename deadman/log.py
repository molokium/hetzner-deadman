"""Structured JSON-lines logging with size-based rotation.

One JSON object per line, to stderr and (optionally) to ``<state_dir>/deadman.log``.
Rotates at 10 MiB, keeping three older files.  Logging must never take the
process down, so every filesystem operation is guarded.
"""

from __future__ import annotations

import json
import os
import sys
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from .util import redact

LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


class JsonLogger:
    def __init__(
        self,
        path: Optional[str] = None,
        level: str = "INFO",
        also_stderr: bool = True,
        max_bytes: int = 10 * 1024 * 1024,
        backups: int = 3,
    ) -> None:
        self.path = path
        self.level = LEVELS.get(str(level).upper(), 20)
        self.also_stderr = also_stderr
        self.max_bytes = max_bytes
        self.backups = backups
        self._lock = threading.RLock()
        self._fh = None
        if path:
            try:
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                self._open()
            except OSError as exc:  # pragma: no cover - read-only fs
                print(f"logger: cannot open {path}: {exc}", file=sys.stderr)

    def _open(self) -> None:
        if not self.path:
            return
        self._fh = open(self.path, "a", encoding="utf-8", buffering=1)

    def _rotate_if_needed(self) -> None:
        if not self.path or not self._fh:
            return
        try:
            if self._fh.tell() < self.max_bytes:
                return
            self._fh.close()
            for idx in range(self.backups - 1, 0, -1):
                src = f"{self.path}.{idx}"
                dst = f"{self.path}.{idx + 1}"
                if os.path.exists(src):
                    os.replace(src, dst)
            if os.path.exists(self.path):
                os.replace(self.path, f"{self.path}.1")
            self._open()
        except OSError:
            # Reopen defensively; logging must not break the daemon.
            try:
                self._open()
            except OSError:
                self._fh = None

    def log(self, level: str, event: str, msg: str = "", **fields: Any) -> None:
        lvl = str(level).upper()
        if LEVELS.get(lvl, 20) < self.level:
            return
        record = {
            "ts": datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "level": lvl,
            "event": event,
        }
        if msg:
            record["msg"] = redact(msg)
        for key, value in fields.items():
            if value is None:
                continue
            if isinstance(value, (dict, list, tuple)):
                record[key] = json.loads(redact(json.dumps(value, default=str)))
            else:
                record[key] = redact(value)
        line = json.dumps(record, ensure_ascii=False, default=str)
        with self._lock:
            if self.also_stderr:
                try:
                    print(line, file=sys.stderr, flush=True)
                except Exception:
                    pass
            if self._fh:
                try:
                    self._rotate_if_needed()
                    self._fh.write(line + "\n")
                except Exception:
                    pass

    def debug(self, event: str, msg: str = "", **fields: Any) -> None:
        self.log("DEBUG", event, msg, **fields)

    def info(self, event: str, msg: str = "", **fields: Any) -> None:
        self.log("INFO", event, msg, **fields)

    def warning(self, event: str, msg: str = "", **fields: Any) -> None:
        self.log("WARNING", event, msg, **fields)

    def error(self, event: str, msg: str = "", **fields: Any) -> None:
        self.log("ERROR", event, msg, **fields)

    def critical(self, event: str, msg: str = "", **fields: Any) -> None:
        self.log("CRITICAL", event, msg, **fields)

    def close(self) -> None:
        with self._lock:
            if self._fh:
                try:
                    self._fh.close()
                finally:
                    self._fh = None


def null_logger() -> JsonLogger:
    logger = JsonLogger(path=None, level="CRITICAL", also_stderr=False)
    return logger
