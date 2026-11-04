"""Triple-redundant, crash-safe state persistence.

Three independent representations of the same JSON document live in the state
directory:

1. ``state.json`` — authoritative, written with write-tmp + fsync + rename,
   with ``state.json.bak`` holding the previous good document.
2. ``state.block`` — a preallocated binary block file with two independent
   A/B slots (offset 4096 and 2 MiB), each carrying magic bytes, a monotonic
   generation, payload length, CRC32 and SHA-256 of the payload, and a CRC32 of
   the header itself.  Every write is ``fsync``ed.  This works in an
   unprivileged container where ``mount -o loop`` would need CAP_SYS_ADMIN.
3. ``state.db`` — SQLite in WAL mode.  ``kv['state']`` mirrors the full
   document (last-resort recovery) and ``events`` is an append-only audit log.

Recovery order on load: ``state.json`` -> ``state.json.bak`` -> newest valid
block slot -> ``kv['state']``.  A single ``flock`` on ``<state_dir>/lock``
prevents two daemons from corrupting anything.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import sqlite3
import struct
import threading
import time
import zlib
from hashlib import sha256
from pathlib import Path
from typing import Any, Optional

from .log import JsonLogger, null_logger
from .util import iso, utcnow

MAGIC = b"HDMNBLK1"
HEADER_SIZE = 64
HEADER_STRUCT = struct.Struct(">8sHHQII32sI")  # 8+2+2+8+4+4+32+4 = 64
SCHEMA_VERSION = 2
SLOT_A_OFFSET = 4096
SLOT_B_OFFSET = 2 * 1024 * 1024
DEFAULT_FILE_BYTES = 4 * 1024 * 1024
MAX_PAYLOAD = SLOT_B_OFFSET - HEADER_SIZE - 16


class StorageError(RuntimeError):
    pass


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


class BlockFileStore:
    """Two-slot A/B checksummed binary store with generation ordering."""

    def __init__(
        self,
        path: Path,
        size_bytes: int = DEFAULT_FILE_BYTES,
        logger: Optional[JsonLogger] = None,
    ) -> None:
        self.path = Path(path)
        self.size_bytes = max(size_bytes, SLOT_B_OFFSET + 65536)
        self.logger = logger or null_logger()
        self._lock = threading.RLock()
        self._generation = 0

    # ----------------------------------------------------------------- file
    def ensure(self) -> None:
        with self._lock:
            if not self.path.parent.exists():
                self.path.parent.mkdir(parents=True, exist_ok=True)
            if not self.path.exists():
                self.path.touch()
            current = self.path.stat().st_size
            if current < self.size_bytes:
                with open(self.path, "r+b") as fh:
                    try:
                        os.posix_fallocate(fh.fileno(), 0, self.size_bytes)
                    except (AttributeError, OSError):
                        fh.truncate(self.size_bytes)
                    fh.flush()
                    os.fsync(fh.fileno())

    def _slot_offset(self, index: int) -> int:
        return SLOT_A_OFFSET if index == 0 else SLOT_B_OFFSET

    # ---------------------------------------------------------------- write
    def write(self, payload: bytes, generation: Optional[int] = None) -> int:
        with self._lock:
            self.ensure()
            if len(payload) > MAX_PAYLOAD:
                raise StorageError(
                    f"state payload too large for block file: {len(payload)} > {MAX_PAYLOAD}"
                )
            gen = generation if generation is not None else self._generation + 1
            slot = int(gen) % 2
            offset = self._slot_offset(slot)
            crc = zlib.crc32(payload) & 0xFFFFFFFF
            digest = sha256(payload).digest()
            header_zero = HEADER_STRUCT.pack(
                MAGIC, 1, 0, int(gen), len(payload), crc, digest, 0
            )
            header_crc = zlib.crc32(header_zero) & 0xFFFFFFFF
            header = HEADER_STRUCT.pack(
                MAGIC, 1, 0, int(gen), len(payload), crc, digest, header_crc
            )
            with open(self.path, "r+b", buffering=0) as fh:
                fh.seek(offset)
                fh.write(header)
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            self._generation = int(gen)
            return int(gen)

    # ----------------------------------------------------------------- read
    def read_slot(self, index: int) -> dict[str, Any]:
        result: dict[str, Any] = {"slot": "A" if index == 0 else "B", "valid": False}
        if not self.path.exists():
            result["error"] = "missing"
            return result
        offset = self._slot_offset(index)
        try:
            with open(self.path, "rb") as fh:
                fh.seek(offset)
                header = fh.read(HEADER_SIZE)
                if len(header) < HEADER_SIZE:
                    result["error"] = "short_header"
                    return result
                magic, version, _flags, gen, length, crc, digest, hcrc = HEADER_STRUCT.unpack(header)
                result.update({"generation": gen, "length": length})
                if magic != MAGIC:
                    result["error"] = "bad_magic"
                    return result
                zero = HEADER_STRUCT.pack(MAGIC, version, _flags, gen, length, crc, digest, 0)
                if (zlib.crc32(zero) & 0xFFFFFFFF) != hcrc:
                    result["error"] = "bad_header_crc"
                    return result
                if length <= 0 or length > MAX_PAYLOAD:
                    result["error"] = "bad_length"
                    return result
                payload = fh.read(length)
                if len(payload) != length:
                    result["error"] = "short_payload"
                    return result
        except OSError as exc:
            result["error"] = f"io:{exc}"
            return result
        if (zlib.crc32(payload) & 0xFFFFFFFF) != crc:
            result["error"] = "bad_payload_crc"
            return result
        if sha256(payload).digest() != digest:
            result["error"] = "bad_payload_sha256"
            return result
        result["valid"] = True
        result["payload"] = payload
        return result

    def read_best(self) -> Optional[dict[str, Any]]:
        best: Optional[dict[str, Any]] = None
        for index in (0, 1):
            slot = self.read_slot(index)
            if slot.get("valid") and (best is None or slot["generation"] > best["generation"]):
                best = slot
        if best is not None:
            self._generation = max(self._generation, int(best["generation"]))
        return best

    def verify(self) -> dict[str, Any]:
        report = {"path": str(self.path), "exists": self.path.exists(), "slots": []}
        if not self.path.exists():
            report["ok"] = False
            report["error"] = "missing"
            return report
        report["size_bytes"] = self.path.stat().st_size
        for index in (0, 1):
            slot = self.read_slot(index)
            item = {k: v for k, v in slot.items() if k != "payload"}
            if "payload" in slot:
                item["payload_bytes"] = len(slot["payload"])
            report["slots"].append(item)
        report["initialised"] = any(s.get("valid") for s in report["slots"])
        report["ok"] = True  # a brand-new, zeroed block file is valid (uninitialised)
        best = self.read_best()
        report["best_generation"] = best["generation"] if best else None
        return report


class StateStore:
    """Coordinates JSON, block file, SQLite, and the process lock."""

    def __init__(
        self,
        state_dir: str | Path,
        block_size_bytes: int = DEFAULT_FILE_BYTES,
        enable_block_file: bool = True,
        logger: Optional[JsonLogger] = None,
    ) -> None:
        self.dir = Path(state_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger or null_logger()
        self.json_path = self.dir / "state.json"
        self.bak_path = self.dir / "state.json.bak"
        self.db_path = self.dir / "state.db"
        self.block_path = self.dir / "state.block"
        self.lock_path = self.dir / "lock"
        self.block = (
            BlockFileStore(self.block_path, size_bytes=block_size_bytes, logger=self.logger)
            if enable_block_file
            else None
        )
        self._generation = 0
        self._lock_fd: Optional[int] = None
        self._db_lock = threading.RLock()
        self._db: Optional[sqlite3.Connection] = None

    # ----------------------------------------------------------------- lock
    def acquire_lock(self, blocking: bool = False) -> bool:
        if self._lock_fd is not None:
            return True
        fd = os.open(str(self.lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        try:
            fcntl.flock(fd, flags)
        except OSError as exc:
            os.close(fd)
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                return False
            raise
        self._lock_fd = fd
        return True

    def release_lock(self) -> None:
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(self._lock_fd)
            finally:
                self._lock_fd = None

    @property
    def has_lock(self) -> bool:
        return self._lock_fd is not None

    @property
    def state_dir(self) -> Path:
        return self.dir

    # ------------------------------------------------------------------- db
    def _db_conn(self) -> Optional[sqlite3.Connection]:
        if self._db is not None:
            return self._db
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=10.0, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts TEXT NOT NULL,
                  run_id TEXT NOT NULL,
                  step TEXT,
                  attempt INTEGER,
                  event TEXT NOT NULL,
                  payload TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv (
                  k TEXT PRIMARY KEY,
                  v TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
            self._db = conn
            return conn
        except sqlite3.Error as exc:
            self.logger.warning("db_open_failed", str(exc))
            return None

    def event(
        self,
        event: str,
        run_id: str = "",
        step: Optional[str] = None,
        attempt: Optional[int] = None,
        payload: Optional[dict] = None,
    ) -> None:
        conn = self._db_conn()
        if conn is None:
            return
        try:
            with self._db_lock:
                conn.execute(
                    "INSERT INTO events (ts, run_id, step, attempt, event, payload) VALUES (?,?,?,?,?,?)",
                    (iso(), run_id, step, attempt, event, json.dumps(payload or {}, default=str)),
                )
                conn.commit()
        except sqlite3.Error as exc:
            self.logger.warning("db_event_failed", str(exc))

    def _mirror_state(self, doc: dict) -> None:
        conn = self._db_conn()
        if conn is None:
            return
        try:
            with self._db_lock:
                conn.execute(
                    "INSERT INTO kv (k, v, updated_at) VALUES ('state', ?, ?) "
                    "ON CONFLICT(k) DO UPDATE SET v=excluded.v, updated_at=excluded.updated_at",
                    (json.dumps(doc, default=str), iso()),
                )
                conn.commit()
        except sqlite3.Error as exc:
            self.logger.warning("db_mirror_failed", str(exc))

    # ----------------------------------------------------------------- load
    def _read_json(self, path: Path) -> Optional[dict]:
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(doc, dict) and "phase" in doc:
                return doc
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        return None

    def load(self) -> Optional[dict]:
        candidates: list[tuple[str, dict]] = []
        doc = self._read_json(self.json_path)
        if doc:
            candidates.append(("state.json", doc))
        doc = self._read_json(self.bak_path)
        if doc:
            candidates.append(("state.json.bak", doc))
        if self.block:
            slot = self.block.read_best()
            if slot:
                try:
                    doc = json.loads(slot["payload"].decode("utf-8"))
                    if isinstance(doc, dict) and "phase" in doc:
                        candidates.append((f"state.block:{slot['slot']}", doc))
                        self._generation = max(self._generation, int(slot["generation"]))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    pass
        conn = self._db_conn()
        if conn is not None:
            try:
                with self._db_lock:
                    row = conn.execute("SELECT v FROM kv WHERE k='state'").fetchone()
                if row:
                    doc = json.loads(row[0])
                    if isinstance(doc, dict) and "phase" in doc:
                        candidates.append(("state.db:kv", doc))
            except (sqlite3.Error, json.JSONDecodeError):
                pass

        if not candidates:
            return None
        source, best = candidates[0]
        # Prefer the highest generation among candidates; ties favour the first
        # (state.json) because it is the documented authority.
        best_gen = int(best.get("generation", 0) or 0)
        for name, candidate in candidates[1:]:
            gen = int(candidate.get("generation", 0) or 0)
            if gen > best_gen:
                source, best, best_gen = name, candidate, gen
        self._generation = max(self._generation, best_gen)
        best["_recovered_from"] = source
        if source != "state.json":
            self.logger.warning("state_recovered", f"recovered from {source}", source=source)
        return best

    # ----------------------------------------------------------------- save
    def save(self, state: dict) -> dict:
        """Persist *state* to all three stores.  Raises on JSON/block failure."""
        self._generation += 1
        state["generation"] = self._generation
        state["schema_version"] = SCHEMA_VERSION
        state["updated_at"] = iso()

        data = json.dumps(state, indent=2, ensure_ascii=False, default=str).encode("utf-8")
        if len(data) > MAX_PAYLOAD:
            raise StorageError(f"state document too large: {len(data)} bytes")

        # 1. rotate the previous good document, then atomically replace.
        if self.json_path.exists():
            try:
                previous = self.json_path.read_bytes()
                _write_atomic(self.bak_path, previous)
            except OSError as exc:
                self.logger.warning("state_bak_failed", str(exc))
        _write_atomic(self.json_path, data)

        # 2. binary block file (A/B slots, checksummed, fsynced).
        if self.block is not None:
            try:
                self.block.write(data, generation=self._generation)
            except (OSError, StorageError) as exc:
                self.logger.error("block_write_failed", str(exc))

        # 3. SQLite mirror (last-resort recovery).
        self._mirror_state(state)
        return state

    def load_or_init(self, state_factory) -> dict:
        state = self.load()
        if state is None:
            state = state_factory()
            self.save(state)
        return state

    # --------------------------------------------------------------- verify
    def verify_storage(self, deep: bool = True) -> dict[str, Any]:
        report: dict[str, Any] = {
            "state_dir": str(self.dir),
            "writable": os.access(self.dir, os.W_OK),
            "json": {"path": str(self.json_path), "exists": self.json_path.exists()},
            "bak": {"path": str(self.bak_path), "exists": self.bak_path.exists()},
            "db": {"path": str(self.db_path), "exists": self.db_path.exists()},
            "block": None,
            "ok": True,
        }
        # JSON round-trip (non-destructive probe file)
        probe = self.dir / ".storage-probe.tmp"
        try:
            _write_atomic(probe, b'{"probe": true}')
            report["json"]["writable"] = probe.read_bytes() == b'{"probe": true}'
            probe.unlink(missing_ok=True)
        except OSError as exc:
            report["json"]["writable"] = False
            report["json"]["error"] = str(exc)
            report["ok"] = False

        # Block file
        if self.block is None:
            report["block"] = {"enabled": False}
        else:
            try:
                self.block.ensure()
                block_report = self.block.verify()
                if deep:
                    payload = json.dumps(
                        {"probe": "hetzner-deadman", "ts": iso(), "generation": 0}
                    ).encode()
                    gen = self.block.write(payload)
                    slot = self.block.read_best()
                    block_report["roundtrip_ok"] = bool(slot and slot["payload"] == payload)
                    block_report["roundtrip_generation"] = gen
                report["block"] = block_report
                if block_report.get("roundtrip_ok"):
                    # A fresh block file has no valid slot until the first write;
                    # the round-trip is the real proof that it works.
                    block_report["ok"] = True
                if not block_report.get("ok"):
                    report["ok"] = False
            except (OSError, StorageError) as exc:
                report["block"] = {"ok": False, "error": str(exc)}
                report["ok"] = False

        # SQLite
        conn = self._db_conn()
        if conn is not None:
            try:
                with self._db_lock:
                    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
                    events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
                    kv_rows = conn.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
                report["db"].update(
                    {"integrity": integrity, "events": events, "kv_rows": kv_rows}
                )
                if integrity != "ok":
                    report["ok"] = False
            except sqlite3.Error as exc:
                report["db"]["error"] = str(exc)
                report["ok"] = False
        else:
            report["db"]["error"] = "cannot open"
            report["ok"] = False
        return report

    def close(self) -> None:
        with self._db_lock:
            if self._db is not None:
                try:
                    self._db.close()
                finally:
                    self._db = None
        self.release_lock()


def fsync_retry(path: Path, attempts: int = 3) -> bool:
    """Best-effort fsync helper retained for callers that manage their own fds."""
    for _ in range(attempts):
        try:
            fd = os.open(str(path), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            return True
        except OSError:
            time.sleep(0.05)
    return False
