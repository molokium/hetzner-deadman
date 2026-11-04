"""Small shared helpers: time handling, ids, formatting, redaction."""

from __future__ import annotations

import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

UTC = timezone.utc

_BEARER_RE = re.compile(r"(Bearer\s+)[A-Za-z0-9._\-]+", re.IGNORECASE)
_TOKENISH_RE = re.compile(r"\b([A-Za-z0-9]{40,})\b")
_BOT_TOKEN_RE = re.compile(r"(/bot)\d+:[A-Za-z0-9_-]+")


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def iso(dt: Optional[datetime] = None) -> str:
    """RFC3339 / ISO-8601 with a trailing Z, second resolution."""
    dt = dt or utcnow()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        txt = str(value).strip()
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        dt = datetime.fromisoformat(txt)
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except (ValueError, TypeError):
        return None


def seconds_until(value: Any, now: Optional[datetime] = None) -> Optional[float]:
    dt = parse_iso(value)
    if dt is None:
        return None
    return (dt - (now or utcnow())).total_seconds()


def humanize_duration(seconds: float) -> str:
    """Compact human duration, e.g. '6d 3h', '59m', '12s'."""
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h" if hours else f"{days}d"
    if hours:
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    if minutes:
        return f"{minutes}m"
    return f"{secs}s"


def new_run_id() -> str:
    return utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4)


def redact(text: Any) -> str:
    """Strip bearer tokens and long token-looking strings from log lines."""
    s = str(text)
    s = _BEARER_RE.sub(r"\1<redacted>", s)
    s = _BOT_TOKEN_RE.sub(r"\1<redacted>", s)
    return s


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "enable", "enabled"}


def as_int_list(value: Any) -> list[int]:
    """Parse '[1,2]' / '1,2' / '1 2' / list into a list of ints."""
    if value is None or value == "":
        return []
    if isinstance(value, int):
        return [value]
    if isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        text = str(value).strip().strip("[]")
        items = re.split(r"[,\s]+", text) if text else []
    out: list[int] = []
    for item in items:
        if item in ("", None):
            continue
        try:
            out.append(int(str(item).strip().strip("'\"")))
        except (TypeError, ValueError):
            continue
    return out


def as_str_list(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        text = value.strip().strip("[]")
        return [p.strip().strip("'\"") for p in re.split(r"[,\s]+", text) if p.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if str(v).strip()]
    return [str(value)]


def dedupe(items: Iterable[Any]) -> list:
    seen = set()
    out = []
    for item in items:
        key = item
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def truncate(text: str, limit: int = 3500) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 20] + "\n…(truncated)"


def seconds_from_hours(hours: float) -> float:
    return float(hours) * 3600.0


def deadline_after(last: datetime, days: float) -> datetime:
    return last + timedelta(days=float(days))
