"""Configuration loading: defaults <- JSON/YAML file <- environment <- CLI flags.

Env names are accepted both bare (``MAIN_SERVER_ID``) and namespaced
(``DEADMAN_MAIN_SERVER_ID``).  The bare names are what the deployment
documentation uses; the namespaced variants avoid collisions.

Precedence: CLI override > environment > config file > default.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Optional

from .util import as_bool, as_int_list, as_str_list


class ConfigError(RuntimeError):
    pass


#: canonical field -> default value
DEFAULTS: dict[str, Any] = {
    # --- Hetzner API ---
    "token": "",
    "token_file": "",
    "base_url": "https://api.hetzner.cloud/v1",
    "request_timeout_seconds": 30.0,
    # --- target inventory ---
    "main_server_id": None,
    "main_server_name": "",
    "main_primary_ip_ids": [],
    "main_primary_ip": "",
    "protected_ip_ids": [],
    "protected_ips": [],
    "reserve_main_ipv6": True,
    "label_selector": "",
    "expect_min_servers": 0,
    "auto_detect_main_server": True,
    # --- behaviour ---
    "dry_run": False,
    "state_dir": "/data",
    "enable_block_file": True,
    "block_file_size_mb": 4,
    "log_level": "INFO",
    "log_file": "",
    # --- dead man lease / telegram ---
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "telegram_user_id": "",
    "telegram_poll_timeout": 20,
    "telegram_api_base": "https://api.telegram.org",
    "checkin_interval_hours": 24.0,
    "grace_period_days": 7.0,
    "heartbeat_file": "",
    # --- retry semantics ---
    "retry_interval_seconds": 3600,
    "max_retry_hours": 6.0,
    "max_step_attempts": 1000,
    "skip_step_after_max_retry_hours": True,
    "require_snapshot_before_main_delete": False,
    "protect_snapshot": True,
    "abort_on_message_during_teardown": True,
    "snapshot_description_prefix": "deadman-final-snapshot",
    # --- timing ---
    "tick_seconds": 30,
    "action_poll_seconds": 2.0,
    "action_timeout_seconds": 900,
    "shutdown_grace_seconds": 180,
    "poweroff_timeout_seconds": 120,
    "image_timeout_seconds": 1800,
    # --- rate limiting ---
    "rate_limit_floor": 50,
    "rate_limit_max_wait": 3,
    # --- billing (never allowed to block teardown) ---
    "billing_mode": "auto",  # auto | none | webhook | command
    "billing_webhook_url": "",
    "billing_command": [],
    "billing_timeout_seconds": 30,
    # --- post-run ---
    "enable_post_sweep": False,
    "sweep_interval_seconds": 86400,
}

LIST_INT_FIELDS = {"main_primary_ip_ids", "protected_ip_ids"}
LIST_STR_FIELDS = {"billing_command", "protected_ips"}
BOOL_FIELDS = {
    "dry_run",
    "enable_block_file",
    "reserve_main_ipv6",
    "auto_detect_main_server",
    "skip_step_after_max_retry_hours",
    "require_snapshot_before_main_delete",
    "protect_snapshot",
    "abort_on_message_during_teardown",
    "enable_post_sweep",
}
INT_FIELDS = {
    "main_server_id",
    "block_file_size_mb",
    "telegram_poll_timeout",
    "retry_interval_seconds",
    "max_step_attempts",
    "tick_seconds",
    "action_timeout_seconds",
    "shutdown_grace_seconds",
    "poweroff_timeout_seconds",
    "image_timeout_seconds",
    "rate_limit_floor",
    "rate_limit_max_wait",
    "billing_timeout_seconds",
    "sweep_interval_seconds",
    "expect_min_servers",
}
FLOAT_FIELDS = {
    "request_timeout_seconds",
    "checkin_interval_hours",
    "grace_period_days",
    "max_retry_hours",
    "action_poll_seconds",
}

#: Extra env aliases (all are tried before the generated names).
ENV_ALIASES: dict[str, list[str]] = {
    "token": [
        "HCLOUD_TOKEN",
        "HETZNER_CLOUD_TOKEN",
        "HETZNER_API_TOKEN",
        "HETZNER_TOKEN",
        "HCLOUD_API_TOKEN",
        "DEADMAN_HETZNER_TOKEN",
    ],
    "token_file": ["HCLOUD_TOKEN_FILE", "HETZNER_TOKEN_FILE"],
    "main_server_id": ["MAIN_SERVER_ID", "HETZNER_SERVER_ID", "SERVER_ID"],
    "main_server_name": ["MAIN_SERVER_NAME", "SERVER_NAME"],
    "main_primary_ip_ids": ["MAIN_PRIMARY_IP_IDS", "MAIN_PRIMARY_IP_ID"],
    "main_primary_ip": ["MAIN_PRIMARY_IP", "MAIN_IPV4", "SERVER_IPV4"],
    "protected_ip_ids": [
        "RESERVED_PRIMARY_IPS",
        "RESERVED_PRIMARY_IP_IDS",
        "PROTECTED_PRIMARY_IP_IDS",
        "PROTECTED_IP_IDS",
    ],
    "protected_ips": ["RESERVED_IPS", "PROTECTED_IPS", "RESERVED_IP_ADDRESSES"],
    "reserve_main_ipv6": ["RESERVE_MAIN_IPV6", "PRESERVE_MAIN_IPV6"],
    "grace_period_days": [
        "GRACE_PERIOD_DAYS",
        "DEADMAN_DAYS",
        "GRACE_DAYS",
        "DEADMAN_GRACE_DAYS",
        "X_DAYS",
        "DAYS_BEFORE_SELF_DESTRUCT",
        "INACTIVITY_DAYS",
    ],
    "checkin_interval_hours": ["CHECKIN_INTERVAL_HOURS", "CHECK_IN_INTERVAL_HOURS", "CHECKIN_HOURS"],
    "retry_interval_seconds": ["RETRY_INTERVAL_SECONDS", "RETRY_INTERVAL", "STEP_RETRY_INTERVAL_SECONDS"],
    "max_retry_hours": [
        "MAX_RETRY_HOURS",
        "MAX_STEP_RETRY_HOURS",
        "X_RETRY_HOURS",
        "STEP_RETRY_HOURS",
        "RETRY_HOURS",
    ],
    "skip_step_after_max_retry_hours": ["SKIP_STEP_AFTER_MAX_RETRY_HOURS", "SKIP_STEP_AFTER_MAX_RETRIES"],
    "require_snapshot_before_main_delete": ["REQUIRE_SNAPSHOT_BEFORE_MAIN_DELETE", "REQUIRE_SNAPSHOT"],
    "abort_on_message_during_teardown": ["ABORT_ON_MESSAGE_DURING_TEARDOWN", "ABORT_ON_ANY_MESSAGE"],
    "telegram_bot_token": ["TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN", "TG_BOT_TOKEN"],
    "telegram_chat_id": ["TELEGRAM_CHAT_ID", "TG_CHAT_ID"],
    "telegram_user_id": ["TELEGRAM_USER_ID", "TG_USER_ID"],
    "state_dir": ["DEADMAN_STATE_DIR", "STATE_DIR", "DATA_DIR"],
    "enable_block_file": ["ENABLE_BLOCK_FILE", "USE_BLOCK_FILE"],
    "block_file_size_mb": ["BLOCK_FILE_SIZE_MB", "STATE_BLOCK_MB"],
    "billing_mode": ["BILLING_MODE"],
    "billing_webhook_url": ["BILLING_WEBHOOK_URL", "BILLING_WEBHOOK"],
    "billing_command": ["BILLING_COMMAND", "BILLING_HOOK_COMMAND"],
    "billing_timeout_seconds": ["BILLING_TIMEOUT_SECONDS"],
    "dry_run": ["DRY_RUN", "DEADMAN_DRY_RUN"],
    "label_selector": ["HCLOUD_LABEL_SELECTOR", "DEADMAN_LABEL_SELECTOR"],
    "heartbeat_file": ["HEARTBEAT_FILE"],
    "log_level": ["LOG_LEVEL"],
    # canonical names still accepted bare via generation below
}

#: config-file dotted keys -> canonical field (unknown keys are reported)
FILE_KEYS: dict[str, str] = {
    "api.base_url": "base_url",
    "api.timeout_seconds": "request_timeout_seconds",
    "api.request_timeout_seconds": "request_timeout_seconds",
    "api.token": "token",
    "api.token_file": "token_file",
    "target.main_server_id": "main_server_id",
    "target.main_server_name": "main_server_name",
    "target.main_primary_ip_ids": "main_primary_ip_ids",
    "target.main_primary_ip": "main_primary_ip",
    "target.protected_ip_ids": "protected_ip_ids",
    "target.protected_ips": "protected_ips",
    "target.reserve_main_ipv6": "reserve_main_ipv6",
    "target.label_selector": "label_selector",
    "target.expect_min_servers": "expect_min_servers",
    "target.auto_detect_main_server": "auto_detect_main_server",
    "teardown.retry_interval_seconds": "retry_interval_seconds",
    "teardown.max_step_retry_hours": "max_retry_hours",
    "teardown.max_retry_hours": "max_retry_hours",
    "teardown.max_step_attempts": "max_step_attempts",
    "teardown.skip_step_after_max_retry_hours": "skip_step_after_max_retry_hours",
    "teardown.require_snapshot_before_main_delete": "require_snapshot_before_main_delete",
    "teardown.snapshot_description_prefix": "snapshot_description_prefix",
    "teardown.protect_snapshot": "protect_snapshot",
    "teardown.abort_on_message_during_teardown": "abort_on_message_during_teardown",
    "teardown.enable_post_sweep": "enable_post_sweep",
    "teardown.sweep_interval_seconds": "sweep_interval_seconds",
    "timing.tick_seconds": "tick_seconds",
    "timing.action_poll_seconds": "action_poll_seconds",
    "timing.action_timeout_seconds": "action_timeout_seconds",
    "timing.shutdown_grace_seconds": "shutdown_grace_seconds",
    "timing.poweroff_timeout_seconds": "poweroff_timeout_seconds",
    "timing.image_timeout_seconds": "image_timeout_seconds",
    "rate_limit.floor": "rate_limit_floor",
    "rate_limit.max_wait_attempts": "rate_limit_max_wait",
    "lease.checkin_interval_hours": "checkin_interval_hours",
    "lease.grace_period_days": "grace_period_days",
    "lease.timeout_hours": "grace_period_days",  # legacy name from SPEC
    "lease.heartbeat_file": "heartbeat_file",
    "telegram.bot_token": "telegram_bot_token",
    "telegram.chat_id": "telegram_chat_id",
    "telegram.user_id": "telegram_user_id",
    "telegram.poll_timeout": "telegram_poll_timeout",
    "state.dir": "state_dir",
    "state.use_block_file": "enable_block_file",
    "state.block_file_size_mb": "block_file_size_mb",
    "billing.mode": "billing_mode",
    "billing.webhook_url": "billing_webhook_url",
    "billing.command": "billing_command",
    "billing.timeout_seconds": "billing_timeout_seconds",
    "dry_run": "dry_run",
    "log.level": "log_level",
    "log.file": "log_file",
}


def _generated_env_names(field: str) -> list[str]:
    upper = field.upper()
    if field == "token":
        # Never read a generic bare TOKEN from the environment; too collision-prone.
        return [f"DEADMAN_{upper}"]
    return [f"DEADMAN_{upper}", upper]


def _env_value(field: str) -> Optional[str]:
    for name in ENV_ALIASES.get(field, []) + _generated_env_names(field):
        if name in os.environ and os.environ[name] != "":
            return os.environ[name]
    return None


def _coerce(field: str, value: Any) -> Any:
    if field in LIST_INT_FIELDS:
        return as_int_list(value)
    if field in LIST_STR_FIELDS:
        return as_str_list(value)
    if field in BOOL_FIELDS:
        return as_bool(value)
    if field in INT_FIELDS:
        if value is None or value == "":
            return None
        return int(float(str(value).strip()))
    if field in FLOAT_FIELDS:
        return float(str(value).strip())
    return value if isinstance(value, str) else str(value)


def _flatten(doc: dict, prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in doc.items():
        dotted = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            flat.update(_flatten(value, dotted))
        else:
            flat[dotted] = value
    return flat


class Config:
    """Plain attribute bag with validation and redacted description."""

    def __init__(self, values: Optional[dict[str, Any]] = None) -> None:
        for field, default in DEFAULTS.items():
            value = default.copy() if isinstance(default, (list, dict)) else default
            setattr(self, field, value)
        self.unknown_file_keys: list[str] = []
        self.env_overrides_ignored: list[str] = []
        if values:
            for key, value in values.items():
                if hasattr(self, key):
                    setattr(self, key, value)

    # ------------------------------------------------------------------ load
    @classmethod
    def load(
        cls,
        config_file: Optional[str] = None,
        overrides: Optional[dict[str, Any]] = None,
        require_token: bool = False,
    ) -> "Config":
        cfg = cls()
        path = config_file or os.environ.get("DEADMAN_CONFIG") or os.environ.get("CONFIG_FILE")
        if path:
            cfg._apply_file(path)

        for field in DEFAULTS:
            raw = _env_value(field)
            if raw is not None:
                try:
                    setattr(cfg, field, _coerce(field, raw))
                except (TypeError, ValueError) as exc:
                    raise ConfigError(f"invalid value for {field}: {raw!r} ({exc})") from exc

        # Compatibility with the older SPEC naming: ALLOW_DELETE_WITHOUT_SNAPSHOT
        # is the inverse of REQUIRE_SNAPSHOT_BEFORE_MAIN_DELETE.
        for alias in ("ALLOW_DELETE_WITHOUT_SNAPSHOT", "DEADMAN_ALLOW_DELETE_WITHOUT_SNAPSHOT"):
            if os.environ.get(alias, "") != "":
                cfg.require_snapshot_before_main_delete = not as_bool(os.environ[alias])

        if overrides:
            for key, value in overrides.items():
                if value is None:
                    continue
                if not hasattr(cfg, key):
                    raise ConfigError(f"unknown configuration field: {key}")
                setattr(cfg, key, _coerce(key, value))

        # Token file support: 0600 file containing just the token.
        if not cfg.token and cfg.token_file:
            try:
                cfg.token = Path(cfg.token_file).read_text(encoding="utf-8").strip()
            except OSError as exc:
                raise ConfigError(f"cannot read token_file {cfg.token_file}: {exc}") from exc

        cfg._normalise()
        cfg.validate(require_token=require_token)
        return cfg

    def _apply_file(self, path: str) -> None:
        p = Path(path)
        if not p.exists():
            raise ConfigError(f"config file not found: {path}")
        text = p.read_text(encoding="utf-8")
        doc: Any
        if p.suffix.lower() in {".yaml", ".yml"}:
            try:
                import yaml  # type: ignore
            except ImportError as exc:  # pragma: no cover - optional dependency
                raise ConfigError(
                    "PyYAML is not installed; use a .json config file or install pyyaml"
                ) from exc
            try:
                doc = yaml.safe_load(text) or {}
            except Exception as exc:
                raise ConfigError(f"cannot parse YAML {path}: {exc}") from exc
        else:
            try:
                doc = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ConfigError(f"cannot parse JSON {path}: {exc}") from exc
        if not isinstance(doc, dict):
            raise ConfigError("config file root must be a mapping")
        flat = _flatten(doc)
        for dotted, value in flat.items():
            field = FILE_KEYS.get(dotted.lower())
            if field is None:
                # also accept flat canonical names ("main_server_id": 4711)
                candidate = dotted.lower().replace("-", "_")
                if candidate in DEFAULTS:
                    field = candidate
                else:
                    self.unknown_file_keys.append(dotted)
                    continue
            if dotted.lower() == "lease.timeout_hours" and value not in (None, ""):
                # `lease.timeout_hours` is HOURS (legacy SPEC name) while
                # `grace_period_days` is DAYS: assigning 72 straight through
                # would arm a 72-day lease instead of a 3-day one.
                try:
                    value = float(str(value).strip()) / 24.0
                except (TypeError, ValueError) as exc:
                    raise ConfigError(
                        f"invalid config value {dotted}={value!r}: expected a number of hours"
                    ) from exc
            try:
                setattr(self, field, _coerce(field, value))
            except (TypeError, ValueError) as exc:
                raise ConfigError(f"invalid config value {dotted}={value!r}: {exc}") from exc

    def _normalise(self) -> None:
        self.token = str(self.token or "").strip()
        self.main_server_id = int(self.main_server_id) if self.main_server_id else None
        self.telegram_bot_token = str(self.telegram_bot_token or "").strip()
        self.telegram_chat_id = str(self.telegram_chat_id or "").strip()
        self.telegram_user_id = str(self.telegram_user_id or "").strip()
        self.billing_mode = str(self.billing_mode or "auto").lower().strip()
        if self.billing_mode == "auto":
            if self.billing_webhook_url:
                self.billing_mode = "webhook"
            elif self.billing_command:
                self.billing_mode = "command"
            else:
                self.billing_mode = "none"
        if not self.log_file:
            self.log_file = str(Path(self.state_dir) / "deadman.log")
        self.main_primary_ip = str(self.main_primary_ip or "").strip()
        self.main_server_name = str(self.main_server_name or "").strip()
        # MAIN_PRIMARY_IP_ID and MAIN_PRIMARY_IP_IDS may both be present; merge.
        ids = as_int_list(self.main_primary_ip_ids)
        self.main_primary_ip_ids = list(dict.fromkeys(ids))
        self.protected_ip_ids = list(dict.fromkeys(as_int_list(self.protected_ip_ids)))
        self.protected_ips = list(dict.fromkeys(as_str_list(self.protected_ips)))

    # -------------------------------------------------------------- validate
    def validate(self, require_token: bool = False) -> None:
        errors: list[str] = []
        if require_token and not self.token:
            errors.append(
                "HCLOUD_TOKEN is not set (create a project-scoped Hetzner Cloud API token)"
            )
        if self.base_url and not re.match(r"^https?://", self.base_url):
            errors.append("base_url must start with http:// or https://")
        if self.grace_period_days <= 0:
            errors.append("GRACE_PERIOD_DAYS must be > 0")
        if self.checkin_interval_hours <= 0:
            errors.append("CHECKIN_INTERVAL_HOURS must be > 0")
        if self.retry_interval_seconds <= 0:
            errors.append("RETRY_INTERVAL_SECONDS must be > 0")
        if self.max_retry_hours < 0:
            errors.append("MAX_RETRY_HOURS must be >= 0")
        if self.block_file_size_mb < 1:
            errors.append("BLOCK_FILE_SIZE_MB must be >= 1")
        if self.billing_mode not in {"none", "webhook", "command"}:
            errors.append("BILLING_MODE must be one of: auto, none, webhook, command")
        if require_token:
            # Only enforce run-time integrations when starting the daemon or a
            # real pre-flight; `status` must keep working with a partial env.
            if self.billing_mode == "webhook" and not self.billing_webhook_url:
                errors.append("BILLING_MODE=webhook requires BILLING_WEBHOOK_URL")
            if self.billing_mode == "command" and not self.billing_command:
                errors.append("BILLING_MODE=command requires BILLING_COMMAND")
            if self.telegram_bot_token and not self.telegram_chat_id:
                errors.append("TELEGRAM_BOT_TOKEN is set but TELEGRAM_CHAT_ID is missing")
        if errors:
            raise ConfigError("; ".join(errors))

    # ----------------------------------------------------------- properties
    @property
    def state_path(self) -> Path:
        return Path(self.state_dir)

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    @property
    def billing_enabled(self) -> bool:
        return self.billing_mode in {"webhook", "command"}

    @property
    def block_file_bytes(self) -> int:
        return int(self.block_file_size_mb) * 1024 * 1024

    def describe(self) -> dict[str, Any]:
        safe = {}
        secret_fields = {"token", "telegram_bot_token", "billing_webhook_url"}
        for field in DEFAULTS:
            value = getattr(self, field)
            if field in secret_fields:
                value = "set" if value else "unset"
            safe[field] = value
        return safe

    def __repr__(self) -> str:  # pragma: no cover
        return f"Config(state_dir={self.state_dir!r}, dry_run={self.dry_run}, main_server_id={self.main_server_id})"
