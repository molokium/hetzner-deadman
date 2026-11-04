"""Minimal Telegram Bot API client (stdlib only).

Only three endpoints are needed: ``sendMessage``, ``getUpdates`` and ``getMe``.
Long polling with a persisted ``last_update_id`` gives the dead man's switch a
reliable inbound channel without any webhook or third-party framework.
"""

from __future__ import annotations

import json
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional

from .log import JsonLogger, null_logger
from .util import truncate

MAX_MESSAGE = 3900


class TelegramClient:
    def __init__(
        self,
        token: str,
        chat_id: str,
        user_id: str = "",
        api_base: str = "https://api.telegram.org",
        logger: Optional[JsonLogger] = None,
        timeout: float = 15.0,
    ) -> None:
        self.token = token or ""
        self.chat_id = str(chat_id or "")
        self.user_id = str(user_id or "")
        self.api_base = api_base.rstrip("/")
        self.logger = logger or null_logger()
        self.timeout = timeout
        self.sent_count = 0
        self.last_error: Optional[str] = None

    # ------------------------------------------------------------------ core
    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    def _api(self, method: str, payload: dict, timeout: Optional[float] = None) -> dict:
        url = f"{self.api_base}/bot{self.token}/{method}"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "hetzner-deadman/1.0"},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
                body = resp.read()
            parsed = json.loads(body.decode("utf-8")) if body else {}
            if not parsed.get("ok"):
                desc = parsed.get("description", "unknown telegram error")
                self.last_error = str(desc)
                raise RuntimeError(desc)
            return parsed
        except urllib.error.HTTPError as exc:
            # Telegram signals most errors with a 4xx/5xx JSON body; read it for
            # the description and always close the response object.
            description = ""
            try:
                raw = exc.read()
                payload = json.loads(raw.decode("utf-8")) if raw else {}
                description = str(payload.get("description") or "")
            except Exception:
                description = ""
            finally:
                try:
                    exc.close()
                except Exception:
                    pass
            self.last_error = description or str(exc)
            raise RuntimeError(self.last_error) from None
        except (urllib.error.URLError, socket.timeout, TimeoutError, OSError, json.JSONDecodeError) as exc:
            self.last_error = str(exc)
            raise

    # ------------------------------------------------------------------ send
    def send(self, text: str, retries: int = 2, timeout: Optional[float] = None) -> bool:
        """Send a message, truncated to Telegram's limit. Never raises."""
        if not self.enabled:
            return False
        text = truncate(str(text), MAX_MESSAGE)
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        for attempt in range(1, retries + 2):
            try:
                self._api("sendMessage", payload, timeout=timeout)
                self.sent_count += 1
                self.logger.info("telegram_sent", "telegram message sent", chars=len(text))
                return True
            except Exception as exc:
                self.logger.warning(
                    "telegram_send_failed", str(exc), attempt=attempt, retries=retries
                )
                if attempt <= retries:
                    time.sleep(min(2 * attempt, 6))
        return False

    def send_async(self, text: str, retries: int = 1) -> None:
        """Fire-and-forget send on a daemon thread; never blocks teardown."""
        if not self.enabled:
            return

        def _runner() -> None:
            try:
                self.send(text, retries=retries, timeout=10)
            except Exception:
                pass

        threading.Thread(target=_runner, name="telegram-send", daemon=True).start()

    # --------------------------------------------------------------- receive
    def get_updates(self, offset: int = 0, timeout: Optional[int] = None) -> list[dict]:
        """Long-poll for updates. Returns [] on any failure."""
        if not self.enabled:
            return []
        poll_timeout = int(timeout if timeout is not None else 20)
        payload: dict[str, Any] = {
            "timeout": poll_timeout,
            "allowed_updates": ["message"],
            "limit": 50,
        }
        if offset:
            payload["offset"] = int(offset)
        try:
            parsed = self._api("getUpdates", payload, timeout=poll_timeout + 15)
        except Exception as exc:
            self.logger.warning("telegram_poll_failed", str(exc))
            return []
        updates = parsed.get("result") or []
        return updates if isinstance(updates, list) else []

    def get_me(self) -> Optional[dict]:
        try:
            parsed = self._api("getMe", {}, timeout=10)
            return parsed.get("result")
        except Exception as exc:
            self.logger.warning("telegram_getme_failed", str(exc))
            return None

    # --------------------------------------------------------------- helpers
    def is_authorized(self, message: dict) -> bool:
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id", ""))
        user = message.get("from") or {}
        user_id = str(user.get("id", ""))
        if self.chat_id and chat_id == self.chat_id:
            return True
        if self.user_id and user_id == self.user_id:
            return True
        return False

    @staticmethod
    def extract_text(update: dict) -> Optional[str]:
        message = update.get("message") or update.get("edited_message") or {}
        text = message.get("text")
        return str(text) if text else None

    @staticmethod
    def parse_command(text: str, bot_username: str = "") -> tuple[str, str]:
        """Return (command, argument). '/status@bot foo' -> ('/status', 'foo')."""
        text = (text or "").strip()
        if not text.startswith("/"):
            return "", text
        parts = text.split(maxsplit=1)
        command = parts[0].lower()
        if "@" in command:
            command, _, target = command.partition("@")
            if bot_username and target.lower() != bot_username.lower():
                return "", text
        return command, (parts[1].strip() if len(parts) > 1 else "")
