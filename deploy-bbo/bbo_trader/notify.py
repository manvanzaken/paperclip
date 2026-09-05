"""Telegram: send messages and poll the operator commands (/stop, /start, /close_all, /status).

This is the only remote control path of a bot without an automatic kill switch, so the poller must never
wedge: every update advances the offset whatever its shape, a malformed message costs nothing, commands are
matched case-insensitively and with a `@botname` suffix, the configured chat id is normalized, updates dated
before process start are ignored (a /close_all sent during an outage must not replay against reloaded
positions), and the first failure per process is a WARNING that never contains the token."""
from __future__ import annotations

import logging
import time

import aiohttp

log = logging.getLogger("bbo.notify")
COMMANDS = ("/stop", "/start", "/close_all", "/status")
MAX_TEXT = 4000


def parse_commands(updates: list[dict], chat_id: str, offset: int, not_before: float = 0.0) -> tuple[list[str], int]:
    """Pure: Telegram getUpdates payload → (commands from our chat, next offset). Never raises on a payload
    Telegram can produce; every update with an id advances the offset."""
    cmds: list[str] = []
    nxt = offset
    want = str(chat_id).strip()
    for u in updates:
        if not isinstance(u, dict):
            continue
        try:
            uid = int(u.get("update_id", 0))
        except (TypeError, ValueError):
            continue
        nxt = max(nxt, uid + 1)
        msg = u.get("message")
        if not isinstance(msg, dict):
            continue                                    # edited_message / channel_post / callback_query: ignored
        chat = msg.get("chat")
        if not isinstance(chat, dict) or str(chat.get("id", "")).strip() != want:
            continue
        try:
            dated = float(msg.get("date") or 0.0)
        except (TypeError, ValueError):
            dated = 0.0
        if not_before and dated and dated < not_before:
            continue
        parts = str(msg.get("text") or "").strip().split()
        text = parts[0].split("@")[0].lower() if parts else ""
        if text in COMMANDS:
            cmds.append(text)
    return cmds, nxt


class Telegram:
    def __init__(self, token: str, chat_id: str, session_factory=aiohttp.ClientSession, clock=time.time):
        self.token = (token or "").strip()
        self.chat_id = str(chat_id or "").strip()
        self.enabled = bool(self.token and self.chat_id)
        if bool(self.token) != bool(self.chat_id):
            log.warning("TELEGRAM_DISABLED: set both TELEGRAM_TOKEN and TELEGRAM_CHAT_ID (only one is set)")
        self._session_factory = session_factory
        self._offset = 0
        self._started = clock()          # updates dated before this are ignored: no replay of a stale /close_all
        #                                  (Telegram dates are whole seconds; a command sent in the start-up second may be
        #                                  dropped — the safe side of the bias: a missed /stop costs a re-send, a replayed
        #                                  /close_all costs money)
        self._warned = False

    def _fail(self, what: str) -> None:
        """First failure per process at WARNING (a dead token must not be silent), the rest at DEBUG. Never the
        exception repr: the request URL carries the bot token."""
        if not self._warned:
            self._warned = True
            log.warning("TELEGRAM_FAILING %s — further failures logged at DEBUG", what)
        else:
            log.debug("telegram %s", what)

    async def send(self, text: str) -> bool:
        if not self.enabled:
            return False
        if len(text) > MAX_TEXT:
            text = text[:MAX_TEXT - 3] + "..."
        try:
            async with self._session_factory() as s:
                async with s.post(f"https://api.telegram.org/bot{self.token}/sendMessage",
                                  json={"chat_id": self.chat_id, "text": text},
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        self._fail(f"sendMessage HTTP {r.status}")
                        return False
                    return True
        except Exception as e:  # noqa: BLE001
            self._fail(f"sendMessage {type(e).__name__}")
            return False

    async def poll_commands(self) -> list[str]:
        if not self.enabled:
            return []
        try:
            async with self._session_factory() as s:
                async with s.get(f"https://api.telegram.org/bot{self.token}/getUpdates",
                                 params={"offset": self._offset, "timeout": 0},
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
                    data = await r.json(content_type=None)
        except Exception as e:  # noqa: BLE001
            self._fail(f"getUpdates {type(e).__name__}")
            return []
        if not isinstance(data, dict) or data.get("ok") is not True:
            desc = data.get("description") if isinstance(data, dict) else type(data).__name__
            self._fail(f"getUpdates ok=false: {str(desc)[:120]}")
            return []
        result = data.get("result")
        if not isinstance(result, list):
            return []
        try:
            cmds, self._offset = parse_commands(result, self.chat_id, self._offset, not_before=self._started)
        except Exception:  # noqa: BLE001 — belt and braces: a parse bug must never freeze the offset
            log.exception("TELEGRAM_PARSE_ERROR")
            ids = [int(u.get("update_id", 0)) for u in result if isinstance(u, dict) and str(u.get("update_id", "")).isdigit()]
            self._offset = max([self._offset] + [i + 1 for i in ids])
            return []
        return cmds
