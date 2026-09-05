"""Telegram: send messages and poll the operator commands (/stop, /start, /close_all, /status)."""
from __future__ import annotations

import logging

import aiohttp

log = logging.getLogger("bbo.notify")
COMMANDS = ("/stop", "/start", "/close_all", "/status")


def parse_commands(updates: list[dict], chat_id: str, offset: int) -> tuple[list[str], int]:
    """Pure: Telegram getUpdates payload → (commands from our chat, next offset)."""
    cmds: list[str] = []
    nxt = offset
    for u in updates:
        uid = int(u.get("update_id", 0))
        nxt = max(nxt, uid + 1)
        msg = u.get("message") or {}
        if str((msg.get("chat") or {}).get("id", "")) != str(chat_id):
            continue
        text = str(msg.get("text", "")).strip().split()[0] if msg.get("text") else ""
        if text in COMMANDS:
            cmds.append(text)
    return cmds, nxt


class Telegram:
    def __init__(self, token: str, chat_id: str, session_factory=aiohttp.ClientSession):
        self.token, self.chat_id = token, chat_id
        self.enabled = bool(token and chat_id)
        self._session_factory = session_factory
        self._offset = 0

    async def send(self, text: str) -> bool:
        if not self.enabled:
            return False
        try:
            async with self._session_factory() as s:
                async with s.post(f"https://api.telegram.org/bot{self.token}/sendMessage",
                                  json={"chat_id": self.chat_id, "text": text[:4000]},
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    return r.status == 200
        except Exception as e:  # noqa: BLE001
            log.debug("telegram send failed: %r", e)
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
            log.debug("telegram poll failed: %r", e)
            return []
        cmds, self._offset = parse_commands(data.get("result") or [], self.chat_id, self._offset)
        return cmds