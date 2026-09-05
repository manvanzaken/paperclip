import json
import logging

from bbo_trader.notify import parse_commands, Telegram


def test_parse_commands_filters_chat_and_advances_offset():
    updates = [
        {"update_id": 10, "message": {"chat": {"id": 42}, "text": "/stop"}},
        {"update_id": 11, "message": {"chat": {"id": 99}, "text": "/start"}},        # someone else
        {"update_id": 12, "message": {"chat": {"id": 42}, "text": "hello"}},
        {"update_id": 13, "message": {"chat": {"id": 42}, "text": "/close_all now"}},
    ]
    cmds, nxt = parse_commands(updates, "42", offset=0)
    assert cmds == ["/stop", "/close_all"] and nxt == 14
    assert parse_commands([], "42", 14) == ([], 14)
    # nothing Telegram can send may wedge the poller: every update advances the offset
    for text in (" ", "\n", "\t", "\xa0", "　", None, 7):
        assert parse_commands([{"update_id": 5, "message": {"chat": {"id": 42}, "text": text}}], "42", 0) == ([], 6)
    assert parse_commands([{"update_id": 5, "edited_message": {"chat": {"id": 42}, "text": "/stop"}}], "42", 0) == ([], 6)
    assert parse_commands([{"update_id": 5, "message": "junk"}, "junk", {"update_id": "x"}], "42", 0) == ([], 6)
    assert parse_commands([{"update_id": 5, "message": {"chat": "junk", "text": "/stop"}},
                           {"update_id": 6, "message": {"chat": {"id": 42}, "text": "/stop"}}], "42", 0) == (["/stop"], 7)
    # the command menu in a group sends /stop@botname; phones capitalize
    assert parse_commands([{"update_id": 5, "message": {"chat": {"id": 42}, "text": "/Stop@bbo_bot"}}], "42", 0) == (["/stop"], 6)
    # a chat id with stray whitespace (env file) still matches
    assert parse_commands([{"update_id": 5, "message": {"chat": {"id": 42}, "text": "/stop"}}], " 42 ", 0) == (["/stop"], 6)
    # updates dated before process start are consumed but not executed (no replay of a stale /close_all)
    old = {"update_id": 5, "message": {"chat": {"id": 42}, "text": "/close_all", "date": 1000}}
    new = {"update_id": 6, "message": {"chat": {"id": 42}, "text": "/close_all", "date": 3000}}
    assert parse_commands([old, new], "42", 0, not_before=2000) == (["/close_all"], 7)


async def test_disabled_telegram_is_a_noop(caplog):
    t = Telegram("", "")
    assert not t.enabled and await t.send("x") is False and await t.poll_commands() == []
    with caplog.at_level(logging.WARNING, logger="bbo.notify"):
        half = Telegram("tok", "")                              # half-configured: say so once at start-up
    assert not half.enabled and "TELEGRAM_DISABLED" in caplog.text


class _Resp:
    def __init__(self, status, body):
        self.status, self._body = status, body

    async def json(self, content_type=None):
        return json.loads(self._body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, status=200, body="{}", raise_with=None):
        self.status, self.body, self.raise_with, self.calls = status, body, raise_with, []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def get(self, url, **kw):
        self.calls.append(("get", url, kw))
        if self.raise_with:
            raise self.raise_with
        return _Resp(self.status, self.body)

    def post(self, url, **kw):
        self.calls.append(("post", url, kw))
        if self.raise_with:
            raise self.raise_with
        return _Resp(self.status, self.body)


async def test_telegram_failures_are_visible_once_and_never_leak_the_token(caplog):
    token = "123456:SECRET-TOKEN-VALUE"
    sess = _Session(200, json.dumps({"ok": False, "error_code": 401, "description": "Unauthorized"}))
    t = Telegram(token, " 42 ", session_factory=lambda: sess)
    assert t.chat_id == "42"
    with caplog.at_level(logging.DEBUG, logger="bbo.notify"):
        assert await t.poll_commands() == []
        assert await t.poll_commands() == []
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "Unauthorized" in warnings[0].getMessage()     # once at WARNING, then DEBUG
    caplog.clear()
    boom = _Session(raise_with=ValueError(f"https://api.telegram.org/bot{token}/sendMessage is bad"))
    t2 = Telegram(token, "42", session_factory=lambda: boom)
    with caplog.at_level(logging.DEBUG, logger="bbo.notify"):
        assert await t2.send("x") is False and await t2.poll_commands() == []
    assert "TELEGRAM_FAILING" in caplog.text and token not in caplog.text          # the URL carries the token: never logged
    sess3 = _Session(500, "{}")
    t3 = Telegram(token, "42", session_factory=lambda: sess3)
    with caplog.at_level(logging.WARNING, logger="bbo.notify"):
        assert await t3.send("x") is False
    assert "HTTP 500" in caplog.text


async def test_telegram_polls_advance_the_offset_and_skip_pre_start_updates():
    now = [5000.0]
    first = json.dumps({"ok": True, "result": [
        {"update_id": 10, "message": {"chat": {"id": 42}, "text": "/close_all", "date": 4000}},   # before start: ignored
        {"update_id": 11, "message": {"chat": {"id": 42}, "text": "/status", "date": 5001}}]})
    sess = _Session(200, first)
    t = Telegram("tok", "42", session_factory=lambda: sess, clock=lambda: now[0])
    assert await t.poll_commands() == ["/status"] and t._offset == 12
    sess.body = json.dumps({"ok": True, "result": []})
    assert await t.poll_commands() == [] and sess.calls[-1][2]["params"]["offset"] == 12
    sess.body = json.dumps({"ok": True, "result": "not-a-list"})
    assert await t.poll_commands() == [] and t._offset == 12
    long_sess = _Session(200, json.dumps({"ok": True, "result": {}}))
    t2 = Telegram("tok", "42", session_factory=lambda: long_sess)
    assert await t2.send("y" * 5000) is True
    assert len(long_sess.calls[-1][2]["json"]["text"]) == 4000 and long_sess.calls[-1][2]["json"]["text"].endswith("...")
