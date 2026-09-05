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


async def test_disabled_telegram_is_a_noop():
    t = Telegram("", "")
    assert not t.enabled and await t.send("x") is False and await t.poll_commands() == []