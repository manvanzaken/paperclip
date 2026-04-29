"""Atomic JSON state writes.

POSIX guarantees `os.replace()` is atomic on the same filesystem. Writing
the new payload to `path.tmp` first and replacing only on full success
prevents the file from ever being seen in a half-written state — which
matters because the bot reads the file on startup to recover state.

`read_state` never raises: missing or corrupt files return `{}` so a
corrupted state file does not prevent the bot from starting.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)


def write_state(path: str | Path, payload: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    # Write fully, fsync, then atomic replace.
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, p)


def read_state(path: str | Path) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError) as e:
        log.error("read_state: corrupt or unreadable file %s: %s", p, e)
        return {}
