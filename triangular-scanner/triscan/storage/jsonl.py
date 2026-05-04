from __future__ import annotations
import asyncio
import gzip
import json
import logging
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


class JsonlWriter:
    def __init__(self, data_dir: Path | str, retention_days: int):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.retention_days = retention_days
        self._lock = asyncio.Lock()
        self._current_date: Optional[str] = None
        self._fh = None

    def _today(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _path(self, date: str) -> Path:
        return self.data_dir / f"events-{date}.jsonl"

    async def _rotate_if_needed(self) -> None:
        today = self._today()
        if self._current_date == today:
            return
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        for p in self.data_dir.glob("events-*.jsonl"):
            stem = p.name[len("events-"):-len(".jsonl")]
            if stem != today:
                gz_path = p.with_suffix(p.suffix + ".gz")
                with p.open("rb") as src, gzip.open(gz_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                p.unlink()
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        for p in self.data_dir.glob("events-*"):
            stem = p.name[len("events-"):]
            stem = stem.split(".", 1)[0]
            try:
                d = datetime.strptime(stem, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if d < cutoff:
                try:
                    p.unlink()
                except OSError as e:
                    log.warning("could not delete %s: %s", p, e)
        self._current_date = today
        self._fh = self._path(today).open("a")

    async def write(self, event: dict) -> None:
        async with self._lock:
            await self._rotate_if_needed()
            self._fh.write(json.dumps(event) + "\n")
            self._fh.flush()

    async def close(self) -> None:
        async with self._lock:
            if self._fh is not None:
                self._fh.close()
                self._fh = None
