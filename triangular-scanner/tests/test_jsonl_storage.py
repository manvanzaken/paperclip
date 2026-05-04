import gzip
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pytest
from triscan.storage.jsonl import JsonlWriter


@pytest.mark.asyncio
async def test_writes_event_to_today_file(tmp_path):
    w = JsonlWriter(data_dir=tmp_path, retention_days=30)
    await w.write({"type": "test", "k": 1})
    await w.close()
    files = list(tmp_path.glob("events-*.jsonl"))
    assert len(files) == 1
    line = files[0].read_text().strip()
    assert json.loads(line) == {"type": "test", "k": 1}


@pytest.mark.asyncio
async def test_rotates_at_day_boundary_and_gzips_previous(tmp_path):
    w = JsonlWriter(data_dir=tmp_path, retention_days=30)
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    (tmp_path / f"events-{yesterday}.jsonl").write_text('{"type":"old"}\n')
    await w.write({"type": "today"})
    await w.close()
    assert (tmp_path / f"events-{yesterday}.jsonl.gz").exists()
    assert not (tmp_path / f"events-{yesterday}.jsonl").exists()


@pytest.mark.asyncio
async def test_drops_files_past_retention(tmp_path):
    w = JsonlWriter(data_dir=tmp_path, retention_days=2)
    old = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%Y-%m-%d")
    p = tmp_path / f"events-{old}.jsonl.gz"
    with gzip.open(p, "wt") as fh:
        fh.write('{"type":"old"}\n')
    await w.write({"type": "today"})
    await w.close()
    assert not p.exists()
