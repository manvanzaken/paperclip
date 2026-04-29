"""Tests for heal.atomic_writes."""

from heal.atomic_writes import read_state, write_state


class TestRoundTrip:
    def test_write_then_read(self, tmp_path):
        path = tmp_path / "state.json"
        payload = {"foo": "bar", "n": 42, "list": [1, 2, 3]}
        write_state(path, payload)
        assert read_state(path) == payload

    def test_overwrites_existing(self, tmp_path):
        path = tmp_path / "state.json"
        write_state(path, {"v": 1})
        write_state(path, {"v": 2})
        assert read_state(path) == {"v": 2}


class TestRecovery:
    def test_read_missing_returns_empty(self, tmp_path):
        assert read_state(tmp_path / "does-not-exist.json") == {}

    def test_read_corrupt_returns_empty(self, tmp_path):
        path = tmp_path / "corrupt.json"
        path.write_text("{not json")
        assert read_state(path) == {}

    def test_write_does_not_leave_tmp_file(self, tmp_path):
        path = tmp_path / "state.json"
        write_state(path, {"x": 1})
        assert not (tmp_path / "state.json.tmp").exists()


class TestAtomicity:
    def test_no_partial_visible_state_on_crash(self, tmp_path, monkeypatch):
        """If the .tmp write fails, the original file must be untouched."""
        path = tmp_path / "state.json"
        write_state(path, {"version": "good"})

        # Force os.replace to raise; the original file must still hold "good".
        import heal.atomic_writes as aw
        original_replace = aw.os.replace

        def boom(*args, **kwargs):
            raise OSError("simulated crash")

        monkeypatch.setattr(aw.os, "replace", boom)
        try:
            write_state(path, {"version": "bad"})
        except OSError:
            pass
        # Original file untouched.
        assert read_state(path) == {"version": "good"}
        # Restore for cleanup.
        monkeypatch.setattr(aw.os, "replace", original_replace)
