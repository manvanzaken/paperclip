from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict
from rich.live import Live
from rich.table import Table


@dataclass
class ConsoleRow:
    triangle_id: str
    state: str
    net_edge_pct: float
    size_usd: float
    profit_usd: float
    bottleneck_leg: int
    age_ms: int


class LiveConsole:
    def __init__(self, refresh_ms: int, top_n: int, show_candidates: bool):
        self.refresh_ms = refresh_ms
        self.top_n = top_n
        self.show_candidates = show_candidates
        self._rows: Dict[str, ConsoleRow] = {}
        self._live = Live(self._render(), refresh_per_second=max(1, 1000 // refresh_ms))

    def __enter__(self):
        self._live.start()
        return self

    def __exit__(self, *exc):
        self._live.stop()

    def update_row(self, row: ConsoleRow) -> None:
        self._rows[row.triangle_id] = row
        self._live.update(self._render())

    def remove_row(self, triangle_id: str) -> None:
        self._rows.pop(triangle_id, None)
        self._live.update(self._render())

    def _render(self) -> Table:
        t = Table(title="Triangular Arb Scanner", expand=False)
        t.add_column("State"); t.add_column("Triangle"); t.add_column("Net%", justify="right")
        t.add_column("Size$", justify="right"); t.add_column("Profit$", justify="right")
        t.add_column("Bottleneck", justify="center")
        rows = list(self._rows.values())
        if not self.show_candidates:
            rows = [r for r in rows if r.state == "confirmed"]
        rows.sort(key=lambda r: (r.state != "confirmed", -r.profit_usd))
        for r in rows[: self.top_n]:
            color = "green" if r.state == "confirmed" else "yellow"
            t.add_row(f"[{color}]{r.state}[/]", r.triangle_id,
                      f"{r.net_edge_pct:+.4f}", f"{r.size_usd:.0f}",
                      f"{r.profit_usd:.2f}", str(r.bottleneck_leg))
        return t
