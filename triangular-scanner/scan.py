from __future__ import annotations
import argparse
import asyncio
import logging
import sys
from pathlib import Path

from triscan.config import load_config
from triscan.enumerator import enumerate_triangles
from triscan.rest_poller import Tier1Poller
from triscan.sources.mexc import MexcSource

log = logging.getLogger("triscan")


def _build_source(name: str, ex_cfg):
    if name == "mexc":
        return MexcSource(name="mexc", taker_fee_pct=ex_cfg.taker_fee_pct,
                          rest_url=ex_cfg.rest_url, ws_url=ex_cfg.ws_url)
    raise NotImplementedError(f"source not implemented yet: {name}")


async def _run(cfg):
    sources = []
    for name, ex_cfg in cfg.exchanges.items():
        if not ex_cfg.enabled:
            continue
        try:
            sources.append(_build_source(name, ex_cfg))
        except NotImplementedError as e:
            log.warning("skipping exchange %s: %s", name, e)

    if not sources:
        log.error("no enabled & implemented sources — exiting")
        return

    pollers = []
    for src in sources:
        log.info("loading markets for %s ...", src.name)
        markets = await src.fetch_markets()
        triangles = enumerate_triangles(
            exchange=src.name,
            markets=markets,
            anchors=cfg.filters.anchors,
            min_volume_usd=cfg.filters.min_24h_volume_usd,
        )
        log.info("%s: %d eligible markets, %d triangles", src.name, len(markets), len(triangles))
        pollers.append(Tier1Poller(source=src, triangles=triangles))

    async def on_results(results):
        results.sort(key=lambda r: r.gross_edge_pct, reverse=True)
        top = results[: cfg.output.console.top_n]
        print(f"--- tier1 cycle (top {len(top)}) ---")
        for r in top:
            print(f"  {r.triangle.id}  gross={r.gross_edge_pct:+.4f}%  net={r.net_edge_pct:+.4f}%")

    try:
        await asyncio.gather(*(p.run_loop(cfg.scanner.tier1_interval_sec, on_results) for p in pollers))
    finally:
        await asyncio.gather(*(s.close() for s in sources), return_exceptions=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)
    logging.basicConfig(level=getattr(logging, cfg.output.log_level), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(_run(cfg))
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
