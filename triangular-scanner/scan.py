from __future__ import annotations
import argparse
import asyncio
import logging
import sys
from pathlib import Path

from triscan.config import load_config
from triscan.enumerator import enumerate_triangles
from triscan.rest_poller import Tier1Poller

log = logging.getLogger("triscan")


def _build_source(name: str, ex_cfg):
    from triscan.sources.mexc import MexcSource
    from triscan.sources.binance import BinanceSource
    from triscan.sources.gate import GateSource
    from triscan.sources.kucoin import KucoinSource
    from triscan.sources.bybit import BybitSource
    cls = {
        "mexc": MexcSource,
        "binance": BinanceSource,
        "gate": GateSource,
        "kucoin": KucoinSource,
        "bybit": BybitSource,
    }.get(name)
    if cls is None:
        raise NotImplementedError(f"unknown exchange: {name}")
    return cls(name=name, taker_fee_pct=ex_cfg.taker_fee_pct,
               rest_url=ex_cfg.rest_url, ws_url=ex_cfg.ws_url)


async def _reenumerate_loop(cfg, sources, pollers_by_name):
    interval = cfg.scanner.triangle_refresh_hours * 3600
    while True:
        await asyncio.sleep(interval)
        for src in sources:
            try:
                markets = await src.fetch_markets()
                tris = enumerate_triangles(
                    exchange=src.name, markets=markets,
                    anchors=cfg.filters.anchors, min_volume_usd=cfg.filters.min_24h_volume_usd,
                )
                pollers_by_name[src.name].triangles = tris
                pollers_by_name[src.name]._symbols = sorted({s for t in tris for s in t.symbols})
                log.info("re-enumerated %s: %d triangles", src.name, len(tris))
            except Exception as e:
                log.warning("re-enumeration failed for %s: %s", src.name, e)


async def _run(cfg):
    sources = []
    for name, ex_cfg in cfg.exchanges.items():
        if not ex_cfg.enabled:
            continue
        try:
            sources.append(_build_source(name, ex_cfg))
        except NotImplementedError as e:
            log.warning("skipping %s: %s", name, e)
    if not sources:
        log.error("no sources enabled — exiting")
        return

    pollers_by_name = {}
    for src in sources:
        log.info("loading markets for %s ...", src.name)
        markets = await src.fetch_markets()
        triangles = enumerate_triangles(
            exchange=src.name, markets=markets,
            anchors=cfg.filters.anchors, min_volume_usd=cfg.filters.min_24h_volume_usd,
        )
        log.info("%s: %d markets, %d triangles", src.name, len(markets), len(triangles))
        pollers_by_name[src.name] = Tier1Poller(source=src, triangles=triangles)

    def make_callback(name):
        async def cb(results):
            results.sort(key=lambda r: r.gross_edge_pct, reverse=True)
            top = results[: cfg.output.console.top_n]
            if top:
                print(f"--- {name} tier1 (top {len(top)}) ---")
                for r in top:
                    print(f"  {r.triangle.id}  gross={r.gross_edge_pct:+.4f}%  net={r.net_edge_pct:+.4f}%")
        return cb

    tasks = [
        p.run_loop(cfg.scanner.tier1_interval_sec, make_callback(name))
        for name, p in pollers_by_name.items()
    ]
    tasks.append(_reenumerate_loop(cfg, sources, pollers_by_name))

    try:
        await asyncio.gather(*tasks)
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
