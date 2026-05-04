from __future__ import annotations
import argparse
import asyncio
import json
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

    from triscan.storage.jsonl import JsonlWriter
    from triscan.storage.sqlite import SqliteStore

    data_dir = Path(cfg.storage.data_dir)
    jsonl = JsonlWriter(data_dir=data_dir, retention_days=cfg.storage.jsonl_retention_days)
    sqlite_store = SqliteStore(cfg.storage.sqlite_path)

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

    from triscan.ws_manager import WsManager
    from triscan.pipeline import Pipeline, PipelineConfig
    from decimal import Decimal as _D

    pipelines = {}
    ws_managers = {}
    for src in sources:
        ws_managers[src.name] = WsManager(source=src,
                                          max_subscriptions=cfg.scanner.max_ws_subscriptions_per_exchange)
        pcfg = PipelineConfig(
            tier1_threshold_pct=cfg.scanner.tier1_threshold_pct,
            tier2_threshold_pct=cfg.scanner.tier2_threshold_pct,
            min_profit_usd=cfg.scanner.min_profit_usd,
            cooldown_sec=cfg.scanner.cooldown_sec,
            max_size_cap_usd=_D(str(cfg.scanner.max_size_cap_usd)),
            taker_fee_pct=_D(str(src.taker_fee_pct)),
            sample_every_n_updates=cfg.scanner.sample_every_n_updates,
        )

        async def on_opp(evt, _name=src.name, _jsonl=jsonl, _sqlite=sqlite_store):
            await _jsonl.write(evt)
            if evt["type"] == "OpportunityClosed":
                _sqlite.insert_opportunity(evt["opportunity"])
            o = evt["opportunity"]
            if evt["type"] == "OpportunityOpen":
                print(f"[{_name}] OPEN  {o['triangle_id']} net={o['open_net_edge_pct']:+.4f}% "
                      f"size=${o['peak_executable_size_usd']:.0f} profit=${o['peak_executable_profit_usd']:.2f}")
            else:
                print(f"[{_name}] CLOSE {o['triangle_id']} life={o['lifetime_ms']}ms "
                      f"peak_net={o['peak_net_edge_pct']:+.4f}% peak_profit=${o['peak_executable_profit_usd']:.2f} "
                      f"reason={o['closed_reason']}")

        pipelines[src.name] = Pipeline(
            triangles=pollers_by_name[src.name].triangles,
            ws_manager=ws_managers[src.name],
            config=pcfg,
            on_opportunity=on_opp,
        )
        ws_managers[src.name]._on_symbol_failure = pipelines[src.name].notify_ws_failed  # late-bind

    def make_callback(name):
        async def cb(results):
            results.sort(key=lambda r: r.gross_edge_pct, reverse=True)
            top = results[: cfg.output.console.top_n]
            if top:
                print(f"--- {name} tier1 (top {len(top)}) ---")
                for r in top:
                    print(f"  {r.triangle.id}  gross={r.gross_edge_pct:+.4f}%  net={r.net_edge_pct:+.4f}%")
            if name in pipelines:
                await pipelines[name].handle_tier1(results)
        return cb

    async def tick_loop():
        while True:
            for p in pipelines.values():
                await p.tick()
            await asyncio.sleep(1)

    total_triangles = sum(len(p.triangles) for p in pollers_by_name.values())
    run_id = sqlite_store.start_scan_run(
        config=json.loads(cfg.model_dump_json()),
        exchanges=[s.name for s in sources],
        triangle_count=total_triangles,
    )

    tasks = [
        p.run_loop(cfg.scanner.tier1_interval_sec, make_callback(name))
        for name, p in pollers_by_name.items()
    ]
    tasks.append(_reenumerate_loop(cfg, sources, pollers_by_name))
    tasks.append(tick_loop())

    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()

    import signal
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, main_task.cancel)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            sqlite_store.end_scan_run(run_id)
        except Exception as e:
            log.warning("end_scan_run failed: %s", e)
        await jsonl.close()
        sqlite_store.close()
        await asyncio.gather(*(s.close() for s in sources), return_exceptions=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    cfg = load_config(args.config)
    logging.basicConfig(level=getattr(logging, cfg.output.log_level), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(_run(cfg))


if __name__ == "__main__":
    main()
