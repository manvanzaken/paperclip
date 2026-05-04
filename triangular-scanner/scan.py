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


async def _validate_loop(src, ws_managers):
    """Periodically compare in-memory books to REST tickers; log WARN on mismatch."""
    from triscan.validate import compare_book_to_quote
    while True:
        await asyncio.sleep(30)
        wm = ws_managers.get(src.name)
        if wm is None:
            continue
        symbols = list(wm._fanout.keys())
        if not symbols:
            continue
        try:
            quotes = await src.fetch_tickers(symbols)
        except Exception as e:
            log.warning("validate: fetch_tickers failed for %s: %s", src.name, e)
            continue
        for sym, quote in quotes.items():
            # find any pipeline that has this symbol's book
            book = None
            for ts_map in []:  # populated below
                pass
            if book is None:
                continue
            mm = compare_book_to_quote(book, quote, max_bp=5.0)
            if mm is not None:
                log.warning("validate: %s/%s book mismatch field=%s book=%.6f quote=%.6f diff_bp=%.2f",
                            src.name, sym, mm.field, mm.book_value, mm.quote_value, mm.diff_bp)


async def _run(cfg, args=None):
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

    use_live_console = cfg.output.console.enabled

    live_console = None
    if use_live_console:
        from triscan.output.console import LiveConsole, ConsoleRow
        live_console = LiveConsole(
            refresh_ms=cfg.output.console.refresh_ms,
            top_n=cfg.output.console.top_n,
            show_candidates=cfg.output.console.show_candidates,
        )
        live_console.__enter__()

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
            if not use_live_console:
                o = evt["opportunity"]
                if evt["type"] == "OpportunityOpen":
                    print(f"[{_name}] OPEN  {o['triangle_id']} net={o['open_net_edge_pct']:+.4f}% "
                          f"size=${o['peak_executable_size_usd']:.0f} profit=${o['peak_executable_profit_usd']:.2f}")
                else:
                    print(f"[{_name}] CLOSE {o['triangle_id']} life={o['lifetime_ms']}ms "
                          f"peak_net={o['peak_net_edge_pct']:+.4f}% peak_profit=${o['peak_executable_profit_usd']:.2f} "
                          f"reason={o['closed_reason']}")
            else:
                o = evt["opportunity"]
                if evt["type"] == "OpportunityOpen":
                    log.debug("[%s] OPEN %s net=%+.4f%%", _name, o['triangle_id'], o['open_net_edge_pct'])
                else:
                    log.debug("[%s] CLOSE %s reason=%s", _name, o['triangle_id'], o.get('closed_reason'))

        async def on_state_change(triangle, state, info, _console=live_console):
            if _console is None:
                return
            from triscan.output.console import ConsoleRow
            from triscan.models import OpportunityState as _OState
            if state == _OState.IDLE:
                _console.remove_row(triangle.id)
            else:
                _console.update_row(ConsoleRow(
                    triangle_id=triangle.id,
                    state=state.value,
                    net_edge_pct=info.get("net_edge_pct", 0.0),
                    size_usd=info.get("executable_size_usd", 0.0),
                    profit_usd=info.get("executable_profit_usd", 0.0),
                    bottleneck_leg=info.get("bottleneck_leg", -1),
                    age_ms=0,
                ))

        pipelines[src.name] = Pipeline(
            triangles=pollers_by_name[src.name].triangles,
            ws_manager=ws_managers[src.name],
            config=pcfg,
            on_opportunity=on_opp,
            on_state_change=on_state_change if use_live_console else None,
            data_dir=data_dir,
        )
        ws_managers[src.name]._on_symbol_failure = pipelines[src.name].notify_ws_failed  # late-bind

    def make_callback(name):
        async def cb(results):
            results.sort(key=lambda r: r.gross_edge_pct, reverse=True)
            top = results[: cfg.output.console.top_n]
            if not use_live_console:
                if top:
                    print(f"--- {name} tier1 (top {len(top)}) ---")
                    for r in top:
                        print(f"  {r.triangle.id}  gross={r.gross_edge_pct:+.4f}%  net={r.net_edge_pct:+.4f}%")
            else:
                if top:
                    log.debug("--- %s tier1 (top %d) ---", name, len(top))
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
    for p in pipelines.values():
        tasks.append(p.status_writer())
    if args is not None and getattr(args, "validate_books", False):
        for src in sources:
            tasks.append(_validate_loop(src, ws_managers))

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
        if live_console is not None:
            try:
                live_console.__exit__(None, None, None)
            except Exception:
                pass
        try:
            sqlite_store.end_scan_run(run_id)
        except Exception as e:
            log.warning("end_scan_run failed: %s", e)
        await jsonl.close()
        sqlite_store.close()
        await asyncio.gather(*(s.close() for s in sources), return_exceptions=True)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")

    scan_p = sub.add_parser("scan")
    scan_p.add_argument("--config", default="config.yaml")
    scan_p.add_argument("--validate-books", action="store_true")

    rb = sub.add_parser("rebuild-db")
    rb.add_argument("--config", default="config.yaml")

    args = ap.parse_args()
    if args.cmd is None or args.cmd == "scan":
        # If no cmd, set defaults that scan would have
        if not hasattr(args, "config"):
            args.config = "config.yaml"
        if not hasattr(args, "validate_books"):
            args.validate_books = False
        cfg = load_config(args.config)
        logging.basicConfig(level=getattr(logging, cfg.output.log_level), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        try:
            asyncio.run(_run(cfg, args))
        except KeyboardInterrupt:
            sys.exit(0)
    elif args.cmd == "rebuild-db":
        cfg = load_config(args.config)
        logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s: %(message)s")
        from triscan.storage.sqlite import SqliteStore
        store = SqliteStore(cfg.storage.sqlite_path)
        n = store.rebuild_from_jsonl(cfg.storage.data_dir)
        store.close()
        print(f"rebuilt {n} opportunities into {cfg.storage.sqlite_path}")


if __name__ == "__main__":
    main()
