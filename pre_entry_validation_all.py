#!/usr/bin/env python3
"""
Pre-entry price action VALIDATION — ALL trades (winners + losers).
Fetches 4h of 1-min candles before each of the last 3000 trades,
computes features, and tests whether signals separate winners from losers.
"""

import pandas as pd
import numpy as np
import requests
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

CSV_PATH = "/Users/vandenboogaard/Claude projects/Claude Paperclip/trade_history.csv"
OUTPUT_PATH = "/Users/vandenboogaard/Claude projects/Claude Paperclip/pre_entry_validation_all.csv"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

# ─── Step 1: Load last 3000 trades ────────────────────────────────────────────

def load_trades(csv_path, n=3000):
    df = pd.read_csv(csv_path)
    df['entry_time_dt'] = pd.to_datetime(df['entry_time'])
    df = df.sort_values('entry_time_dt').tail(n).reset_index(drop=True)

    print(f"Total trades in CSV: {len(pd.read_csv(csv_path))}")
    print(f"Using last {len(df)} trades")
    print(f"Date range: {df['entry_time_dt'].min()} to {df['entry_time_dt'].max()}")
    print(f"\nPnL distribution:")
    print(f"  Mean: ${df['net_pnl_usd'].mean():.4f}")
    print(f"  Median: ${df['net_pnl_usd'].median():.4f}")
    print(f"  Losers (< $0): {(df['net_pnl_usd'] < 0).sum()} ({(df['net_pnl_usd'] < 0).mean()*100:.1f}%)")
    print(f"  Winners (>= $0): {(df['net_pnl_usd'] >= 0).sum()} ({(df['net_pnl_usd'] >= 0).mean()*100:.1f}%)")
    print(f"  Big winners (>= $2): {(df['net_pnl_usd'] >= 2).sum()} ({(df['net_pnl_usd'] >= 2).mean()*100:.1f}%)")
    print(f"  Big losers (<= -$2): {(df['net_pnl_usd'] <= -2).sum()} ({(df['net_pnl_usd'] <= -2).mean()*100:.1f}%)")
    print(f"  Unique symbols: {df['symbol'].nunique()}")

    return df

# ─── Step 2: Fetch candles ────────────────────────────────────────────────────

# Cache symbols known to not be on Binance
skip_symbols = set()

def fetch_candles(symbol, entry_time_str):
    """Fetch 240 1-minute candles ending at entry_time from Binance."""
    if symbol in skip_symbols:
        return None, "skipped_symbol"

    dt = pd.to_datetime(entry_time_str)
    end_ms = int(dt.timestamp() * 1000)
    clean_symbol = ''.join(c for c in symbol if c.isalnum()).upper()

    params = {
        'symbol': clean_symbol,
        'interval': '1m',
        'endTime': end_ms,
        'limit': 240
    }

    try:
        resp = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)
        if resp.status_code == 429:
            time.sleep(30)
            resp = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)

        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"

        data = resp.json()
        if isinstance(data, dict) and 'code' in data:
            if data.get('code') == -1121 or 'Invalid symbol' in str(data.get('msg', '')):
                skip_symbols.add(symbol)
            return None, f"API error: {data.get('msg', 'unknown')}"

        if len(data) < 60:
            return None, f"Only {len(data)} candles"

        candles = []
        for c in data:
            candles.append({
                'open': float(c[1]),
                'high': float(c[2]),
                'low': float(c[3]),
                'close': float(c[4]),
                'volume': float(c[5]),
            })
        return candles, None
    except Exception as e:
        return None, str(e)

def fetch_worker(args):
    """Worker for ThreadPoolExecutor."""
    idx, symbol, entry_time = args
    time.sleep(0.1)  # 100ms between requests
    candles, error = fetch_candles(symbol, entry_time)
    return idx, candles, error

# ─── Step 3: Compute features ────────────────────────────────────────────────

def compute_features(candles):
    """Compute all pre-entry features."""
    closes = np.array([c['close'] for c in candles])
    highs = np.array([c['high'] for c in candles])
    lows = np.array([c['low'] for c in candles])
    volumes = np.array([c['volume'] for c in candles])
    opens = np.array([c['open'] for c in candles])
    n = len(closes)

    # Returns
    returns = np.diff(closes) / closes[:-1] * 100  # in %

    features = {}

    # Volatility (std dev of returns)
    for window, label in [(5, '5m'), (15, '15m'), (60, '60m'), (240, '240m')]:
        w = min(len(returns), window)
        features[f'vol_{label}'] = np.std(returns[-w:]) if w > 1 else np.nan

    # Trend (% price change)
    for window, label in [(5, '5m'), (15, '15m'), (60, '60m')]:
        w = min(n, window)
        if w > 1:
            features[f'trend_{label}'] = ((closes[-1] - closes[-w]) / closes[-w]) * 100
        else:
            features[f'trend_{label}'] = np.nan

    # Volume ratios
    if n >= 30:
        vol_5 = np.sum(volumes[-5:])
        avg_vol_30 = np.mean(volumes[-30:])
        features['vol_ratio_5v30'] = vol_5 / (avg_vol_30 * 5 + 1e-10) if avg_vol_30 > 0 else np.nan
    else:
        features['vol_ratio_5v30'] = np.nan

    if n >= 240:
        avg_vol_30 = np.mean(volumes[-30:])
        avg_vol_prior_210 = np.mean(volumes[-240:-30])
        features['vol_ratio_30v210'] = avg_vol_30 / (avg_vol_prior_210 + 1e-10)
    else:
        features['vol_ratio_30v210'] = np.nan

    # Volume spike
    if n >= 240:
        features['vol_spike_15m'] = np.max(volumes[-15:]) / (np.mean(volumes) + 1e-10)
    elif n >= 60:
        features['vol_spike_15m'] = np.max(volumes[-15:]) / (np.mean(volumes) + 1e-10)
    else:
        features['vol_spike_15m'] = np.nan

    # Price ranges
    for window, label in [(15, '15m'), (60, '60m')]:
        w = min(n, window)
        features[f'range_{label}'] = ((np.max(highs[-w:]) - np.min(lows[-w:])) / closes[-1]) * 100

    # Avg candle size
    if n >= 5:
        sizes = np.abs(closes[-5:] - opens[-5:]) / opens[-5:] * 100
        features['avg_candle_size_5m'] = np.mean(sizes)
    else:
        features['avg_candle_size_5m'] = np.nan

    # MA distance
    if n >= 60:
        ma60 = np.mean(closes[-60:])
        features['ma_distance_60'] = ((closes[-1] - ma60) / ma60) * 100
        features['above_ma_60'] = 1 if closes[-1] > ma60 else 0
    else:
        features['ma_distance_60'] = np.nan
        features['above_ma_60'] = np.nan

    if n >= 240:
        ma240 = np.mean(closes[-240:])
        features['ma_distance_240'] = ((closes[-1] - ma240) / ma240) * 100
    else:
        features['ma_distance_240'] = np.nan

    # % up candles
    for window, label in [(15, '15'), (60, '60')]:
        w = min(n, window)
        up = np.sum(closes[-w:] > opens[-w:])
        features[f'pct_up_candles_{label}'] = (up / w) * 100

    features['n_candles'] = n
    return features

# ─── Step 4: Analysis functions ──────────────────────────────────────────────

def analyze_results(rdf):
    """Full analysis of features vs trade outcomes."""

    feature_cols = [c for c in rdf.columns if c not in
                    ['symbol', 'entry_time', 'net_pnl_usd', 'entry_spread_pct',
                     'n_candles', 'is_winner', 'is_big_winner', 'is_loser']]

    rdf['is_winner'] = (rdf['net_pnl_usd'] >= 0).astype(int)
    rdf['is_big_winner'] = (rdf['net_pnl_usd'] >= 2).astype(int)
    rdf['is_loser'] = (rdf['net_pnl_usd'] < 0).astype(int)

    overall_win_rate = rdf['is_winner'].mean() * 100
    overall_avg_pnl = rdf['net_pnl_usd'].mean()
    overall_big_win_rate = rdf['is_big_winner'].mean() * 100
    overall_loss_rate = rdf['is_loser'].mean() * 100

    print("\n" + "=" * 80)
    print("ANALYSIS: DO PRE-ENTRY SIGNALS SEPARATE WINNERS FROM LOSERS?")
    print("=" * 80)
    print(f"\nBaseline (all {len(rdf)} trades with candle data):")
    print(f"  Win rate: {overall_win_rate:.1f}%")
    print(f"  Avg PnL: ${overall_avg_pnl:.4f}")
    print(f"  Big win rate (>=$2): {overall_big_win_rate:.1f}%")
    print(f"  Loss rate: {overall_loss_rate:.1f}%")

    # ─── 4a: Per-feature quartile analysis ────────────────────────────────
    print("\n" + "-" * 80)
    print("4a) PER-FEATURE QUARTILE ANALYSIS")
    print("-" * 80)

    feature_scores = {}

    for feat in feature_cols:
        valid = rdf[[feat, 'net_pnl_usd', 'is_winner', 'is_big_winner', 'is_loser']].dropna()
        if len(valid) < 100:
            continue

        corr = valid[feat].corr(valid['net_pnl_usd'])

        try:
            valid['Q'] = pd.qcut(valid[feat], 4, labels=['Q1', 'Q2', 'Q3', 'Q4'], duplicates='drop')
        except Exception:
            continue

        q_stats = valid.groupby('Q').agg(
            mean_pnl=('net_pnl_usd', 'mean'),
            win_rate=('is_winner', 'mean'),
            big_win_rate=('is_big_winner', 'mean'),
            loss_rate=('is_loser', 'mean'),
            count=('net_pnl_usd', 'count')
        )
        q_stats['win_rate'] *= 100
        q_stats['big_win_rate'] *= 100
        q_stats['loss_rate'] *= 100

        # Score: max win rate spread across quartiles
        wr_spread = q_stats['win_rate'].max() - q_stats['win_rate'].min()
        pnl_spread = q_stats['mean_pnl'].max() - q_stats['mean_pnl'].min()

        feature_scores[feat] = {
            'corr': corr,
            'wr_spread': wr_spread,
            'pnl_spread': pnl_spread,
            'q_stats': q_stats,
            'best_q_wr': q_stats['win_rate'].idxmax(),
            'best_q_pnl': q_stats['mean_pnl'].idxmax(),
        }

    # Sort by win rate spread
    sorted_feats = sorted(feature_scores.items(), key=lambda x: x[1]['wr_spread'], reverse=True)

    print(f"\n{'Feature':<22} {'Corr':>7} {'WR Spread':>10} {'PnL Spread':>11}  Quartile Win Rates (Q1->Q4)")
    print("-" * 100)
    for feat, info in sorted_feats:
        qs = info['q_stats']
        qwr = " | ".join([f"{qs.loc[q, 'win_rate']:.1f}%" for q in ['Q1', 'Q2', 'Q3', 'Q4'] if q in qs.index])
        print(f"{feat:<22} {info['corr']:>+7.4f} {info['wr_spread']:>9.1f}pp ${info['pnl_spread']:>9.4f}  [{qwr}]")

    # Detailed view of top features
    print("\n\n--- DETAILED VIEW: Top 8 features by win rate spread ---")
    for feat, info in sorted_feats[:8]:
        qs = info['q_stats']
        print(f"\n  {feat} (corr={info['corr']:+.4f}):")
        print(f"  {'Quartile':<6} {'Count':>6} {'Win Rate':>9} {'Big Win':>9} {'Loss Rate':>10} {'Avg PnL':>10}")
        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            if q in qs.index:
                r = qs.loc[q]
                print(f"  {q:<6} {int(r['count']):>6} {r['win_rate']:>8.1f}% {r['big_win_rate']:>8.1f}% {r['loss_rate']:>9.1f}% ${r['mean_pnl']:>9.4f}")

    # ─── 4b: Specific thresholds from prior analysis ─────────────────────
    print("\n\n" + "-" * 80)
    print("4b) TESTING SPECIFIC THRESHOLDS FROM PRIOR (WINNERS-ONLY) ANALYSIS")
    print("-" * 80)

    thresholds = [
        ('vol_5m', 1.01, '>', 'vol_5m > 1.01%'),
        ('vol_ratio_5v30', 2.86, '>', 'vol_ratio_5v30 > 2.86x'),
        ('vol_spike_15m', 8.4, '>', 'vol_spike_15m > 8.4x'),
        ('above_ma_60', 0.5, '>', 'above_ma_60 = 1'),
    ]

    for feat, thresh, direction, label in thresholds:
        if feat not in rdf.columns:
            print(f"\n  {label}: feature not available")
            continue
        valid = rdf[[feat, 'net_pnl_usd', 'is_winner', 'is_big_winner', 'is_loser']].dropna()
        if len(valid) < 50:
            print(f"\n  {label}: insufficient data ({len(valid)} trades)")
            continue

        if direction == '>':
            above = valid[valid[feat] > thresh]
            below = valid[valid[feat] <= thresh]
        else:
            above = valid[valid[feat] < thresh]
            below = valid[valid[feat] >= thresh]

        print(f"\n  {label}:")
        print(f"  {'Group':<25} {'N':>6} {'Win Rate':>9} {'Big Win':>9} {'Loss Rate':>10} {'Avg PnL':>10}")
        for name, subset in [('ABOVE threshold', above), ('BELOW threshold', below)]:
            if len(subset) > 0:
                wr = subset['is_winner'].mean() * 100
                bwr = subset['is_big_winner'].mean() * 100
                lr = subset['is_loser'].mean() * 100
                ap = subset['net_pnl_usd'].mean()
                print(f"  {name:<25} {len(subset):>6} {wr:>8.1f}% {bwr:>8.1f}% {lr:>9.1f}% ${ap:>9.4f}")

        if len(above) > 0 and len(below) > 0:
            wr_diff = above['is_winner'].mean()*100 - below['is_winner'].mean()*100
            pnl_diff = above['net_pnl_usd'].mean() - below['net_pnl_usd'].mean()
            pct_filtered = len(below) / len(valid) * 100
            print(f"  => Win rate difference: {wr_diff:+.1f}pp | PnL diff: ${pnl_diff:+.4f} | Would filter out: {pct_filtered:.1f}%")

    # ─── 4c: Does high vol predict BOTH big wins and big losses? ─────────
    print("\n\n" + "-" * 80)
    print("4c) DO SIGNALS PREDICT BIG WINS *AND* BIG LOSSES? (The survivorship bias check)")
    print("-" * 80)

    rdf['is_big_loss'] = (rdf['net_pnl_usd'] <= -2).astype(int)

    for feat in ['vol_5m', 'vol_15m', 'vol_60m', 'vol_ratio_5v30', 'vol_spike_15m', 'range_15m', 'range_60m']:
        if feat not in rdf.columns:
            continue
        valid = rdf[[feat, 'net_pnl_usd', 'is_big_winner', 'is_big_loss']].dropna()
        if len(valid) < 100:
            continue

        try:
            valid['half'] = pd.qcut(valid[feat], 2, labels=['LOW', 'HIGH'], duplicates='drop')
        except:
            continue

        h = valid[valid['half'] == 'HIGH']
        l = valid[valid['half'] == 'LOW']

        h_bw = h['is_big_winner'].mean() * 100
        h_bl = h['is_big_loss'].mean() * 100
        l_bw = l['is_big_winner'].mean() * 100
        l_bl = l['is_big_loss'].mean() * 100

        verdict = ""
        if h_bw > l_bw and h_bl > l_bl:
            verdict = "BOTH big wins AND big losses increase -> RISKY signal"
        elif h_bw > l_bw and h_bl <= l_bl:
            verdict = "More big wins, same/fewer big losses -> GOOD signal"
        elif h_bw <= l_bw and h_bl > l_bl:
            verdict = "More big losses, fewer big wins -> BAD signal"
        else:
            verdict = "No clear pattern"

        print(f"\n  {feat}:")
        print(f"    HIGH half: big_win={h_bw:.1f}%, big_loss={h_bl:.1f}%, n={len(h)}")
        print(f"    LOW  half: big_win={l_bw:.1f}%, big_loss={l_bl:.1f}%, n={len(l)}")
        print(f"    => {verdict}")

    # ─── 4d: Best single-feature filter ──────────────────────────────────
    print("\n\n" + "-" * 80)
    print("4d) BEST SINGLE-FEATURE FILTER (must keep >= 40% of trades)")
    print("-" * 80)

    best_single = None
    best_single_score = -999
    single_results = []

    for feat in feature_cols:
        valid = rdf[[feat, 'net_pnl_usd', 'is_winner']].dropna()
        if len(valid) < 100:
            continue

        for pct in [20, 25, 30, 33, 40, 50, 60, 67, 70, 75, 80]:
            threshold = np.percentile(valid[feat], pct)

            # Try keeping above threshold
            above = valid[valid[feat] >= threshold]
            if len(above) >= 0.4 * len(valid) and len(above) < len(valid):
                wr = above['is_winner'].mean() * 100
                ap = above['net_pnl_usd'].mean()
                wr_improve = wr - overall_win_rate
                pnl_improve = ap - overall_avg_pnl
                kept_pct = len(above) / len(valid) * 100

                # Score = win rate improvement * sqrt(kept fraction)
                score = wr_improve * np.sqrt(kept_pct / 100)

                single_results.append({
                    'feature': feat, 'direction': '>=', 'threshold': threshold,
                    'percentile': pct, 'kept_n': len(above), 'kept_pct': kept_pct,
                    'win_rate': wr, 'wr_improve': wr_improve,
                    'avg_pnl': ap, 'pnl_improve': pnl_improve, 'score': score
                })

            # Try keeping below threshold
            below = valid[valid[feat] < threshold]
            if len(below) >= 0.4 * len(valid) and len(below) < len(valid):
                wr = below['is_winner'].mean() * 100
                ap = below['net_pnl_usd'].mean()
                wr_improve = wr - overall_win_rate
                pnl_improve = ap - overall_avg_pnl
                kept_pct = len(below) / len(valid) * 100

                score = wr_improve * np.sqrt(kept_pct / 100)

                single_results.append({
                    'feature': feat, 'direction': '<', 'threshold': threshold,
                    'percentile': pct, 'kept_n': len(below), 'kept_pct': kept_pct,
                    'win_rate': wr, 'wr_improve': wr_improve,
                    'avg_pnl': ap, 'pnl_improve': pnl_improve, 'score': score
                })

    single_df = pd.DataFrame(single_results).sort_values('score', ascending=False)

    print(f"\nTop 15 single-feature filters:")
    print(f"{'Feature':<22} {'Dir':>3} {'Threshold':>10} {'Kept%':>6} {'WinRate':>8} {'WR Impr':>8} {'Avg PnL':>9} {'PnL Impr':>9}")
    print("-" * 90)
    for _, row in single_df.head(15).iterrows():
        print(f"{row['feature']:<22} {row['direction']:>3} {row['threshold']:>10.4f} {row['kept_pct']:>5.1f}% {row['win_rate']:>7.1f}% {row['wr_improve']:>+7.1f}pp ${row['avg_pnl']:>8.4f} ${row['pnl_improve']:>+8.4f}")

    best_single = single_df.iloc[0] if len(single_df) > 0 else None

    # ─── 4e: Best 2-feature combo ────────────────────────────────────────
    print("\n\n" + "-" * 80)
    print("4e) BEST 2-FEATURE COMBO FILTER (must keep >= 40% of trades)")
    print("-" * 80)

    # Use top 10 single features for combo search
    top_feats = single_df.drop_duplicates('feature').head(10)['feature'].tolist()

    combo_results = []

    for i, feat1 in enumerate(top_feats):
        for feat2 in top_feats[i+1:]:
            valid = rdf[[feat1, feat2, 'net_pnl_usd', 'is_winner']].dropna()
            if len(valid) < 100:
                continue

            # Get best direction/threshold for each from single analysis
            f1_rows = single_df[single_df['feature'] == feat1].sort_values('score', ascending=False)
            f2_rows = single_df[single_df['feature'] == feat2].sort_values('score', ascending=False)

            if len(f1_rows) == 0 or len(f2_rows) == 0:
                continue

            # Try top 3 thresholds for each
            for _, r1 in f1_rows.head(3).iterrows():
                for _, r2 in f2_rows.head(3).iterrows():
                    if r1['direction'] == '>=':
                        mask1 = valid[feat1] >= r1['threshold']
                    else:
                        mask1 = valid[feat1] < r1['threshold']

                    if r2['direction'] == '>=':
                        mask2 = valid[feat2] >= r2['threshold']
                    else:
                        mask2 = valid[feat2] < r2['threshold']

                    combo = valid[mask1 & mask2]

                    if len(combo) >= 0.4 * len(valid) and len(combo) < len(valid):
                        wr = combo['is_winner'].mean() * 100
                        ap = combo['net_pnl_usd'].mean()
                        wr_improve = wr - overall_win_rate
                        pnl_improve = ap - overall_avg_pnl
                        kept_pct = len(combo) / len(valid) * 100
                        score = wr_improve * np.sqrt(kept_pct / 100)

                        combo_results.append({
                            'feat1': feat1, 'dir1': r1['direction'], 'thresh1': r1['threshold'],
                            'feat2': feat2, 'dir2': r2['direction'], 'thresh2': r2['threshold'],
                            'kept_n': len(combo), 'kept_pct': kept_pct,
                            'win_rate': wr, 'wr_improve': wr_improve,
                            'avg_pnl': ap, 'pnl_improve': pnl_improve, 'score': score
                        })

    if combo_results:
        combo_df = pd.DataFrame(combo_results).sort_values('score', ascending=False)

        print(f"\nTop 10 2-feature combo filters:")
        print(f"{'Features':<45} {'Kept%':>6} {'WinRate':>8} {'WR Impr':>8} {'Avg PnL':>9} {'PnL Impr':>9}")
        print("-" * 95)
        for _, row in combo_df.head(10).iterrows():
            feat_desc = f"{row['feat1']}{row['dir1']}{row['thresh1']:.3f} & {row['feat2']}{row['dir2']}{row['thresh2']:.3f}"
            print(f"{feat_desc:<45} {row['kept_pct']:>5.1f}% {row['win_rate']:>7.1f}% {row['wr_improve']:>+7.1f}pp ${row['avg_pnl']:>8.4f} ${row['pnl_improve']:>+8.4f}")

        best_combo = combo_df.iloc[0]
    else:
        best_combo = None
        print("  No valid combos found")

    # ─── 5: VERDICT ──────────────────────────────────────────────────────
    print("\n\n" + "=" * 80)
    print("5) FINAL VERDICT")
    print("=" * 80)

    print(f"\n  BASELINE:")
    print(f"    Win rate: {overall_win_rate:.1f}%")
    print(f"    Avg PnL: ${overall_avg_pnl:.4f}")
    print(f"    Total trades: {len(rdf)}")

    if best_single is not None:
        print(f"\n  BEST SINGLE FILTER:")
        print(f"    Rule: {best_single['feature']} {best_single['direction']} {best_single['threshold']:.4f}")
        print(f"    Win rate: {best_single['win_rate']:.1f}% ({best_single['wr_improve']:+.1f}pp improvement)")
        print(f"    Avg PnL: ${best_single['avg_pnl']:.4f} (${best_single['pnl_improve']:+.4f} improvement)")
        print(f"    Trades kept: {best_single['kept_pct']:.1f}%")

        meaningful_wr = abs(best_single['wr_improve']) >= 2.0
        meaningful_pnl = best_single['pnl_improve'] > 0

        print(f"\n    Win rate improvement >= 2pp? {'YES' if meaningful_wr else 'NO'} ({best_single['wr_improve']:+.1f}pp)")
        print(f"    PnL improvement positive? {'YES' if meaningful_pnl else 'NO'} (${best_single['pnl_improve']:+.4f})")

    if best_combo is not None:
        print(f"\n  BEST 2-FEATURE COMBO:")
        print(f"    Rule: {best_combo['feat1']} {best_combo['dir1']} {best_combo['thresh1']:.4f}")
        print(f"           AND {best_combo['feat2']} {best_combo['dir2']} {best_combo['thresh2']:.4f}")
        print(f"    Win rate: {best_combo['win_rate']:.1f}% ({best_combo['wr_improve']:+.1f}pp improvement)")
        print(f"    Avg PnL: ${best_combo['avg_pnl']:.4f} (${best_combo['pnl_improve']:+.4f} improvement)")
        print(f"    Trades kept: {best_combo['kept_pct']:.1f}%")

    # Decision logic
    implement = False
    reason = ""

    if best_single is not None:
        if best_single['wr_improve'] >= 3.0 and best_single['pnl_improve'] > 0:
            implement = True
            reason = f"Single filter improves win rate by {best_single['wr_improve']:+.1f}pp with positive PnL impact"
        elif best_combo is not None and best_combo['wr_improve'] >= 3.0 and best_combo['pnl_improve'] > 0:
            implement = True
            reason = f"Combo filter improves win rate by {best_combo['wr_improve']:+.1f}pp with positive PnL impact"
        elif best_single['wr_improve'] >= 2.0 and best_single['pnl_improve'] > 0:
            implement = True
            reason = f"Moderate but consistent improvement: {best_single['wr_improve']:+.1f}pp win rate, ${best_single['pnl_improve']:+.4f} PnL"
        else:
            reason = f"Best single filter only improves win rate by {best_single['wr_improve']:+.1f}pp (need >= 2pp) or PnL is negative"
    else:
        reason = "No valid filters found"

    print(f"\n  ╔══════════════════════════════════════════════════════════╗")
    print(f"  ║  SHOULD WE IMPLEMENT THIS FILTER?  {'YES' if implement else 'NO ':>3}                  ║")
    print(f"  ╚══════════════════════════════════════════════════════════╝")
    print(f"  Reason: {reason}")

    if implement and best_single is not None:
        print(f"\n  RECOMMENDED IMPLEMENTATION:")
        print(f"    Filter: {best_single['feature']} {best_single['direction']} {best_single['threshold']:.4f}")
        print(f"    Expected impact:")
        print(f"      - Win rate: {overall_win_rate:.1f}% -> {best_single['win_rate']:.1f}%")
        print(f"      - Avg PnL: ${overall_avg_pnl:.4f} -> ${best_single['avg_pnl']:.4f}")
        print(f"      - Trades filtered out: {100-best_single['kept_pct']:.1f}%")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("PRE-ENTRY VALIDATION: ALL TRADES (Winners + Losers)")
    print("Last 3000 trades — do pre-entry signals actually predict outcomes?")
    print("=" * 80)

    # Load
    print("\n--- Step 1: Loading trades ---")
    df = load_trades(CSV_PATH, 3000)

    # Fetch candles with ThreadPoolExecutor
    print("\n--- Step 2: Fetching candles from Binance (10 threads, 100ms/req) ---")

    tasks = [(i, row['symbol'], row['entry_time']) for i, row in df.iterrows()]

    results = {}
    success = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_worker, t): t for t in tasks}

        for i, future in enumerate(as_completed(futures)):
            idx, candles, error = future.result()

            if candles is not None:
                results[idx] = candles
                success += 1
            else:
                fail += 1

            total = i + 1
            if total % 200 == 0 or total == len(tasks):
                print(f"  Progress: {total}/{len(tasks)} ({success} ok, {fail} failed, {len(skip_symbols)} symbols skipped)")

    print(f"\n  Final: {success} success, {fail} failed")
    print(f"  Skipped symbols: {skip_symbols}")

    # Compute features
    print("\n--- Step 3: Computing features ---")
    rows = []
    for idx, candles in results.items():
        features = compute_features(candles)
        features['symbol'] = df.loc[idx, 'symbol']
        features['entry_time'] = df.loc[idx, 'entry_time']
        features['net_pnl_usd'] = df.loc[idx, 'net_pnl_usd']
        features['entry_spread_pct'] = df.loc[idx, 'entry_spread_pct']
        rows.append(features)

    rdf = pd.DataFrame(rows)
    print(f"  Computed features for {len(rdf)} trades")

    # Save raw data
    rdf.to_csv(OUTPUT_PATH, index=False)
    print(f"  Saved to {OUTPUT_PATH}")

    # Analyze
    analyze_results(rdf)

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == '__main__':
    main()
