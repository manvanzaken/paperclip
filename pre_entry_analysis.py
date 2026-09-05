#!/usr/bin/env python3
"""
Pre-entry price action analysis for crypto arbitrage trades.
Fetches 4h of 1-min candles before each trade entry from Binance,
computes features, and correlates with trade outcomes.
"""

import pandas as pd
import numpy as np
import requests
import time
import json
import sys
from datetime import datetime, timezone
from collections import defaultdict

CSV_PATH = "/Users/vandenboogaard/Claude projects/Claude Paperclip/trade_history.csv"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
SLEEP_BETWEEN_REQUESTS = 0.15  # 150ms between requests

# ─── Step 1: Load and sample trades ───────────────────────────────────────────

def load_and_sample(csv_path):
    df = pd.read_csv(csv_path)
    print(f"Total trades loaded: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print(f"\nPnL distribution:")
    print(f"  Mean: ${df['net_pnl_usd'].mean():.4f}")
    print(f"  Median: ${df['net_pnl_usd'].median():.4f}")
    print(f"  Min: ${df['net_pnl_usd'].min():.2f}, Max: ${df['net_pnl_usd'].max():.2f}")
    print(f"  Losers (< $0): {(df['net_pnl_usd'] < 0).sum()}")
    print(f"  Small winners ($0-$0.50): {((df['net_pnl_usd'] >= 0) & (df['net_pnl_usd'] < 0.5)).sum()}")
    print(f"  Medium winners ($0.50-$2): {((df['net_pnl_usd'] >= 0.5) & (df['net_pnl_usd'] < 2)).sum()}")
    print(f"  Big winners (>= $2): {(df['net_pnl_usd'] >= 2).sum()}")

    print(f"\nUnique symbols: {df['symbol'].nunique()}")
    print(f"Top 10 symbols: {df['symbol'].value_counts().head(10).to_dict()}")

    # Parse entry_time
    df['entry_time_dt'] = pd.to_datetime(df['entry_time'])

    # Define tiers
    big_winners = df[df['net_pnl_usd'] >= 2].copy()
    med_winners = df[(df['net_pnl_usd'] >= 0.5) & (df['net_pnl_usd'] < 2)].copy()
    small_winners = df[(df['net_pnl_usd'] >= 0) & (df['net_pnl_usd'] < 0.5)].copy()
    losers = df[df['net_pnl_usd'] < 0].copy()

    def sample_diverse(tier_df, n, tier_name):
        """Sample n trades spread across symbols and times."""
        if len(tier_df) <= n:
            print(f"  {tier_name}: using all {len(tier_df)} trades (wanted {n})")
            return tier_df

        # Sort by entry_time to spread across time
        tier_df = tier_df.sort_values('entry_time_dt')

        # Try to get diversity in symbols: stratified sampling
        indices = []
        for sym, grp in tier_df.groupby('symbol'):
            k = max(1, int(n * len(grp) / len(tier_df)))
            k = min(k, len(grp))
            indices.extend(grp.sample(k, random_state=42).index.tolist())

        picked = tier_df.loc[indices]

        if len(picked) < n:
            remaining = tier_df[~tier_df.index.isin(picked.index)]
            extra = remaining.sample(min(n - len(picked), len(remaining)), random_state=42)
            picked = pd.concat([picked, extra])
        elif len(picked) > n:
            picked = picked.sample(n, random_state=42)

        print(f"  {tier_name}: sampled {len(picked)} trades across {picked['symbol'].nunique()} symbols")
        return picked

    np.random.seed(42)
    s1 = sample_diverse(big_winners, 50, "Big winners")
    s2 = sample_diverse(med_winners, 50, "Medium winners")
    s3 = sample_diverse(small_winners, 50, "Small winners")
    s4 = sample_diverse(losers, 50, "Losers")

    sampled = pd.concat([s1, s2, s3, s4])
    sampled['tier'] = 'unknown'
    sampled.loc[sampled['net_pnl_usd'] >= 2, 'tier'] = 'big_winner'
    sampled.loc[(sampled['net_pnl_usd'] >= 0.5) & (sampled['net_pnl_usd'] < 2), 'tier'] = 'med_winner'
    sampled.loc[(sampled['net_pnl_usd'] >= 0) & (sampled['net_pnl_usd'] < 0.5), 'tier'] = 'small_winner'
    sampled.loc[sampled['net_pnl_usd'] < 0, 'tier'] = 'loser'

    print(f"\nTotal sampled: {len(sampled)}")
    return sampled

# ─── Step 2: Fetch candles from Binance ───────────────────────────────────────

def fetch_candles(symbol, entry_time_str):
    """Fetch 240 1-minute candles ending at entry_time from Binance."""
    # Parse the entry time and convert to ms timestamp
    dt = pd.to_datetime(entry_time_str)
    end_ms = int(dt.timestamp() * 1000)

    # Clean symbol - remove any non-alphanumeric
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
            # Rate limited - wait and retry once
            print(f"  Rate limited, waiting 30s...")
            time.sleep(30)
            resp = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)

        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"

        data = resp.json()
        if isinstance(data, dict) and 'code' in data:
            return None, f"API error: {data.get('msg', 'unknown')}"

        if len(data) < 10:
            return None, f"Only {len(data)} candles returned"

        # Parse candles: [open_time, open, high, low, close, volume, close_time, ...]
        candles = []
        for c in data:
            candles.append({
                'open_time': c[0],
                'open': float(c[1]),
                'high': float(c[2]),
                'low': float(c[3]),
                'close': float(c[4]),
                'volume': float(c[5]),
                'close_time': c[6],
                'quote_volume': float(c[7]),
                'trades': int(c[8])
            })
        return candles, None
    except Exception as e:
        return None, str(e)

# ─── Step 3: Compute features from candles ────────────────────────────────────

def compute_features(candles):
    """Compute pre-entry price action features from candle data."""
    closes = np.array([c['close'] for c in candles])
    highs = np.array([c['high'] for c in candles])
    lows = np.array([c['low'] for c in candles])
    volumes = np.array([c['volume'] for c in candles])
    opens = np.array([c['open'] for c in candles])

    n = len(closes)

    # Returns (close-to-close)
    returns = np.diff(closes) / closes[:-1]

    features = {}

    # --- Volatility: std dev of returns ---
    for window, label in [(min(n-1, 240), '240m'), (min(n-1, 60), '60m'),
                           (min(n-1, 15), '15m'), (min(n-1, 5), '5m')]:
        if window > 0:
            features[f'vol_{label}'] = np.std(returns[-window:]) * 100  # in %
        else:
            features[f'vol_{label}'] = np.nan

    # --- Trend: % price change over period ---
    for window, label in [(min(n, 240), '240m'), (min(n, 60), '60m'),
                           (min(n, 15), '15m'), (min(n, 5), '5m')]:
        if window > 1:
            features[f'trend_{label}'] = ((closes[-1] - closes[-window]) / closes[-window]) * 100
        else:
            features[f'trend_{label}'] = np.nan

    # --- Momentum: acceleration (rate of change of rate of change) ---
    # Compare recent 15m trend vs prior 15m trend
    if n >= 30:
        recent_trend = (closes[-1] - closes[-15]) / closes[-15]
        prior_trend = (closes[-15] - closes[-30]) / closes[-30]
        features['momentum_15m'] = (recent_trend - prior_trend) * 100
    else:
        features['momentum_15m'] = np.nan

    if n >= 120:
        recent_60 = (closes[-1] - closes[-60]) / closes[-60]
        prior_60 = (closes[-60] - closes[-120]) / closes[-120]
        features['momentum_60m'] = (recent_60 - prior_60) * 100
    else:
        features['momentum_60m'] = np.nan

    # --- Volume pattern: last 30 min vs prior 3.5 hours ---
    if n >= 240:
        vol_recent_30 = np.mean(volumes[-30:])
        vol_prior = np.mean(volumes[-240:-30])
        features['vol_ratio_30v210'] = vol_recent_30 / (vol_prior + 1e-10)
    elif n >= 60:
        vol_recent_30 = np.mean(volumes[-30:])
        vol_prior = np.mean(volumes[:-30])
        features['vol_ratio_30v210'] = vol_recent_30 / (vol_prior + 1e-10)
    else:
        features['vol_ratio_30v210'] = np.nan

    # Volume trend: last 5 min vs last 30 min
    if n >= 30:
        features['vol_ratio_5v30'] = np.mean(volumes[-5:]) / (np.mean(volumes[-30:]) + 1e-10)
    else:
        features['vol_ratio_5v30'] = np.nan

    # --- Price range: (high-low)/close ---
    for window, label in [(min(n, 60), '60m'), (min(n, 15), '15m')]:
        if window > 0:
            h = np.max(highs[-window:])
            l = np.min(lows[-window:])
            features[f'range_{label}'] = ((h - l) / closes[-1]) * 100
        else:
            features[f'range_{label}'] = np.nan

    # --- Candle pattern: % of up candles ---
    for window, label in [(min(n, 60), '60m'), (min(n, 15), '15m')]:
        if window > 0:
            up_candles = sum(1 for i in range(-window, 0) if closes[i] >= opens[i])
            features[f'up_pct_{label}'] = (up_candles / window) * 100
        else:
            features[f'up_pct_{label}'] = np.nan

    # --- Mean reversion signal: distance from moving average ---
    if n >= 60:
        ma60 = np.mean(closes[-60:])
        features['dist_ma60'] = ((closes[-1] - ma60) / ma60) * 100
    else:
        features['dist_ma60'] = np.nan

    if n >= 240:
        ma240 = np.mean(closes[-240:])
        features['dist_ma240'] = ((closes[-1] - ma240) / ma240) * 100
    else:
        features['dist_ma240'] = np.nan

    # --- Absolute volatility (price-based, not return-based) ---
    if n >= 60:
        features['abs_vol_60m'] = (np.std(closes[-60:]) / np.mean(closes[-60:])) * 100
    else:
        features['abs_vol_60m'] = np.nan

    # --- Recent candle size (absolute) ---
    if n >= 5:
        recent_sizes = [abs(closes[-i] - opens[-i]) / closes[-i] * 100 for i in range(1, 6)]
        features['avg_candle_size_5m'] = np.mean(recent_sizes)
    else:
        features['avg_candle_size_5m'] = np.nan

    # --- Volume spike: max volume in last 15 min / avg volume ---
    if n >= 60:
        features['vol_spike_15m'] = np.max(volumes[-15:]) / (np.mean(volumes[-60:]) + 1e-10)
    else:
        features['vol_spike_15m'] = np.nan

    # --- Consecutive direction: how many consecutive up/down candles at end ---
    consec = 0
    if n >= 2:
        direction = 1 if closes[-1] >= opens[-1] else -1
        for i in range(1, min(n, 20)+1):
            c_dir = 1 if closes[-i] >= opens[-i] else -1
            if c_dir == direction:
                consec += 1
            else:
                break
    features['consec_direction'] = consec * (1 if n >= 2 and closes[-1] >= opens[-1] else -1)

    return features

# ─── Step 4: Main pipeline ───────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("PRE-ENTRY PRICE ACTION ANALYSIS FOR CRYPTO ARBITRAGE TRADES")
    print("=" * 70)

    # Load and sample
    print("\n--- Loading and sampling trades ---")
    sampled = load_and_sample(CSV_PATH)

    # Fetch candles and compute features
    print("\n--- Fetching candles from Binance ---")
    results = []
    success_count = 0
    fail_count = 0
    skip_symbols = set()

    for idx, (_, row) in enumerate(sampled.iterrows()):
        symbol = row['symbol']
        entry_time = row['entry_time']

        if symbol in skip_symbols:
            fail_count += 1
            continue

        sys.stdout.write(f"\r  [{idx+1}/{len(sampled)}] {symbol} ... ")
        sys.stdout.flush()

        candles, error = fetch_candles(symbol, entry_time)

        if candles is None:
            if error and ("Invalid symbol" in str(error) or "-1121" in str(error)):
                skip_symbols.add(symbol)
                print(f"SKIP (not on Binance: {symbol})")
            else:
                print(f"FAIL: {error}")
            fail_count += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            continue

        features = compute_features(candles)
        features['symbol'] = symbol
        features['entry_time'] = entry_time
        features['tier'] = row['tier']
        features['net_pnl_usd'] = row['net_pnl_usd']
        features['entry_spread_pct'] = row['entry_spread_pct']
        features['n_candles'] = len(candles)

        results.append(features)
        success_count += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    print(f"\n\n  Fetched: {success_count} success, {fail_count} failed")
    print(f"  Symbols not on Binance: {skip_symbols}")

    if success_count < 20:
        print("\nNot enough data to analyze. Exiting.")
        return

    # ─── Step 5: Analysis ─────────────────────────────────────────────────────

    rdf = pd.DataFrame(results)

    print("\n" + "=" * 70)
    print("ANALYSIS RESULTS")
    print("=" * 70)

    # Feature columns
    feature_cols = [c for c in rdf.columns if c not in ['symbol', 'entry_time', 'tier', 'net_pnl_usd', 'entry_spread_pct', 'n_candles']]

    # --- 5a: Summary stats by tier ---
    print("\n--- Feature Means by PnL Tier ---")
    tier_order = ['loser', 'small_winner', 'med_winner', 'big_winner']
    tier_counts = rdf['tier'].value_counts()
    print(f"\nSample sizes: {dict(tier_counts)}")

    tier_means = rdf.groupby('tier')[feature_cols].mean()
    tier_means = tier_means.reindex(tier_order)

    # Print as formatted table
    print(f"\n{'Feature':<25} {'Loser':>12} {'Small Win':>12} {'Med Win':>12} {'Big Win':>12} {'Corr w/PnL':>12}")
    print("-" * 87)

    correlations = {}
    for feat in feature_cols:
        valid = rdf[[feat, 'net_pnl_usd']].dropna()
        if len(valid) >= 10:
            corr = valid[feat].corr(valid['net_pnl_usd'])
            correlations[feat] = corr
        else:
            correlations[feat] = np.nan

        vals = [tier_means.loc[t, feat] if t in tier_means.index else np.nan for t in tier_order]
        val_strs = [f"{v:12.4f}" if not np.isnan(v) else f"{'N/A':>12}" for v in vals]
        corr_str = f"{correlations[feat]:12.4f}" if not np.isnan(correlations[feat]) else f"{'N/A':>12}"
        print(f"{feat:<25} {''.join(val_strs)} {corr_str}")

    # --- 5b: Top correlations ---
    print("\n\n--- Top Features Correlated with PnL (absolute correlation) ---")
    sorted_corr = sorted(correlations.items(), key=lambda x: abs(x[1]) if not np.isnan(x[1]) else 0, reverse=True)
    for feat, corr in sorted_corr[:15]:
        direction = "HIGHER is better" if corr > 0 else "LOWER is better"
        print(f"  {feat:<25} r = {corr:+.4f}  ({direction})")

    # --- 5c: Volatility analysis ---
    print("\n\n--- Volatility Sweet Spot Analysis ---")
    for vol_feat in ['vol_240m', 'vol_60m', 'vol_15m', 'vol_5m']:
        valid = rdf[[vol_feat, 'net_pnl_usd', 'tier']].dropna()
        if len(valid) < 10:
            continue

        # Quartile analysis
        valid['quartile'] = pd.qcut(valid[vol_feat], 4, labels=['Q1_low', 'Q2', 'Q3', 'Q4_high'], duplicates='drop')
        q_stats = valid.groupby('quartile')['net_pnl_usd'].agg(['mean', 'median', 'count'])
        print(f"\n  {vol_feat} quartiles:")
        for q_name, row_data in q_stats.iterrows():
            print(f"    {q_name}: mean PnL ${row_data['mean']:.4f}, median ${row_data['median']:.4f}, n={int(row_data['count'])}")

        # Find the sweet spot
        best_q = q_stats['mean'].idxmax()
        worst_q = q_stats['mean'].idxmin()
        print(f"    => Best quartile: {best_q}, Worst: {worst_q}")

    # --- 5d: Trend direction analysis ---
    print("\n\n--- Trend Direction Analysis ---")
    for trend_feat in ['trend_240m', 'trend_60m', 'trend_15m', 'trend_5m']:
        valid = rdf[[trend_feat, 'net_pnl_usd']].dropna()
        if len(valid) < 10:
            continue

        up_trades = valid[valid[trend_feat] > 0]
        down_trades = valid[valid[trend_feat] <= 0]

        print(f"\n  {trend_feat}:")
        print(f"    Uptrend entries ({len(up_trades)}): mean PnL ${up_trades['net_pnl_usd'].mean():.4f}")
        print(f"    Downtrend entries ({len(down_trades)}): mean PnL ${down_trades['net_pnl_usd'].mean():.4f}")

    # --- 5e: Volume spike analysis ---
    print("\n\n--- Volume Analysis ---")
    for vol_feat in ['vol_ratio_30v210', 'vol_ratio_5v30', 'vol_spike_15m']:
        valid = rdf[[vol_feat, 'net_pnl_usd']].dropna()
        if len(valid) < 10:
            continue

        try:
            valid['quartile'] = pd.qcut(valid[vol_feat], 4, labels=['Q1_low', 'Q2', 'Q3', 'Q4_high'], duplicates='drop')
            q_stats = valid.groupby('quartile')['net_pnl_usd'].agg(['mean', 'median', 'count'])
            print(f"\n  {vol_feat} quartiles:")
            for q_name, row_data in q_stats.iterrows():
                print(f"    {q_name}: mean PnL ${row_data['mean']:.4f}, median ${row_data['median']:.4f}, n={int(row_data['count'])}")
        except Exception:
            print(f"\n  {vol_feat}: insufficient variation for quartile analysis")

    # --- 5f: Mean reversion analysis ---
    print("\n\n--- Mean Reversion Signal Analysis ---")
    for mr_feat in ['dist_ma60', 'dist_ma240']:
        valid = rdf[[mr_feat, 'net_pnl_usd']].dropna()
        if len(valid) < 10:
            continue

        # Split by distance buckets
        abs_dist = valid[mr_feat].abs()
        try:
            valid['dist_bucket'] = pd.qcut(abs_dist, 3, labels=['near_MA', 'moderate', 'far_from_MA'], duplicates='drop')
            q_stats = valid.groupby('dist_bucket')['net_pnl_usd'].agg(['mean', 'median', 'count'])
            print(f"\n  {mr_feat} (absolute distance from MA):")
            for q_name, row_data in q_stats.iterrows():
                print(f"    {q_name}: mean PnL ${row_data['mean']:.4f}, median ${row_data['median']:.4f}, n={int(row_data['count'])}")
        except Exception:
            pass

        # Also check direction
        above = valid[valid[mr_feat] > 0]
        below = valid[valid[mr_feat] <= 0]
        print(f"    Above MA ({len(above)}): mean PnL ${above['net_pnl_usd'].mean():.4f}")
        print(f"    Below MA ({len(below)}): mean PnL ${below['net_pnl_usd'].mean():.4f}")

    # --- 5g: Entry spread vs pre-entry conditions ---
    print("\n\n--- Entry Spread vs Pre-Entry Volatility ---")
    valid = rdf[['entry_spread_pct', 'vol_60m', 'net_pnl_usd']].dropna()
    if len(valid) >= 10:
        corr_spread_vol = valid['entry_spread_pct'].corr(valid['vol_60m'])
        corr_spread_pnl = valid['entry_spread_pct'].corr(valid['net_pnl_usd'])
        print(f"  Correlation(entry_spread, vol_60m) = {corr_spread_vol:.4f}")
        print(f"  Correlation(entry_spread, net_pnl) = {corr_spread_pnl:.4f}")

    # --- 5h: Combined signal analysis ---
    print("\n\n--- Combined Signal Analysis ---")
    # High vol + high volume = ?
    valid = rdf[['vol_60m', 'vol_ratio_30v210', 'trend_60m', 'dist_ma60', 'net_pnl_usd']].dropna()
    if len(valid) >= 20:
        median_vol = valid['vol_60m'].median()
        median_volr = valid['vol_ratio_30v210'].median()

        # High vol + increasing volume
        hv_hvolr = valid[(valid['vol_60m'] > median_vol) & (valid['vol_ratio_30v210'] > median_volr)]
        hv_lvolr = valid[(valid['vol_60m'] > median_vol) & (valid['vol_ratio_30v210'] <= median_volr)]
        lv_hvolr = valid[(valid['vol_60m'] <= median_vol) & (valid['vol_ratio_30v210'] > median_volr)]
        lv_lvolr = valid[(valid['vol_60m'] <= median_vol) & (valid['vol_ratio_30v210'] <= median_volr)]

        print(f"  High volatility + Rising volume ({len(hv_hvolr)}): mean PnL ${hv_hvolr['net_pnl_usd'].mean():.4f}")
        print(f"  High volatility + Falling volume ({len(hv_lvolr)}): mean PnL ${hv_lvolr['net_pnl_usd'].mean():.4f}")
        print(f"  Low volatility + Rising volume ({len(lv_hvolr)}): mean PnL ${lv_hvolr['net_pnl_usd'].mean():.4f}")
        print(f"  Low volatility + Falling volume ({len(lv_lvolr)}): mean PnL ${lv_lvolr['net_pnl_usd'].mean():.4f}")

    # --- 5i: Specific thresholds ---
    print("\n\n--- Threshold Analysis (Actionable Rules) ---")
    for feat in sorted_corr[:8]:
        feat_name = feat[0]
        valid = rdf[[feat_name, 'net_pnl_usd', 'tier']].dropna()
        if len(valid) < 20:
            continue

        # Try different percentile thresholds
        best_threshold = None
        best_diff = 0
        for pct in [25, 33, 50, 67, 75]:
            threshold = np.percentile(valid[feat_name], pct)
            above = valid[valid[feat_name] >= threshold]
            below = valid[valid[feat_name] < threshold]
            if len(above) >= 5 and len(below) >= 5:
                diff = above['net_pnl_usd'].mean() - below['net_pnl_usd'].mean()
                if abs(diff) > abs(best_diff):
                    best_diff = diff
                    best_threshold = (pct, threshold, len(above), above['net_pnl_usd'].mean(),
                                     len(below), below['net_pnl_usd'].mean())

        if best_threshold:
            pct, thr, n_above, mean_above, n_below, mean_below = best_threshold
            direction = ">=" if mean_above > mean_below else "<"
            better_mean = max(mean_above, mean_below)
            worse_mean = min(mean_above, mean_below)
            print(f"\n  {feat_name}:")
            print(f"    Threshold at p{pct} = {thr:.4f}")
            print(f"    Above (n={n_above}): mean PnL ${mean_above:.4f}")
            print(f"    Below (n={n_below}): mean PnL ${mean_below:.4f}")
            print(f"    Difference: ${abs(best_diff):.4f}")

            # Win rate comparison
            above_set = valid[valid[feat_name] >= thr]
            below_set = valid[valid[feat_name] < thr]
            above_wr = (above_set['tier'] != 'loser').mean() * 100
            below_wr = (below_set['tier'] != 'loser').mean() * 100
            print(f"    Win rate above: {above_wr:.1f}%, below: {below_wr:.1f}%")

    # Save raw results
    output_path = "/Users/vandenboogaard/Claude projects/Claude Paperclip/pre_entry_features.csv"
    rdf.to_csv(output_path, index=False)
    print(f"\n\nRaw feature data saved to: {output_path}")

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)

if __name__ == '__main__':
    main()
