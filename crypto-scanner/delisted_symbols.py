"""
Comprehensive list of symbols delisted or announced for delisting across major
crypto exchanges (Binance, Bybit, Gate.io, Bitget, OKX, MEXC) during the period
January 2026 – April 2026.

Format: XXXUSDT (unified symbol format)

Sources (collected April 1, 2026):
- Binance announcements (spot + futures delistings Jan–Apr 2026)
- Bybit announcements (spot + perpetual delistings Jan–Mar 2026)
- OKX announcements (spot delistings Jan–Mar 2026)
- Bitget announcements (spot + futures delistings Jan–Apr 2026)
- MEXC announcements (spot + futures delistings Jan–Mar 2026)
- Gate.io announcements (spot delistings Jan 2026)

Note: Some symbols may have been delisted on only one exchange while remaining
active on others. Symbols delisted on MULTIPLE exchanges are especially risky
and are flagged separately below.
"""

# ─────────────────────────────────────────────────────────────────────────────
# BINANCE delistings (Jan–Apr 2026)
# ─────────────────────────────────────────────────────────────────────────────

# Binance — Futures perpetual contract delistings
BINANCE_FUTURES_DELISTED = [
    "42USDT",        # Jan 30, 2026 — low-liquidity perp
    "COMMONUSDT",    # Jan 30, 2026 — low-liquidity perp
    "CUDISUSDT",     # Jan 30, 2026 — low-liquidity perp
    "EPTUSDT",       # Jan 30, 2026 — low-liquidity perp
    "BIDUSDT",       # Jan 2026 — perp contract
    "DMCUSDT",       # Jan 2026 — perp contract
    "ZRCUSDT",       # Jan 2026 — perp contract
    "TANSSIUSDT",    # Jan 2026 — perp contract (listed as TANSSI)
    "RVVUSDT",       # Feb 10, 2026 — perp contract
    "YALAUSDT",      # Feb 10, 2026 — perp contract
    "VFYUSDT",       # Mar 2026 — perp contract
    "AIAUSDT",       # delisted perp
]

# Binance — Full spot delistings (token removed from exchange, Apr 1 2026)
BINANCE_SPOT_DELISTED = [
    "A2ZUSDT",       # Apr 1, 2026 — full delist (Arena-Z)
    "FORTHUSDT",     # Apr 1, 2026 — full delist (Ampleforth Governance)
    "HOOKUSDT",      # Apr 1, 2026 — full delist (Hooked Protocol)
    "IDEXUSDT",      # Apr 1, 2026 — full delist
    "LRCUSDT",       # Apr 1, 2026 — full delist (Loopring)
    "NTRNUSDT",      # Apr 1, 2026 — full delist (Neutron)
    "RDNTUSDT",      # Apr 1, 2026 — full delist (Radiant Capital)
    "SXPUSDT",       # Apr 1, 2026 — full delist (Solar)
    "STGUSDT",       # Mar 18, 2026 — Binance.US delist (Stargate Finance)
    "LTOUSDT",       # Mar 18, 2026 — Binance.US delist (LTO Network)
]

# Binance — Monitoring tag / at risk of delisting
BINANCE_MONITORING_TAG = [
    "ACAUSDT",       # Jan 2, 2026 — monitoring tag
    "DUSDT",         # Jan 2, 2026 — monitoring tag (DAR Open Network)
    "DATAUSDT",      # Jan 2, 2026 — monitoring tag (Streamr)
    "FLOWUSDT",      # Jan 2, 2026 — monitoring tag
]

# ─────────────────────────────────────────────────────────────────────────────
# OKX delistings (Jan–Mar 2026)
# ─────────────────────────────────────────────────────────────────────────────

OKX_DELISTED = [
    # January 2026 — spot pair removals
    "ULTIUSDT",      # Jan 27/30, 2026
    "GEARUSDT",      # Jan 27/30, 2026
    "VRAUSDT",       # Jan 27/30, 2026
    "DAOUSDT",       # Jan 27/30, 2026
    "CXTUSDT",       # Jan 27/30, 2026
    "RDNTUSDT",      # Jan 27/30, 2026
    "ELONUSDT",      # Jan 27/30, 2026
    # Late 2025 with 2026 withdrawal deadlines
    "ACAUSDT",       # withdrawal suspended Mar 2, 2026
    "CLVUSDT",       # withdrawal suspended Mar 2, 2026
    "FOXYUSDT",      # withdrawal suspended Mar 2, 2026
    "PSTAKEUSDT",    # withdrawal suspended Mar 2, 2026
    "RACAUSDT",      # withdrawal suspended Mar 2, 2026
    "ICEUSDT",       # withdrawal suspended Mar 11, 2026
    # March 2026 — spot pair removals
    "RSS3USDT",      # Mar 16, 2026
    "MEMEFIUSDT",    # Mar 16, 2026
    "GHSTUSDT",      # Mar 16, 2026
    "RIOUSDT",       # Mar 16, 2026
    "SWEATUSDT",     # Mar 16, 2026
    "UXLINKUSDT",    # Mar 31, 2026
]

# ─────────────────────────────────────────────────────────────────────────────
# BYBIT delistings (Jan–Mar 2026)
# ─────────────────────────────────────────────────────────────────────────────

BYBIT_DELISTED = [
    # Perpetual futures delistings
    "IDEXUSDT",      # Feb 27, 2026 — perp
    "UROUSDT",       # Mar 6, 2026 — perp
    "VFYUSDT",       # Mar 27, 2026 — perp
    "OBTUSDT",       # perp delisted
    "A2ZUSDT",       # perp delisted
    "CTSIUSDT",      # perp delisted
    "XCHUSDT",       # perp delisted
    "TAIUSDT",       # perp delisted
    "DODOUSDT",      # perp delisted
    "SDUSDT",        # perp delisted
    "ALUUSDT",       # perp delisted
    "SKYAIUSDT",     # perp delisted
    "AINUSDT",       # perp delisted
    "DGBUSDT",       # perp delisted
    "NSUSDT",        # perp delisted
    # Spot delistings
    "SFUNDUSDT",     # spot delisted
    "SKATEUSDT",     # spot delisted
    "SOLUSDT",       # spot delisted (SOLO token, not Solana)
    "NRNUSDT",       # spot delisted
    "SNSUSDT",       # Bybit Alpha delisted
]

# ─────────────────────────────────────────────────────────────────────────────
# BITGET delistings (Jan–Apr 2026)
# ─────────────────────────────────────────────────────────────────────────────

# Bitget — Spot delistings
BITGET_SPOT_DELISTED = [
    # January 9, 2026
    "XUSDT",         # X token
    "L3USDT",
    "ELONUSDT",
    "COQUSDT",       # COQ token
    # January 23, 2026
    "YZYUSDT",
    "A2ZUSDT",
    # February 6, 2026
    "AVAILUSDT",
    "PERPUSDT",
    "MILKUSDT",
    # April 3, 2026
    "SXPUSDT",
    "NTRNUSDT",
    "LRCUSDT",
    "HOOKUSDT",
    "HIGHUSDT",
]

# Bitget — Futures delistings
BITGET_FUTURES_DELISTED = [
    "OMUSDT",        # Feb 2026
    "XDCUSDT",
    "SNTUSDT",
    "COREUSDT",
    "BLASTUSDT",
    "ZBCNUSDT",
    "TSTBSCUSDT",
    "PTBUSDT",
    "BIDUSDT",
    "PRCLSUSDT",
    "TURTLEUSDT",
    "NKNUSDT",
    "XIONUSDT",
    "HOOKUSDT",
    "ZRCUSDT",
    "GHSTUSDT",
    "HEMIUSDT",
    "PROMPTUSDT",
    "SWARMSUSDT",
    "SOLVUSDT",
    "ZEREBROUSDT",
    "CAMPUSDT",
    "HMSTRUSDT",
    "AGIUSDT",
    "GTCUSDT",
    "A2ZUSDT",
    "NFPUSDT",
    "NTRNUSDT",
    "FORTHUSDT",
    "RDNTUSDT",
    "LRCUSDT",
    "PEAQUSDT",
    "GNOUSDT",
    "TRUUSDT",
    "BSUUSDT",
    "AIAUSDT",
]

# ─────────────────────────────────────────────────────────────────────────────
# MEXC delistings (Jan–Mar 2026)
# ─────────────────────────────────────────────────────────────────────────────

MEXC_DELISTED = [
    # Futures delistings
    "ALEOUSDT",      # Jan 24, 2026 — perp
    "FWOGUSDT",      # Jan 24, 2026 — perp
    "XNOUSDT",       # Jan 24, 2026 — perp
    "FORTHUSDT",     # Mar 24, 2026 — perp
    "UUSDT",         # Mar 24, 2026 — perp
    "WMTXUSDT",      # Mar 24, 2026 — perp
    "MEMESUSDT",     # Mar 24, 2026 — perp
    "LAZIOUSDT",     # Mar 24, 2026 — perp
    "RTXUSDT",       # Mar 24, 2026 — perp
    # Spot delistings
    "LEVERUSDT",     # Mar 22, 2026 — spot
    # Meme+ Zone delistings (Mar 2026)
    "PSYOPANIMEUSDT",  # Mar 25, 2026 — perp
    "CLAWNCHUSDT",     # Mar 25, 2026 — perp
    "WARDUSDT",        # Mar 25, 2026 — perp
    "FORTUSDT",        # Mar 25, 2026 — perp (FORT, different from FORTH)
    "CATSTOCKUSDT",    # Mar 25, 2026 — perp
]

# ─────────────────────────────────────────────────────────────────────────────
# GATE.IO delistings (Jan 2026)
# ─────────────────────────────────────────────────────────────────────────────

GATE_DELISTED = [
    "LGCTUSDT",      # Jan 13, 2026 — Gate US
]


# ═══════════════════════════════════════════════════════════════════════════════
# COMBINED: All unique delisted symbols in XXXUSDT format (Jan–Apr 2026)
# ═══════════════════════════════════════════════════════════════════════════════

# Union of all exchange-specific lists, deduplicated and sorted
DELISTED_SYMBOLS = sorted(set(
    BINANCE_FUTURES_DELISTED
    + BINANCE_SPOT_DELISTED
    + OKX_DELISTED
    + BYBIT_DELISTED
    + BITGET_SPOT_DELISTED
    + BITGET_FUTURES_DELISTED
    + MEXC_DELISTED
    + GATE_DELISTED
))


# ═══════════════════════════════════════════════════════════════════════════════
# MULTI-EXCHANGE DELISTINGS: Symbols delisted on 2+ exchanges (highest risk)
# ═══════════════════════════════════════════════════════════════════════════════

def _get_exchange_lists():
    """Return a dict mapping exchange name to its delisted symbols."""
    return {
        "Binance": set(BINANCE_FUTURES_DELISTED + BINANCE_SPOT_DELISTED),
        "OKX": set(OKX_DELISTED),
        "Bybit": set(BYBIT_DELISTED),
        "Bitget": set(BITGET_SPOT_DELISTED + BITGET_FUTURES_DELISTED),
        "MEXC": set(MEXC_DELISTED),
        "Gate": set(GATE_DELISTED),
    }


def get_multi_exchange_delistings(min_exchanges=2):
    """Return symbols delisted on at least `min_exchanges` exchanges."""
    from collections import Counter
    counter = Counter()
    for _name, symbols in _get_exchange_lists().items():
        for s in symbols:
            counter[s] += 1
    return sorted([sym for sym, count in counter.items() if count >= min_exchanges])


# Symbols delisted/discontinued on 2 or more exchanges
MULTI_EXCHANGE_DELISTED = get_multi_exchange_delistings(min_exchanges=2)


# ═══════════════════════════════════════════════════════════════════════════════
# Quick-access flat list for filtering / exclusion in trading bots
# ═══════════════════════════════════════════════════════════════════════════════

# Include monitoring-tagged symbols as they are at high risk of imminent delist
ALL_RISKY_SYMBOLS = sorted(set(DELISTED_SYMBOLS + BINANCE_MONITORING_TAG))


if __name__ == "__main__":
    print(f"Total unique delisted symbols (Jan–Apr 2026): {len(DELISTED_SYMBOLS)}")
    print(f"Multi-exchange delistings (2+):               {len(MULTI_EXCHANGE_DELISTED)}")
    print(f"All risky symbols (incl. monitoring tag):      {len(ALL_RISKY_SYMBOLS)}")
    print()
    print("=== MULTI-EXCHANGE DELISTED (highest risk) ===")
    for sym in MULTI_EXCHANGE_DELISTED:
        print(f"  {sym}")
    print()
    print("=== ALL DELISTED SYMBOLS ===")
    for sym in DELISTED_SYMBOLS:
        print(f"  {sym}")
    print()
    print("=== BINANCE MONITORING TAG (at risk) ===")
    for sym in BINANCE_MONITORING_TAG:
        print(f"  {sym}")
