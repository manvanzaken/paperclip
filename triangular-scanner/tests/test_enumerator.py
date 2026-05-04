from triscan.enumerator import enumerate_triangles, Market
from triscan.models import LegSide


def test_enumerate_simple_triangle():
    markets = [
        Market(symbol="BTC/USDT", base="BTC", quote="USDT", volume_24h_usd=2_000_000),
        Market(symbol="ETH/USDT", base="ETH", quote="USDT", volume_24h_usd=2_000_000),
        Market(symbol="ETH/BTC",  base="ETH", quote="BTC",  volume_24h_usd=2_000_000),
    ]
    triangles = enumerate_triangles(
        exchange="binance", markets=markets,
        anchors=["USDT"], min_volume_usd=1_000_000,
    )
    assert len(triangles) == 2
    sigs = {t.id for t in triangles}
    assert "binance:USDT:BTC/USDT@BUY|ETH/BTC@BUY|ETH/USDT@SELL" in sigs
    assert "binance:USDT:ETH/USDT@BUY|ETH/BTC@SELL|BTC/USDT@SELL" in sigs


def test_enumerate_filters_low_volume():
    markets = [
        Market(symbol="BTC/USDT", base="BTC", quote="USDT", volume_24h_usd=500_000),
        Market(symbol="ETH/USDT", base="ETH", quote="USDT", volume_24h_usd=2_000_000),
        Market(symbol="ETH/BTC",  base="ETH", quote="BTC",  volume_24h_usd=2_000_000),
    ]
    triangles = enumerate_triangles(
        exchange="binance", markets=markets,
        anchors=["USDT"], min_volume_usd=1_000_000,
    )
    assert triangles == []


def test_enumerate_multi_anchor():
    markets = [
        Market("BTC/USDT", "BTC", "USDT", 2_000_000),
        Market("BTC/USDC", "BTC", "USDC", 2_000_000),
        Market("ETH/USDT", "ETH", "USDT", 2_000_000),
        Market("ETH/USDC", "ETH", "USDC", 2_000_000),
        Market("ETH/BTC",  "ETH", "BTC",  2_000_000),
    ]
    triangles = enumerate_triangles(
        exchange="binance", markets=markets,
        anchors=["USDT", "USDC"], min_volume_usd=1_000_000,
    )
    assert len(triangles) == 4
