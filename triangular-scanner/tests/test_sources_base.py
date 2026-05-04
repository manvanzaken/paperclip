import pytest
from triscan.sources.base import Source


def test_source_is_abstract():
    with pytest.raises(TypeError):
        Source(name="x", taker_fee_pct=0.10, rest_url="", ws_url="")
