# Copyright 2022 macanudo527
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta, timezone # CHECK
import os
from typing import Any

from ccxt import binance

from rp2.rp2_decimal import RP2Decimal

from dali.abstract_pair_converter_plugin import AssetPairAndTimestamp
from dali.cache import CACHE_DIR, load_from_cache
from dali.configuration import Keyword
from dali.plugin.pair_converter.ccxt_converter import PairConverterPlugin

BAR_DURATION: str = "1m"
BAR_EXCHANGE: str = "Binance.com"
BAR_TIMESTAMP: datetime = 1504541580000
BAR_LOW: RP2Decimal = RP2Decimal("4230.0")
BAR_HIGH: RP2Decimal = RP2Decimal("4240.6")
BAR_OPEN: RP2Decimal = RP2Decimal("4235.4")
BAR_CLOSE: RP2Decimal = RP2Decimal("4230.7")
BAR_VOLUME: RP2Decimal = RP2Decimal("37.72941911")

class TestCcxtConverterPlugin:

    # pylint: disable=no-self-use
    def test_invalid_exchange(self, mocker: Any) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        data = plugin.get_historic_bar_from_native_source(BAR_TIMESTAMP, "BTC", "USD", "Bogus Exchange")
        assert data is None

    # pylint: disable=no-self-use
    def test_historical_prices(self, mocker: Any) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        cache_path = os.path.join(CACHE_DIR, plugin.cache_key())
        if os.path.exists(cache_path):
            os.remove(cache_path)

        # Reinstantiate plugin now that cache is gone
        plugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        mocker.patch.object(exchange, "fetch_ohlcv").return_value = [
            [
                BAR_TIMESTAMP,  # UTC timestamp in milliseconds, integer
                BAR_OPEN,       # (O)pen price, float
                BAR_HIGH,       # (H)ighest price, float
                BAR_LOW,        # (L)owest price, float
                BAR_CLOSE,      # (C)losing price, float
                BAR_VOLUME      # (V)olume (in terms of the base currency), float
            ],
        ]
        mocker.patch.object(plugin, "exchanges").return_value = {BAR_EXCHANGE: exchange}

        data = plugin.get_historic_bar_from_native_source(BAR_TIMESTAMP, "BTC", "USD", BAR_EXCHANGE)

        assert data
        assert data.timestamp == BAR_TIMESTAMP
        assert data.timestamp == BAR_TIMESTAMP
        assert data.low == BAR_LOW
        assert data.high == BAR_HIGH
        assert data.open == BAR_OPEN
        assert data.close == BAR_CLOSE
        assert data.volume == BAR_VOLUME

        # Read price again, but populate plugin cache this time
        value = plugin.get_conversion_rate(BAR_TIMESTAMP, "BTC", "USD", BAR_EXCHANGE)
        assert value
        assert value == BAR_HIGH

        # Save plugin cache
        plugin.save_historical_price_cache()

        # Load plugin cache and verify
        cache = load_from_cache(plugin.cache_key())
        key = AssetPairAndTimestamp(BAR_TIMESTAMP, "BTC", "USD", BAR_EXCHANGE)
        assert len(cache) == 1, str(cache)
        assert key in cache
        data = cache[key]

        assert data
        assert data.timestamp == BAR_TIMESTAMP
        assert data.timestamp == BAR_TIMESTAMP
        assert data.low == BAR_LOW
        assert data.high == BAR_HIGH
        assert data.open == BAR_OPEN
        assert data.close == BAR_CLOSE
        assert data.volume == BAR_VOLUME

    # pylint: disable=no-self-use
    def test_missing_historical_prices(self, mocker: Any) -> None:
        plugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        timestamp = datetime(2020, 6, 1, 0, 0)

        mocker.patch.object(plugin, "get_historic_bar_from_native_source").return_value = None

        data = plugin.get_historic_bar_from_native_source(timestamp, "EUR", "JPY", BAR_EXCHANGE)
        assert data is None	