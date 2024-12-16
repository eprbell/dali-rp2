# Copyright 2022 eprbell
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

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_pair_converter_plugin import AssetPairAndTimestamp
from dali.cache import CACHE_DIR, load_from_cache
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.plugin.pair_converter.coinbase_advanced import PairConverterPlugin

BAR_DURATION: timedelta = timedelta(seconds=60)
BAR_TIMESTAMP: datetime = datetime(2020, 6, 1, 12, 34).replace(tzinfo=timezone.utc)
BAR_LOW: RP2Decimal = RP2Decimal("9558.02")
BAR_HIGH: RP2Decimal = RP2Decimal("9566.16")
BAR_OPEN: RP2Decimal = RP2Decimal("9566.16")
BAR_CLOSE: RP2Decimal = RP2Decimal("9560.73")
BAR_VOLUME: RP2Decimal = RP2Decimal("7.44296873")

FIVE_MINUTE_DURATION: timedelta = timedelta(seconds=300)
FIVE_MINUTE_TIMESTAMP: datetime = datetime(2020, 6, 1, 12, 30).replace(tzinfo=timezone.utc)
FIVE_MINUTE_LOW: RP2Decimal = RP2Decimal("9558.02")
FIVE_MINUTE_HIGH: RP2Decimal = RP2Decimal("9590.45")
FIVE_MINUTE_OPEN: RP2Decimal = RP2Decimal("9574.85")
FIVE_MINUTE_CLOSE: RP2Decimal = RP2Decimal("9560.73")
FIVE_MINUTE_VOLUME: RP2Decimal = RP2Decimal("33.9492526")

FIFTEEN_MINUTE_DURATION: timedelta = timedelta(seconds=900)
FIFTEEN_MINUTE_TIMESTAMP: datetime = datetime(2020, 6, 1, 12, 30, tzinfo=timezone.utc)
FIFTEEN_MINUTE_OPEN: RP2Decimal = RP2Decimal("9574.85")
FIFTEEN_MINUTE_HIGH: RP2Decimal = RP2Decimal("9590.45")
FIFTEEN_MINUTE_LOW: RP2Decimal = RP2Decimal("9558.02")
FIFTEEN_MINUTE_CLOSE: RP2Decimal = RP2Decimal("9569.97")
FIFTEEN_MINUTE_VOLUME: RP2Decimal = RP2Decimal("99.35339347")

ONE_DAY_DURATION: timedelta = timedelta(days=1)
ONE_DAY_TIMESTAMP: datetime = datetime(2020, 6, 1, 0, 0, tzinfo=timezone.utc)
ONE_DAY_OPEN: RP2Decimal = RP2Decimal("9445.83")
ONE_DAY_HIGH: RP2Decimal = RP2Decimal("10428")
ONE_DAY_LOW: RP2Decimal = RP2Decimal("9417.42")
ONE_DAY_CLOSE: RP2Decimal = RP2Decimal("10208.96")
ONE_DAY_VOLUME: RP2Decimal = RP2Decimal("21676.60828748")


class TestHistoricCryptoPlugin:
    def test_historical_prices(self, mocker: Any) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        cache_path = os.path.join(CACHE_DIR, plugin.cache_key())
        if os.path.exists(cache_path):
            os.remove(cache_path)

        # Reinstantiate plugin now that cache is gone
        plugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        mocker.patch.object(plugin, "get_historic_bar_from_native_source").return_value = HistoricalBar(
            duration=BAR_DURATION,
            timestamp=BAR_TIMESTAMP,
            low=BAR_LOW,
            high=BAR_HIGH,
            open=BAR_OPEN,
            close=BAR_CLOSE,
            volume=BAR_VOLUME,
        )

        # Read price without cache
        data = plugin.get_historic_bar_from_native_source(BAR_TIMESTAMP, "BTC", "USD", "Coinbase Advanced")

        assert data
        assert data.timestamp == BAR_TIMESTAMP
        assert data.low == BAR_LOW
        assert data.high == BAR_HIGH
        assert data.open == BAR_OPEN
        assert data.close == BAR_CLOSE
        assert data.volume == BAR_VOLUME

        # Read price again, but populate plugin cache this time
        value = plugin.get_conversion_rate(BAR_TIMESTAMP, "BTC", "USD", "Coinbase Advanced")
        assert value
        assert value == BAR_HIGH

        # Save plugin cache
        plugin.save_historical_price_cache()

        # Load plugin cache and verify
        cache = load_from_cache(plugin.cache_key())
        key = AssetPairAndTimestamp(BAR_TIMESTAMP, "BTC", "USD", "Coinbase Advanced")
        assert len(cache) == 1, str(cache)
        assert key in cache
        data = cache[key]

        assert data
        assert data.timestamp == BAR_TIMESTAMP
        assert data.low == BAR_LOW
        assert data.high == BAR_HIGH
        assert data.open == BAR_OPEN
        assert data.close == BAR_CLOSE
        assert data.volume == BAR_VOLUME

    def test_missing_historical_prices(self, mocker: Any) -> None:
        plugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        timestamp = datetime(2020, 6, 1, 0, 0)

        mocker.patch.object(plugin, "get_historic_bar_from_native_source").return_value = None

        data = plugin.get_historic_bar_from_native_source(timestamp, "EUR", "JPY", "Coinbase Advanced")
        assert data is None

    @pytest.mark.vcr
    def test_granularity_response(self, mocker: Any) -> None:
        plugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        timestamp = datetime(2020, 6, 1, 12, 34).replace(tzinfo=timezone.utc)

        data = plugin.get_historic_bar_from_native_source(timestamp, "BTC", "USD", "Coinbase Advanced")
        assert data
        assert data.timestamp == BAR_TIMESTAMP
        assert data.low == BAR_LOW
        assert data.high == BAR_HIGH
        assert data.open == BAR_OPEN
        assert data.close == BAR_CLOSE
        assert data.volume == BAR_VOLUME
        assert data.duration == BAR_DURATION

        mocker.patch("dali.plugin.pair_converter.coinbase_advanced.TIME_GRANULARITY", {"FIVE_MINUTE": 300})

        data = plugin.get_historic_bar_from_native_source(timestamp, "BTC", "USD", "Coinbase Advanced")
        assert data
        assert data.timestamp == FIVE_MINUTE_TIMESTAMP
        assert data.low == FIVE_MINUTE_LOW
        assert data.high == FIVE_MINUTE_HIGH
        assert data.open == FIVE_MINUTE_OPEN
        assert data.close == FIVE_MINUTE_CLOSE
        assert data.volume == FIVE_MINUTE_VOLUME
        assert data.duration == FIVE_MINUTE_DURATION

        mocker.patch("dali.plugin.pair_converter.coinbase_advanced.TIME_GRANULARITY", {"FIFTEEN_MINUTE": 900})

        data = plugin.get_historic_bar_from_native_source(timestamp, "BTC", "USD", "Coinbase Advanced")
        assert data
        assert data.timestamp == FIFTEEN_MINUTE_TIMESTAMP
        assert data.low == FIFTEEN_MINUTE_LOW
        assert data.high == FIFTEEN_MINUTE_HIGH
        assert data.open == FIFTEEN_MINUTE_OPEN
        assert data.close == FIFTEEN_MINUTE_CLOSE
        assert data.volume == FIFTEEN_MINUTE_VOLUME
        assert data.duration == FIFTEEN_MINUTE_DURATION

        mocker.patch("dali.plugin.pair_converter.coinbase_advanced.TIME_GRANULARITY", {"ONE_DAY": 86400})

        data = plugin.get_historic_bar_from_native_source(timestamp, "BTC", "USD", "Coinbase Advanced")
        assert data
        assert data.timestamp == ONE_DAY_TIMESTAMP
        assert data.low == ONE_DAY_LOW
        assert data.high == ONE_DAY_HIGH
        assert data.open == ONE_DAY_OPEN
        assert data.close == ONE_DAY_CLOSE
        assert data.volume == ONE_DAY_VOLUME
        assert data.duration == ONE_DAY_DURATION
