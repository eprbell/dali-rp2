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

from datetime import datetime
import os
from typing import Any, List, Union

from ccxt import binance

from rp2.rp2_decimal import RP2Decimal

from dali.abstract_pair_converter_plugin import AssetPairAndTimestamp
from dali.cache import CACHE_DIR, load_from_cache
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.plugin.pair_converter.ccxt_converter import PairConverterPlugin

# Default exchange
TEST_EXCHANGE: str = "Binance.com"

# BTCUSDT conversion
BAR_DURATION: str = "1m"
BAR_TIMESTAMP: datetime = datetime.fromtimestamp(1504541580)
BAR_LOW: RP2Decimal = RP2Decimal("4230.0")
BAR_HIGH: RP2Decimal = RP2Decimal("4240.6")
BAR_OPEN: RP2Decimal = RP2Decimal("4235.4")
BAR_CLOSE: RP2Decimal = RP2Decimal("4230.7")
BAR_VOLUME: RP2Decimal = RP2Decimal("37.72941911")

# Missing Fiat Test
JPY_USD_RATE: RP2Decimal = RP2Decimal("115")

# No Fiat pair conversion
BETHETH_TIMESTAMP: datetime = datetime.fromtimestamp(1504541590)
BETHETH_LOW: RP2Decimal = RP2Decimal("0.9722")
BETHETH_HIGH: RP2Decimal = RP2Decimal("0.9739")
BETHETH_OPEN: RP2Decimal = RP2Decimal("0.9736")
BETHETH_CLOSE: RP2Decimal = RP2Decimal("0.9735")
BETHETH_VOLUME: RP2Decimal = RP2Decimal("309")

ETHUSDT_LOW: RP2Decimal = RP2Decimal("1753.75")
ETHUSDT_HIGH: RP2Decimal = RP2Decimal("1764.70")
ETHUSDT_OPEN: RP2Decimal = RP2Decimal("1755.81")
ETHUSDT_CLOSE: RP2Decimal = RP2Decimal("1763.03")
ETHUSDT_VOLUME: RP2Decimal = RP2Decimal("434")

# Non-USD Fiat pair conversion
BTCGBP_TIMESTAMP: datetime = datetime.fromtimestamp(1504541600)
BTCGBP_LOW: RP2Decimal = RP2Decimal("3379.06")
BTCGBP_HIGH: RP2Decimal = RP2Decimal("3387.53")
BTCGBP_OPEN: RP2Decimal = RP2Decimal("3383.29")
BTCGBP_CLOSE: RP2Decimal = RP2Decimal("3379.66")
BTCGBP_VOLUME: RP2Decimal = RP2Decimal("37.72941911")

# Fiat to Fiat Test
EUR_USD_RATE: RP2Decimal = RP2Decimal("1.0847")
EUR_USD_TIMESTAMP: datetime = datetime.fromtimestamp(1585958400)

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
        mocker.patch.object(plugin, "exchange_markets", {
            TEST_EXCHANGE:
                [
                    "BTCUSDT",
                ]
            }
        )
        mocker.patch.object(exchange, "fetchOHLCV").return_value = [
            [
                BAR_TIMESTAMP,  # UTC timestamp in milliseconds, integer
                BAR_OPEN,       # (O)pen price, float
                BAR_HIGH,       # (H)ighest price, float
                BAR_LOW,        # (L)owest price, float
                BAR_CLOSE,      # (C)losing price, float
                BAR_VOLUME      # (V)olume (in terms of the base currency), float
            ],
        ]
        mocker.patch.object(plugin, "exchanges", {TEST_EXCHANGE: exchange})

        data = plugin.get_historic_bar_from_native_source(BAR_TIMESTAMP, "BTC", "USD", TEST_EXCHANGE)

        assert data
        assert data.timestamp == BAR_TIMESTAMP
        assert data.low == BAR_LOW
        assert data.high == BAR_HIGH
        assert data.open == BAR_OPEN
        assert data.close == BAR_CLOSE
        assert data.volume == BAR_VOLUME

        # Read price again, but populate plugin cache this time
        value = plugin.get_conversion_rate(BAR_TIMESTAMP, "BTC", "USD", TEST_EXCHANGE)
        assert value
        assert value == BAR_HIGH

        # Save plugin cache
        plugin.save_historical_price_cache()

        # Load plugin cache and verify
        cache = load_from_cache(plugin.cache_key())
        key = AssetPairAndTimestamp(BAR_TIMESTAMP, "BTC", "USD", TEST_EXCHANGE)
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

        data = plugin.get_historic_bar_from_native_source(timestamp, "BOGUSCOIN", "JPY", TEST_EXCHANGE)
        assert data is None	

    # Some crypto assets have no fiat or stable coin pair; they are only paired with BTC or ETH (e.g. EZ or BETH)
    # To get an accurate fiat price, we must get the price in the base asset (e.g. BETH -> ETH) then convert that to fiat (e.g. ETH -> USD)
    def test_no_fiat_pair(self, mocker: Any) -> None:  
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, '{"BETH":"BETHETH"}')
        exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        mocker.patch.object(plugin, "exchange_markets", {
            TEST_EXCHANGE:
                [
                    "BETHETH",
                    "ETHUSDT",
                ]
            }
        )
        def no_fiat_fetchOHLCV(symbol: str, timeframe: str, timestamp: int, candles: int) -> List[List[Union[float, int]]]:
            if symbol == "BETH/ETH":
                return [
                    [
                        BETHETH_TIMESTAMP,  # UTC timestamp in milliseconds, integer
                        BETHETH_OPEN,       # (O)pen price, float
                        BETHETH_HIGH,       # (H)ighest price, float
                        BETHETH_LOW,        # (L)owest price, float
                        BETHETH_CLOSE,      # (C)losing price, float
                        BETHETH_VOLUME      # (V)olume (in terms of the base currency), float
                    ],
                ]
            elif symbol == "ETH/USDT":
                return [
                    [
                        BETHETH_TIMESTAMP,  # UTC timestamp in milliseconds, integer
                        ETHUSDT_OPEN,       # (O)pen price, float
                        ETHUSDT_HIGH,       # (H)ighest price, float
                        ETHUSDT_LOW,        # (L)owest price, float
                        ETHUSDT_CLOSE,      # (C)losing price, float
                        ETHUSDT_VOLUME      # (V)olume (in terms of the base currency), float
                    ],
                ] 

        mocker.patch.object(exchange, "fetchOHLCV").side_effect = no_fiat_fetchOHLCV
        mocker.patch.object(plugin, "exchanges", {TEST_EXCHANGE: exchange})

        data = plugin.get_historic_bar_from_native_source(BETHETH_TIMESTAMP, "BETH", "USD", TEST_EXCHANGE)

        assert data        
        assert data.timestamp == BETHETH_TIMESTAMP
        assert data.low == BETHETH_LOW * ETHUSDT_LOW
        assert data.high == BETHETH_HIGH * ETHUSDT_HIGH
        assert data.open == BETHETH_OPEN * ETHUSDT_OPEN
        assert data.close == BETHETH_CLOSE * ETHUSDT_CLOSE
        assert data.volume == BETHETH_VOLUME 

    # Test to make sure the default stable coin is not used with a fiat market that does exist on the exchange
    def test_nonusd_fiat_pair(self, mocker: Any) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        mocker.patch.object(plugin, "exchange_markets", {TEST_EXCHANGE:["BTCGBP"]})
        mocker.patch.object(exchange, "fetchOHLCV").return_value = [
            [
                BTCGBP_TIMESTAMP,  # UTC timestamp in milliseconds, integer
                BTCGBP_OPEN,       # (O)pen price, float
                BTCGBP_HIGH,       # (H)ighest price, float
                BTCGBP_LOW,        # (L)owest price, float
                BTCGBP_CLOSE,      # (C)losing price, float
                BTCGBP_VOLUME      # (V)olume (in terms of the base currency), float
            ],
        ]
        mocker.patch.object(plugin, "exchanges", {TEST_EXCHANGE: exchange})

        data = plugin.get_historic_bar_from_native_source(BTCGBP_TIMESTAMP, "BTC", "GBP", TEST_EXCHANGE)

        assert data
        assert data.timestamp == BTCGBP_TIMESTAMP
        assert data.low == BTCGBP_LOW
        assert data.high == BTCGBP_HIGH
        assert data.open == BTCGBP_OPEN
        assert data.close == BTCGBP_CLOSE
        assert data.volume == BTCGBP_VOLUME

    # Plugin should hand off the handling of a fiat to fiat pair to the fiat converter
    def test_fiat_pair(self, mocker: Any) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )

        # Need to be mocked to prevent logger spam
        mocker.patch.object(plugin, "exchange_markets", {TEST_EXCHANGE:["WHATEVER"]})
        mocker.patch.object(plugin, "get_fiat_exchange_rate").return_value = HistoricalBar(
            duration=86400,
            timestamp=EUR_USD_TIMESTAMP,
            open=RP2Decimal(str(EUR_USD_RATE)),
            high=RP2Decimal(str(EUR_USD_RATE)),
            low=RP2Decimal(str(EUR_USD_RATE)),
            close=RP2Decimal(str(EUR_USD_RATE)),
            volume=Keyword.UNKNOWN.value,
        )
        mocker.patch.object(plugin, "exchanges", {TEST_EXCHANGE: exchange})

        data = plugin.get_historic_bar_from_native_source(EUR_USD_TIMESTAMP, "EUR", "USD", TEST_EXCHANGE)

        assert data
        assert data.timestamp == EUR_USD_TIMESTAMP
        assert data.low == EUR_USD_RATE
        assert data.high == EUR_USD_RATE
        assert data.open == EUR_USD_RATE
        assert data.close == EUR_USD_RATE
        assert data.volume == Keyword.UNKNOWN.value
        assert plugin.get_fiat_exchange_rate.called_once()