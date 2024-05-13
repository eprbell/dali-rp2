# Copyright 2022 Neal Chambers
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
from typing import Any, Dict, Generator, List, Union

import pytest
from ccxt import binance, kraken
from prezzemolo.avl_tree import AVLTree
from prezzemolo.vertex import Vertex
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.abstract_pair_converter_plugin import AssetPairAndTimestamp
from dali.cache import CACHE_DIR, load_from_cache
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.in_transaction import InTransaction
from dali.mapped_graph import MappedGraph
from dali.plugin.pair_converter.ccxt_exchangerate_host import PairConverterPlugin
from dali.plugin.pair_converter.csv.kraken import Kraken as KrakenCsvPricing
from dali.transaction_manifest import TransactionManifest

# Default exchange
ALIAS_EXCHANGE: str = "alias"
ALT_EXCHANGE: str = "Binance.com"
FIAT_EXCHANGE: str = "fiat"
LOCKED_EXCHANGE: str = "Kraken"
PIONEX_EXCHANGE: str = "Pionex"
TEST_EXCHANGE: str = "Kraken"
TEST_MARKETS: Dict[str, List[str]] = {
    "BTCUSDT": [ALT_EXCHANGE],
    "BTCUSDC": [ALT_EXCHANGE],
    "BTCGBP": [ALT_EXCHANGE],
    "BETHETH": [ALT_EXCHANGE],
    "ETHUSDT": [ALT_EXCHANGE],
    "USDCUSD": [TEST_EXCHANGE],
    "USDTUSD": [TEST_EXCHANGE],
    "USDJPY": [FIAT_EXCHANGE],
    "XBTBTC": [ALIAS_EXCHANGE],
}
PIONEX_MARKETS: Dict[str, List[str]] = {"MBTCBTC": [ALIAS_EXCHANGE]}
LOCKED_MARKETS: Dict[str, List[str]] = {
    "BTCUSDT": [TEST_EXCHANGE],
    "USDTUSD": [TEST_EXCHANGE],
}

# BTCUSDC conversion
BTCUSDC_DURATION: str = "1m"
BTCUSDC_TIMESTAMP: datetime = datetime.fromtimestamp(1504541580, timezone.utc)
BTCUSDC_LOW: RP2Decimal = RP2Decimal("5230.0")
BTCUSDC_HIGH: RP2Decimal = RP2Decimal("5240.6")
BTCUSDC_OPEN: RP2Decimal = RP2Decimal("5235.4")
BTCUSDC_CLOSE: RP2Decimal = RP2Decimal("5230.7")
BTCUSDC_VOLUME: RP2Decimal = RP2Decimal("137.72941911")

# BTCUSDT conversion
BTCUSDT_DURATION: str = "1m"
BTCUSDT_TIMESTAMP: datetime = datetime.fromtimestamp(1504541580, timezone.utc)
BTCUSDT_LOW: RP2Decimal = RP2Decimal("4230.0")
BTCUSDT_HIGH: RP2Decimal = RP2Decimal("4240.6")
BTCUSDT_OPEN: RP2Decimal = RP2Decimal("4235.4")
BTCUSDT_CLOSE: RP2Decimal = RP2Decimal("4230.7")
BTCUSDT_VOLUME: RP2Decimal = RP2Decimal("37.72941911")

# USDTUSD conversion
USDTUSD_DURATION: str = "1m"
USDTUSD_TIMESTAMP: datetime = datetime.fromtimestamp(1504541580, timezone.utc)
USDTUSD_LOW: RP2Decimal = RP2Decimal("0.9987")
USDTUSD_HIGH: RP2Decimal = RP2Decimal("0.9988")
USDTUSD_OPEN: RP2Decimal = RP2Decimal("0.9987")
USDTUSD_CLOSE: RP2Decimal = RP2Decimal("0.9988")
USDTUSD_VOLUME: RP2Decimal = RP2Decimal("113.786789")

# USDCUSD conversion
USDCUSD_DURATION: str = "1m"
USDCUSD_TIMESTAMP: datetime = datetime.fromtimestamp(1504541580, timezone.utc)
USDCUSD_LOW: RP2Decimal = RP2Decimal("1.9987")
USDCUSD_HIGH: RP2Decimal = RP2Decimal("1.9988")
USDCUSD_OPEN: RP2Decimal = RP2Decimal("1.9987")
USDCUSD_CLOSE: RP2Decimal = RP2Decimal("1.9988")
USDCUSD_VOLUME: RP2Decimal = RP2Decimal("213.786789")

# Missing Fiat Test
JPY_USD_RATE: RP2Decimal = RP2Decimal("115")

# No Fiat pair conversion
BETHETH_TIMESTAMP: datetime = datetime.fromtimestamp(1504541590, timezone.utc)
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
BTCGBP_TIMESTAMP: datetime = datetime.fromtimestamp(1504541580, timezone.utc)
BTCGBP_LOW: RP2Decimal = RP2Decimal("3379.06")
BTCGBP_HIGH: RP2Decimal = RP2Decimal("3387.53")
BTCGBP_OPEN: RP2Decimal = RP2Decimal("3383.29")
BTCGBP_CLOSE: RP2Decimal = RP2Decimal("3379.66")
BTCGBP_VOLUME: RP2Decimal = RP2Decimal("37.72941911")

# Kraken CSV read Test
KRAKEN_TIMESTAMP: datetime = datetime.fromtimestamp(1601855760, timezone.utc)
KRAKEN_LOW: RP2Decimal = RP2Decimal("1.5557")
KRAKEN_HIGH: RP2Decimal = RP2Decimal("1.5556")
KRAKEN_OPEN: RP2Decimal = RP2Decimal("1.5555")
KRAKEN_CLOSE: RP2Decimal = RP2Decimal("1.5558")
KRAKEN_VOLUME: RP2Decimal = RP2Decimal("15.15")

# Fiat to Fiat Test
EUR_USD_RATE: RP2Decimal = RP2Decimal("1.0847")
EUR_USD_TIMESTAMP: datetime = datetime.fromtimestamp(1585958400, timezone.utc)

_MS_IN_SECOND: int = 1000

# Time in ms
_MINUTE: str = "1m"
_FIVE_MINUTE: str = "5m"
_FIFTEEN_MINUTE: str = "15m"
_ONE_HOUR: str = "1h"
_FOUR_HOUR: str = "4h"
_SIX_HOUR: str = "6h"
_ONE_DAY: str = "1d"
_ONE_WEEK: str = "1w"
_TIME_GRANULARITY_STRING_TO_SECONDS: Dict[str, int] = {
    _MINUTE: 60,
    _FIVE_MINUTE: 300,
    _FIFTEEN_MINUTE: 900,
    _ONE_HOUR: 3600,
    _FOUR_HOUR: 14400,
    _SIX_HOUR: 21600,
    _ONE_DAY: 86400,
    _ONE_WEEK: 604800,
}

# Faked Volume
LOW_VOLUME: RP2Decimal = RP2Decimal("1")
HIGH_VOLUME: RP2Decimal = RP2Decimal("100")

# Fake Transaction
FAKE_TRANSACTION: InTransaction = InTransaction(
    plugin="Plugin",
    unique_id=Keyword.UNKNOWN.value,
    raw_data="raw",
    timestamp=datetime.fromtimestamp(1504541500, timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z"),
    asset="BTC",
    exchange="Kraken",
    holder="test",
    transaction_type=Keyword.BUY.value,
    spot_price=Keyword.UNKNOWN.value,
    crypto_in="1",
    crypto_fee=None,
    fiat_in_no_fee=None,
    fiat_in_with_fee=None,
    fiat_fee=None,
    notes="notes",
)


class TestCcxtPlugin:
    @pytest.fixture
    def vertex_list(self) -> List[Vertex[str]]:
        beth: Vertex[str] = Vertex[str](name="BETH")
        btc: Vertex[str] = Vertex[str](name="BTC")
        eth: Vertex[str] = Vertex[str](name="ETH")
        gbp: Vertex[str] = Vertex[str](name="GBP")
        jpy: Vertex[str] = Vertex[str](name="JPY")
        usdc: Vertex[str] = Vertex[str](name="USDC")
        usdt: Vertex[str] = Vertex[str](name="USDT")
        usd: Vertex[str] = Vertex[str](name="USD")

        beth.add_neighbor(eth, 1.0)
        btc.add_neighbor(usdc, 2.0)  # Has higher volume, but we don't want to disrupt other tests
        btc.add_neighbor(usdt, 1.0)
        btc.add_neighbor(gbp, 2.0)
        eth.add_neighbor(usdt, 1.0)
        usdc.add_neighbor(usd, 2.0)
        usdt.add_neighbor(usd, 1.0)
        usd.add_neighbor(jpy, 50.0)

        return [beth, btc, eth, gbp, jpy, usdc, usdt, usd]

    @pytest.fixture
    def graph_optimized(self, vertex_list: List[Vertex[str]]) -> MappedGraph[str]:
        return MappedGraph[str](TEST_EXCHANGE, vertex_list, {"BETH", "BTC", "ETH", "GBP", "JPY", "USDC", "USDT", "USD"})

    @pytest.fixture
    def pionex_graph_optimized(self, vertex_list: List[Vertex[str]]) -> MappedGraph[str]:
        return MappedGraph[str](PIONEX_EXCHANGE, vertex_list, {"BETH", "BTC", "ETH", "GBP", "JPY", "USDC", "USDT", "USD"})

    @pytest.fixture
    def graph_fiat_optimized(self, vertex_list: List[Vertex[str]]) -> MappedGraph[str]:
        return MappedGraph[str](TEST_EXCHANGE, vertex_list, {"GBP", "JPY", "USD"})

    @pytest.fixture
    def simple_tree(self, graph_optimized: MappedGraph[str]) -> AVLTree[datetime, MappedGraph[str]]:
        simple_tree: AVLTree[datetime, MappedGraph[str]] = AVLTree()

        # The original unoptimized graph is placed at the earliest possible time
        simple_tree.insert_node(datetime.fromtimestamp(1504541580, timezone.utc), graph_optimized)

        return simple_tree

    @pytest.fixture
    def simple_pionex_tree(self, pionex_graph_optimized: MappedGraph[str]) -> AVLTree[datetime, MappedGraph[str]]:
        simple_tree: AVLTree[datetime, MappedGraph[str]] = AVLTree()

        # The original unoptimized graph is placed at the earliest possible time
        simple_tree.insert_node(datetime.fromtimestamp(1504541580, timezone.utc), pionex_graph_optimized)

        return simple_tree

    def __btcusdt_mock(self, plugin: PairConverterPlugin, mocker: Any, graph_optimized: MappedGraph[str]) -> None:
        exchange = kraken(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        alt_exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )

        kraken_csv = KrakenCsvPricing(transaction_manifest=TransactionManifest([FAKE_TRANSACTION], 1, "USD"))

        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {TEST_EXCHANGE: TEST_MARKETS})
        mocker.patch.object(kraken_csv, "find_historical_bars").return_value = [None]
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_csv_reader", {"Kraken": kraken_csv})
        mocker.patch.object(plugin, "_get_request_delay").return_value = 0.0

        def ohlcv_generator(
            symbol_bar: List[Union[float, int]],
            progression: List[float],
            timeframe: str,
        ) -> Generator[List[List[Union[float, int]]], None, None]:
            count = 0
            while count < len(progression):
                yield [
                    [
                        symbol_bar[0] + _TIME_GRANULARITY_STRING_TO_SECONDS[timeframe] * _MS_IN_SECOND * count,  # UTC timestamp in milliseconds, integer
                        symbol_bar[1] + progression[count],  # (O)pen price, float
                        symbol_bar[2] + progression[count],  # (H)ighest price, float
                        symbol_bar[3] + progression[count],  # (L)owest price, float
                        symbol_bar[4] + progression[count],  # (C)losing price, float
                        symbol_bar[5] + progression[count],  # (V)olume (in terms of the base currency), float
                    ],
                ]
                count += 1

        alt_symbol_generators: Dict[str, Generator[List[List[Union[float, int]]], None, None]] = {
            "BTC/USDT1w": ohlcv_generator(
                symbol_bar=[
                    BTCUSDT_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                    float(BTCUSDT_OPEN),  # (O)pen price, float
                    float(BTCUSDT_HIGH),  # (H)ighest price, float
                    float(BTCUSDT_LOW),  # (L)owest price, float
                    float(BTCUSDT_CLOSE),  # (C)losing price, float
                    float(BTCUSDT_VOLUME),  # (V)olume (in terms of the base currency), float
                ],
                progression=[0, 0, 0, 0],
                timeframe="1w",
            ),
            "BTC/USDT1m": ohlcv_generator(
                symbol_bar=[
                    BTCUSDT_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                    float(BTCUSDT_OPEN),  # (O)pen price, float
                    float(BTCUSDT_HIGH),  # (H)ighest price, float
                    float(BTCUSDT_LOW),  # (L)owest price, float
                    float(BTCUSDT_CLOSE),  # (C)losing price, float
                    float(BTCUSDT_VOLUME),  # (V)olume (in terms of the base currency), float
                ],
                progression=[0, 0, 0, 0],
                timeframe="1m",
            ),
            "BTC/USDC1w": ohlcv_generator(
                symbol_bar=[
                    BTCUSDC_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                    float(BTCUSDC_OPEN),  # (O)pen price, float
                    float(BTCUSDC_HIGH),  # (H)ighest price, float
                    float(BTCUSDC_LOW),  # (L)owest price, float
                    float(BTCUSDC_CLOSE),  # (C)losing price, float
                    float(BTCUSDC_VOLUME),  # (V)olume (in terms of the base currency), float
                ],
                progression=[0, 0, -110, 0],
                timeframe="1w",
            ),
            "BTC/USDC1m": ohlcv_generator(
                symbol_bar=[
                    BTCUSDC_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                    float(BTCUSDC_OPEN),  # (O)pen price, float
                    float(BTCUSDC_HIGH),  # (H)ighest price, float
                    float(BTCUSDC_LOW),  # (L)owest price, float
                    float(BTCUSDC_CLOSE),  # (C)losing price, float
                    float(BTCUSDC_VOLUME),  # (V)olume (in terms of the base currency), float
                ],
                progression=[0, 0, 0, 0],
                timeframe="1m",
            ),
            "BTC/GBP1w": ohlcv_generator(
                symbol_bar=[
                    BTCGBP_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                    float(BTCGBP_OPEN),  # (O)pen price, float
                    float(BTCGBP_HIGH),  # (H)ighest price, float
                    float(BTCGBP_LOW),  # (L)owest price, float
                    float(BTCGBP_CLOSE),  # (C)losing price, float
                    float(BTCGBP_VOLUME),  # (V)olume (in terms of the base currency), float
                ],
                progression=[0, 0, 0, 0],
                timeframe="1w",
            ),
            "BTC/GBP1m": ohlcv_generator(
                symbol_bar=[
                    BTCGBP_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                    float(BTCGBP_OPEN),  # (O)pen price, float
                    float(BTCGBP_HIGH),  # (H)ighest price, float
                    float(BTCGBP_LOW),  # (L)owest price, float
                    float(BTCGBP_CLOSE),  # (C)losing price, float
                    float(BTCGBP_VOLUME),  # (V)olume (in terms of the base currency), float
                ],
                progression=[0, 0, 0, 0],
                timeframe="1m",
            ),
        }

        # Mock two different markets for BTC for optimization tests
        def fetch_ohlcv(
            symbol: str, timeframe: str, symbol_generators: Dict[str, Generator[List[List[Union[float, int]]], None, None]]
        ) -> List[List[Union[float, int]]]:
            try:
                return next(symbol_generators[symbol + timeframe])
            except StopIteration:
                return []

        mocker.patch.object(alt_exchange, "fetchOHLCV").side_effect = lambda symbol, timeframe, timestamp, candles: fetch_ohlcv(
            symbol, timeframe, symbol_generators=alt_symbol_generators
        )

        symbol_generators: Dict[str, Generator[List[List[Union[float, int]]], None, None]] = {
            "USDT/USD1w": ohlcv_generator(
                symbol_bar=[
                    USDTUSD_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                    float(USDTUSD_OPEN),  # (O)pen price, float
                    float(USDTUSD_HIGH),  # (H)ighest price, float
                    float(USDTUSD_LOW),  # (L)owest price, float
                    float(USDTUSD_CLOSE),  # (C)losing price, float
                    float(USDTUSD_VOLUME),  # (V)olume (in terms of the base currency), float
                ],
                progression=[0, 0, -110, 0],
                timeframe="1w",
            ),
            "USDT/USD1m": ohlcv_generator(
                symbol_bar=[
                    USDTUSD_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                    float(USDTUSD_OPEN),  # (O)pen price, float
                    float(USDTUSD_HIGH),  # (H)ighest price, float
                    float(USDTUSD_LOW),  # (L)owest price, float
                    float(USDTUSD_CLOSE),  # (C)losing price, float
                    float(USDTUSD_VOLUME),  # (V)olume (in terms of the base currency), float
                ],
                progression=[0, 0, 0, 0],
                timeframe="1m",
            ),
            "USDC/USD1w": ohlcv_generator(
                symbol_bar=[
                    USDCUSD_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                    float(USDCUSD_OPEN),  # (O)pen price, float
                    float(USDCUSD_HIGH),  # (H)ighest price, float
                    float(USDCUSD_LOW),  # (L)owest price, float
                    float(USDCUSD_CLOSE),  # (C)losing price, float
                    float(USDCUSD_VOLUME),  # (V)olume (in terms of the base currency), float
                ],
                progression=[0, 0, -110, 0],
                timeframe="1w",
            ),
            "USDC/USD1m": ohlcv_generator(
                symbol_bar=[
                    USDCUSD_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                    float(USDCUSD_OPEN),  # (O)pen price, float
                    float(USDCUSD_HIGH),  # (H)ighest price, float
                    float(USDCUSD_LOW),  # (L)owest price, float
                    float(USDCUSD_CLOSE),  # (C)losing price, float
                    float(USDCUSD_VOLUME),  # (V)olume (in terms of the base currency), float
                ],
                progression=[0, 0, 0, 0],
                timeframe="1m",
            ),
        }

        mocker.patch.object(exchange, "fetchOHLCV").side_effect = lambda symbol, timeframe, timestamp, candles: fetch_ohlcv(
            symbol, timeframe, symbol_generators=symbol_generators
        )
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchanges", {TEST_EXCHANGE: exchange, ALT_EXCHANGE: alt_exchange})
        mocker.patch.object(plugin, "_generate_unoptimized_graph").return_value = graph_optimized

    def __btcusdt_mock_unoptimized(
        self, plugin: PairConverterPlugin, mocker: Any, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]]
    ) -> None:
        self.__btcusdt_mock(plugin, mocker, graph_optimized)

        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_2_graph_tree", {TEST_EXCHANGE: simple_tree})

    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr
    def test_unknown_exchange(self, mocker: Any, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]]) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, fiat_access_key="BOGUS_KEY")
        self.__btcusdt_mock_unoptimized(plugin, mocker, graph_optimized, simple_tree)

        assert plugin._get_pricing_exchange_for_exchange("Bogus Exchange") == TEST_EXCHANGE  # pylint: disable=protected-access

    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr
    def test_historical_prices(self, mocker: Any, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]]) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, fiat_access_key="BOGUS_KEY")
        cache_path = os.path.join(CACHE_DIR, plugin.cache_key())
        if os.path.exists(cache_path):
            os.remove(cache_path)

        # Reinstantiate plugin now that cache is gone
        plugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, fiat_access_key="BOGUS_KEY")
        self.__btcusdt_mock_unoptimized(plugin, mocker, graph_optimized, simple_tree)

        data = plugin.get_historic_bar_from_native_source(BTCUSDT_TIMESTAMP, "BTC", "USD", TEST_EXCHANGE)

        assert data
        assert data.timestamp == BTCUSDT_TIMESTAMP
        assert data.low == BTCUSDT_LOW * USDTUSD_LOW
        assert data.high == BTCUSDT_HIGH * USDTUSD_HIGH
        assert data.open == BTCUSDT_OPEN * USDTUSD_OPEN
        assert data.close == BTCUSDT_CLOSE * USDTUSD_CLOSE
        assert data.volume == BTCUSDT_VOLUME + USDTUSD_VOLUME

        # Read price again, but populate plugin cache this time
        value = plugin.get_conversion_rate(BTCUSDT_TIMESTAMP, "BTC", "USD", TEST_EXCHANGE)
        assert value
        assert value == BTCUSDT_HIGH * USDTUSD_HIGH

        # Save plugin cache
        plugin.save_historical_price_cache()

        # Load plugin cache and verify
        cache = load_from_cache(plugin.cache_key())
        key = AssetPairAndTimestamp(BTCUSDT_TIMESTAMP, "BTC", "USD", TEST_EXCHANGE)

        # 3 cached prices - BTC/USDT, USDT/USD, BTC/USD
        assert len(cache) == 3, str(cache)
        assert key in cache
        data = cache[key]

        assert data
        assert data.timestamp == BTCUSDT_TIMESTAMP
        assert data.timestamp == BTCUSDT_TIMESTAMP
        assert data.low == BTCUSDT_LOW * USDTUSD_LOW
        assert data.high == BTCUSDT_HIGH * USDTUSD_HIGH
        assert data.open == BTCUSDT_OPEN * USDTUSD_OPEN
        assert data.close == BTCUSDT_CLOSE * USDTUSD_CLOSE
        assert data.volume == BTCUSDT_VOLUME + USDTUSD_VOLUME

    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr
    def test_missing_historical_prices(self, mocker: Any) -> None:
        plugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, fiat_access_key="BOGUS_KEY")
        timestamp = datetime(2020, 6, 1, 0, 0)

        mocker.patch.object(plugin, "get_historic_bar_from_native_source").return_value = None

        data = plugin.get_historic_bar_from_native_source(timestamp, "BOGUSCOIN", "JPY", TEST_EXCHANGE)
        assert data is None

    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr(record_mode="none")
    def test_missing_fiat_pair(self, mocker: Any, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]]) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, fiat_access_key="BOGUS_KEY")
        self.__btcusdt_mock_unoptimized(plugin, mocker, graph_optimized, simple_tree)

        mocker.patch.object(plugin, "_get_fiat_exchange_rate").return_value = HistoricalBar(
            duration=timedelta(seconds=86400),
            timestamp=BTCUSDT_TIMESTAMP,
            open=RP2Decimal(str(JPY_USD_RATE)),
            high=RP2Decimal(str(JPY_USD_RATE)),
            low=RP2Decimal(str(JPY_USD_RATE)),
            close=RP2Decimal(str(JPY_USD_RATE)),
            volume=ZERO,
        )

        data = plugin.get_historic_bar_from_native_source(BTCUSDT_TIMESTAMP, "BTC", "JPY", TEST_EXCHANGE)

        assert data
        assert data.timestamp == BTCUSDT_TIMESTAMP
        assert data.low == BTCUSDT_LOW * USDTUSD_LOW * JPY_USD_RATE
        assert data.high == BTCUSDT_HIGH * USDTUSD_HIGH * JPY_USD_RATE
        assert data.open == BTCUSDT_OPEN * USDTUSD_OPEN * JPY_USD_RATE
        assert data.close == BTCUSDT_CLOSE * USDTUSD_CLOSE * JPY_USD_RATE
        assert data.volume == BTCUSDT_VOLUME + USDTUSD_VOLUME

    # Some crypto assets have no fiat or stable coin pair; they are only paired with BTC or ETH (e.g. EZ or BETH)
    # To get an accurate fiat price, we must get the price in the base asset (e.g. BETH -> ETH) then convert that to fiat (e.g. ETH -> USD)
    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr
    def test_no_fiat_pair(self, mocker: Any, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]]) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, fiat_access_key="BOGUS_KEY")

        exchange = kraken(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        alt_exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {TEST_EXCHANGE: TEST_MARKETS})

        def no_fiat_fetch_ohlcv(symbol: str, timeframe: str, timestamp: int, candles: int) -> List[List[Union[float, int]]]:
            # pylint: disable=unused-argument
            result: List[List[Union[float, int]]] = []
            if symbol == "BETH/ETH":
                result = [
                    [
                        BETHETH_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                        float(BETHETH_OPEN),  # (O)pen price, float
                        float(BETHETH_HIGH),  # (H)ighest price, float
                        float(BETHETH_LOW),  # (L)owest price, float
                        float(BETHETH_CLOSE),  # (C)losing price, float
                        float(BETHETH_VOLUME),  # (V)olume (in terms of the base currency), float
                    ],
                ]
            elif symbol == "ETH/USDT":
                result = [
                    [
                        BETHETH_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                        float(ETHUSDT_OPEN),  # (O)pen price, float
                        float(ETHUSDT_HIGH),  # (H)ighest price, float
                        float(ETHUSDT_LOW),  # (L)owest price, float
                        float(ETHUSDT_CLOSE),  # (C)losing price, float
                        float(ETHUSDT_VOLUME),  # (V)olume (in terms of the base currency), float
                    ],
                ]

            return result

        mocker.patch.object(alt_exchange, "fetchOHLCV").side_effect = no_fiat_fetch_ohlcv
        mocker.patch.object(exchange, "fetchOHLCV").return_value = [
            [
                USDTUSD_TIMESTAMP,  # UTC timestamp in milliseconds, integer
                USDTUSD_OPEN,  # (O)pen price, float
                USDTUSD_HIGH,  # (H)ighest price, float
                USDTUSD_LOW,  # (L)owest price, float
                USDTUSD_CLOSE,  # (C)losing price, float
                USDTUSD_VOLUME,  # (V)olume (in terms of the base currency), float
            ],
        ]
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchanges", {TEST_EXCHANGE: exchange, ALT_EXCHANGE: alt_exchange})
        mocker.patch.object(plugin, "_generate_unoptimized_graph").return_value = graph_optimized
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_2_graph_tree", {TEST_EXCHANGE: simple_tree})

        data = plugin.get_historic_bar_from_native_source(BETHETH_TIMESTAMP, "BETH", "USD", TEST_EXCHANGE)

        assert data
        assert data.timestamp == BETHETH_TIMESTAMP
        assert data.low == BETHETH_LOW * ETHUSDT_LOW * USDTUSD_LOW
        assert data.high == BETHETH_HIGH * ETHUSDT_HIGH * USDTUSD_HIGH
        assert data.open == BETHETH_OPEN * ETHUSDT_OPEN * USDTUSD_OPEN
        assert data.close == BETHETH_CLOSE * ETHUSDT_CLOSE * USDTUSD_CLOSE
        assert data.volume == BETHETH_VOLUME + ETHUSDT_VOLUME + USDTUSD_VOLUME

    # Test to make sure the default stable coin is not used with a fiat market that does exist on the exchange
    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr
    def test_nonusd_fiat_pair(self, mocker: Any, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]]) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, default_exchange="Binance.com", fiat_access_key="BOGUS_KEY")
        alt_exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        exchange = kraken(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {TEST_EXCHANGE: TEST_MARKETS})
        mocker.patch.object(exchange, "fetchOHLCV").return_value = [
            [
                BTCUSDT_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                float(BTCUSDT_OPEN),  # (O)pen price, float
                float(BTCUSDT_HIGH),  # (H)ighest price, float
                float(BTCUSDT_LOW),  # (L)owest price, float
                float(BTCUSDT_CLOSE),  # (C)losing price, float
                float(BTCUSDT_VOLUME),  # (V)olume (in terms of the base currency), float
            ],
        ]
        mocker.patch.object(alt_exchange, "fetchOHLCV").return_value = [
            [
                BTCGBP_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                float(BTCGBP_OPEN),  # (O)pen price, float
                float(BTCGBP_HIGH),  # (H)ighest price, float
                float(BTCGBP_LOW),  # (L)owest price, float
                float(BTCGBP_CLOSE),  # (C)losing price, float
                float(BTCGBP_VOLUME),  # (V)olume (in terms of the base currency), float
            ],
        ]
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchanges", {TEST_EXCHANGE: exchange, ALT_EXCHANGE: alt_exchange})
        mocker.patch.object(plugin, "_generate_unoptimized_graph").return_value = graph_optimized
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_2_graph_tree", {TEST_EXCHANGE: simple_tree})

        data = plugin.get_historic_bar_from_native_source(BTCGBP_TIMESTAMP, "BTC", "GBP", TEST_EXCHANGE)

        assert data
        assert data.timestamp == BTCGBP_TIMESTAMP
        assert data.low == BTCGBP_LOW
        assert data.high == BTCGBP_HIGH
        assert data.open == BTCGBP_OPEN
        assert data.close == BTCGBP_CLOSE
        assert data.volume == BTCGBP_VOLUME

    # Plugin should hand off the handling of a fiat to fiat pair to the fiat converter
    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr
    def test_fiat_pair(self, mocker: Any, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]]) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, fiat_access_key="BOGUS_KEY")
        exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )

        # Need to be mocked to prevent logger spam
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {TEST_EXCHANGE: ["WHATEVER"]})
        mocker.patch.object(plugin, "_generate_unoptimized_graph").return_value = graph_optimized
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_2_graph_tree", {TEST_EXCHANGE: simple_tree})

        mocker.patch.object(plugin, "_get_fiat_exchange_rate").return_value = HistoricalBar(
            duration=timedelta(seconds=86400),
            timestamp=EUR_USD_TIMESTAMP,
            open=RP2Decimal(str(EUR_USD_RATE)),
            high=RP2Decimal(str(EUR_USD_RATE)),
            low=RP2Decimal(str(EUR_USD_RATE)),
            close=RP2Decimal(str(EUR_USD_RATE)),
            volume=ZERO,
        )
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchanges", {TEST_EXCHANGE: exchange})

        data = plugin.get_historic_bar_from_native_source(EUR_USD_TIMESTAMP, "EUR", "USD", TEST_EXCHANGE)

        assert data
        assert data.timestamp == EUR_USD_TIMESTAMP
        assert data.low == EUR_USD_RATE
        assert data.high == EUR_USD_RATE
        assert data.open == EUR_USD_RATE
        assert data.close == EUR_USD_RATE
        assert data.volume == ZERO

    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr
    def disabled_test_kraken_csv(self, mocker: Any, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]]) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, fiat_access_key="BOGUS_KEY")

        cache_path = os.path.join(CACHE_DIR, "Test-" + plugin.cache_key())
        if os.path.exists(cache_path):
            os.remove(cache_path)

        kraken_csv = KrakenCsvPricing(transaction_manifest=TransactionManifest([FAKE_TRANSACTION], 1, "USD"))
        mocker.patch.object(kraken_csv, "cache_key").return_value = "Test-" + kraken_csv.cache_key()
        mocker.patch.object(kraken_csv, "_Kraken__CACHE_DIRECTORY", "output/kraken_test")
        if not os.path.exists("output/kraken_test"):
            os.makedirs("output/kraken_test")
        with open("input/USD_OHLCVT_test.zip", "rb") as file:
            mocker.patch.object(kraken_csv, "_google_file_to_bytes").return_value = file.read()

        mocker.patch.object(plugin, "_PairConverterPlugin__exchange_csv_reader", {"Kraken": kraken_csv})
        exchange = kraken(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        alt_exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        modified_markets = TEST_MARKETS
        modified_markets["BTCUSDT"] = [ALT_EXCHANGE]
        mocker.patch.object(plugin, "_PairConverterPlugin__exchange_markets", {TEST_EXCHANGE: modified_markets})
        mocker.patch.object(alt_exchange, "fetchOHLCV").return_value = [
            [
                KRAKEN_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # Match the timestamp to assure correct price look up
                float(BTCUSDT_OPEN),  # (O)pen price, float
                float(BTCUSDT_HIGH),  # (H)ighest price, float
                float(BTCUSDT_LOW),  # (L)owest price, float
                float(BTCUSDT_CLOSE),  # (C)losing price, float
                float(BTCUSDT_VOLUME),  # (V)olume (in terms of the base currency), float
            ],
        ]

        mocker.patch.object(exchange, "fetchOHLCV").return_value = [
            [
                KRAKEN_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                float(USDTUSD_OPEN),  # (O)pen price, float
                float(USDTUSD_HIGH),  # (H)ighest price, float
                float(USDTUSD_LOW),  # (L)owest price, float
                float(USDTUSD_CLOSE),  # (C)losing price, float
                float(USDTUSD_VOLUME),  # (V)olume (in terms of the base currency), float
            ],
        ]
        mocker.patch.object(plugin, "_PairConverterPlugin__exchanges", {TEST_EXCHANGE: exchange, ALT_EXCHANGE: alt_exchange})
        mocker.patch.object(plugin, "_generate_unoptimized_graph").return_value = graph_optimized
        mocker.patch.object(plugin, "_PairConverterPlugin__exchange_2_graph_tree", {TEST_EXCHANGE: simple_tree})

        data = plugin.get_historic_bar_from_native_source(KRAKEN_TIMESTAMP, "BTC", "USD", TEST_EXCHANGE)

        assert data
        assert data.timestamp == KRAKEN_TIMESTAMP
        assert data.low == BTCUSDT_LOW * KRAKEN_LOW
        assert data.high == BTCUSDT_HIGH * KRAKEN_HIGH
        assert data.open == BTCUSDT_OPEN * KRAKEN_OPEN
        assert data.close == BTCUSDT_CLOSE * KRAKEN_CLOSE
        assert data.volume == BTCUSDT_VOLUME + KRAKEN_VOLUME

    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr
    def test_locked_exchange(self, mocker: Any, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]]) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(
            Keyword.HISTORICAL_PRICE_HIGH.value, default_exchange=LOCKED_EXCHANGE, exchange_locked=True, fiat_access_key="BOGUS_KEY"
        )
        # Name is changed to exchange_instance to avoid conflicts with the side effect function `add_exchange_side_effect`
        exchange_instance = kraken(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        alt_exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )
        kraken_csv = KrakenCsvPricing(transaction_manifest=TransactionManifest([FAKE_TRANSACTION], 1, "USD"))

        mocker.patch.object(kraken_csv, "find_historical_bar").return_value = None
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_csv_reader", {LOCKED_EXCHANGE: kraken_csv})
        mocker.patch.object(plugin, "_get_request_delay").return_value = 0.0
        mocker.patch.object(exchange_instance, "fetchOHLCV").return_value = [
            [
                BTCUSDT_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                float(BTCUSDT_OPEN),  # (O)pen price, float
                float(BTCUSDT_HIGH),  # (H)ighest price, float
                float(BTCUSDT_LOW),  # (L)owest price, float
                float(BTCUSDT_CLOSE),  # (C)losing price, float
                float(BTCUSDT_VOLUME),  # (V)olume (in terms of the base currency), float
            ],
        ]

        mocker.patch.object(alt_exchange, "fetchOHLCV").return_value = [
            [
                USDTUSD_TIMESTAMP.timestamp() * _MS_IN_SECOND,  # UTC timestamp in milliseconds, integer
                float(USDTUSD_OPEN),  # (O)pen price, float
                float(USDTUSD_HIGH),  # (H)ighest price, float
                float(USDTUSD_LOW),  # (L)owest price, float
                float(USDTUSD_CLOSE),  # (C)losing price, float
                float(USDTUSD_VOLUME),  # (V)olume (in terms of the base currency), float
            ],
        ]

        def add_exchange_side_effect(exchange: str) -> MappedGraph[str]:  # pylint: disable=unused-argument
            mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchanges", {LOCKED_EXCHANGE: exchange_instance})
            mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {"not-kraken": LOCKED_MARKETS})
            mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_2_graph_tree", {"not-kraken": simple_tree})

            return graph_optimized

        mocker.patch.object(plugin, "_cache_graph_snapshots", autospec=True).side_effect = add_exchange_side_effect

        data = plugin.get_historic_bar_from_native_source(BTCUSDT_TIMESTAMP, "BTC", "USD", "not-kraken")

        assert data
        plugin._cache_graph_snapshots.assert_called_once_with("not-kraken")  # type: ignore # pylint: disable=protected-access, no-member

    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr
    def test_optimization_of_graph(self, mocker: Any, graph_fiat_optimized: MappedGraph[str]) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, "BOGUS_KEY")

        self.__btcusdt_mock(plugin, mocker, graph_fiat_optimized)

        test_manifest: TransactionManifest = TransactionManifest([FAKE_TRANSACTION], 1, "USD")

        plugin.optimize(test_manifest)

        optimized_data = plugin.get_historic_bar_from_native_source(BTCUSDT_TIMESTAMP, "BTC", "USD", TEST_EXCHANGE)

        assert optimized_data
        assert optimized_data.timestamp == BTCUSDC_TIMESTAMP
        assert optimized_data.low == BTCUSDC_LOW * USDCUSD_LOW
        assert optimized_data.high == BTCUSDC_HIGH * USDCUSD_HIGH
        assert optimized_data.open == BTCUSDC_OPEN * USDCUSD_OPEN
        assert optimized_data.close == BTCUSDC_CLOSE * USDCUSD_CLOSE
        assert optimized_data.volume == BTCUSDC_VOLUME + USDCUSD_VOLUME

        # Testing for graph compression
        exchange_tree: AVLTree[datetime, MappedGraph[str]] = plugin.exchange_2_graph_tree[TEST_EXCHANGE]
        # There is a bug that is creating an empty optimization at the beginning
        assert exchange_tree._get_height(exchange_tree.root) == 3  # pylint: disable=protected-access

        # Testing if separate snapshot was correctly made
        second_week_timestamp: datetime = BTCUSDT_TIMESTAMP + timedelta(weeks=2)

        new_snapshot = plugin.get_historic_bar_from_native_source(second_week_timestamp, "BTC", "USD", TEST_EXCHANGE)

        assert new_snapshot
        assert new_snapshot.timestamp == second_week_timestamp
        assert new_snapshot.low == BTCUSDT_LOW * USDTUSD_LOW
        assert new_snapshot.high == BTCUSDT_HIGH * USDTUSD_HIGH
        assert new_snapshot.open == BTCUSDT_OPEN * USDTUSD_OPEN
        assert new_snapshot.close == BTCUSDT_CLOSE * USDTUSD_CLOSE
        assert new_snapshot.volume == BTCUSDT_VOLUME + USDTUSD_VOLUME

    @pytest.mark.default_cassette("exchange_rate_host_symbol_call.yaml")
    @pytest.mark.vcr
    def test_base_universal_aliases(
        self,
        mocker: Any,
        graph_optimized: MappedGraph[str],
        simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]],
        simple_pionex_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]],
    ) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value, fiat_access_key="BOGUS_KEY")
        self.__btcusdt_mock_unoptimized(plugin, mocker, graph_optimized, simple_tree)
        pionex_markets: Dict[str, List[str]] = TEST_MARKETS
        pionex_markets.update(PIONEX_MARKETS)
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {PIONEX_EXCHANGE: pionex_markets, TEST_EXCHANGE: TEST_MARKETS})
        mocker.patch.object(
            plugin, "_AbstractCcxtPairConverterPlugin__exchange_2_graph_tree", {PIONEX_EXCHANGE: simple_pionex_tree, TEST_EXCHANGE: simple_tree}
        )

        data = plugin.get_historic_bar_from_native_source(BTCUSDT_TIMESTAMP, "XBT", "USD", TEST_EXCHANGE)

        # XBT should have the exact same price as BTC
        assert data
        assert data.timestamp == BTCUSDT_TIMESTAMP
        assert data.low == BTCUSDT_LOW * USDTUSD_LOW
        assert data.high == BTCUSDT_HIGH * USDTUSD_HIGH
        assert data.open == BTCUSDT_OPEN * USDTUSD_OPEN
        assert data.close == BTCUSDT_CLOSE * USDTUSD_CLOSE
        assert data.volume == BTCUSDT_VOLUME + USDTUSD_VOLUME + RP2Decimal("1")

        # Test micro assets on a specific exchange
        data = plugin.get_historic_bar_from_native_source(BTCUSDT_TIMESTAMP, "MBTC", "USD", "Pionex")

        # MBTC should have 0.001 price as BTC
        assert data
        assert data.timestamp == BTCUSDT_TIMESTAMP
        assert data.low == BTCUSDT_LOW * USDTUSD_LOW * RP2Decimal("0.001")
        assert data.high == BTCUSDT_HIGH * USDTUSD_HIGH * RP2Decimal("0.001")
        assert data.open == BTCUSDT_OPEN * USDTUSD_OPEN * RP2Decimal("0.001")
        assert data.close == BTCUSDT_CLOSE * USDTUSD_CLOSE * RP2Decimal("0.001")
        assert data.volume == BTCUSDT_VOLUME + USDTUSD_VOLUME + RP2Decimal("1")
