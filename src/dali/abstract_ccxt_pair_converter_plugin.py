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

# Disabled for now. Hopefully, we can refactor some of the logic out to MappedGraph.
# pylint: disable=too-many-lines

import logging
from datetime import datetime, timedelta, timezone
from time import sleep, time
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Set, Union, cast

from ccxt import (
    DDoSProtection,
    Exchange,
    ExchangeError,
    ExchangeNotAvailable,
    NetworkError,
    RequestTimeout,
    binance,
    binanceus,
    bitfinex,
    gateio,
    huobi,
    kraken,
    okex,
    upbit,
)
from dateutil.relativedelta import relativedelta
from prezzemolo.avl_tree import AVLTree
from prezzemolo.vertex import Vertex
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError, RP2ValueError

from dali.abstract_pair_converter_plugin import (
    AbstractPairConverterPlugin,
    AssetPairAndTimestamp,
)
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.logger import LOGGER
from dali.mapped_graph import Alias, MappedGraph
from dali.plugin.pair_converter.csv.kraken import Kraken as KrakenCsvPricing
from dali.transaction_manifest import TransactionManifest

# Native format keywords
_ID: str = "id"
_BASE: str = "base"
_QUOTE: str = "quote"
_SYMBOL: str = "symbol"
_TYPE: str = "type"

# Time in ms
_MINUTE: str = "1m"
_FIVE_MINUTE: str = "5m"
_FIFTEEN_MINUTE: str = "15m"
_ONE_HOUR: str = "1h"
_FOUR_HOUR: str = "4h"
_SIX_HOUR: str = "6h"
_ONE_DAY: str = "1d"
_ONE_WEEK: str = "1w"
_TIME_GRANULARITY: List[str] = [_MINUTE, _FIVE_MINUTE, _FIFTEEN_MINUTE, _ONE_HOUR, _FOUR_HOUR, _ONE_DAY, _ONE_WEEK]
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

# Currently supported exchanges
_ALIAS: str = "Alias"  # Virtual exchange - to be removed when teleportation is implemented
_BINANCE: str = "Binance.com"
_BINANCEUS: str = "Binance US"
_BITFINEX: str = "Bitfinex"
_COINBASE: str = "Coinbase"  # Can't be used for pricing
_COINBASE_PRO: str = "Coinbase Pro"
_GATE: str = "Gate"
_HUOBI: str = "Huobi"
_KRAKEN: str = "Kraken"
_OKEX: str = "Okex"
_PIONEX: str = "Pionex"  # Not currently supported by CCXT
_UPBIT: str = "Upbit"
_FIAT_EXCHANGE: str = "Exchangerate.host"
_DEFAULT_EXCHANGE: str = _KRAKEN
_EXCHANGE_DICT: Dict[str, Any] = {
    _BINANCE: binance,
    _BINANCEUS: binanceus,
    _BITFINEX: bitfinex,
    _GATE: gateio,
    _HUOBI: huobi,
    _KRAKEN: kraken,
    _OKEX: okex,
    _UPBIT: upbit,
}
_COINBASE_PRO_GRANULARITY_LIST: List[str] = [_MINUTE, _FIVE_MINUTE, _FIFTEEN_MINUTE, _ONE_HOUR, _SIX_HOUR, _ONE_DAY, _ONE_WEEK]
_TIME_GRANULARITY_DICT: Dict[str, List[str]] = {
    _COINBASE_PRO: _COINBASE_PRO_GRANULARITY_LIST,
}
_NONSTANDARD_GRANULARITY_EXCHANGE_SET: Set[str] = set(_TIME_GRANULARITY_DICT.keys())
_TIME_GRANULARITY_SET: Set[str] = set(_TIME_GRANULARITY) | set(_COINBASE_PRO_GRANULARITY_LIST)

# Delay in fractional seconds before making a request to avoid too many request errors
# Kraken states it has a limit of 1 call per second, but this doesn't seem to be correct.
# It appears Kraken public API is limited to around 12 calls per minute.
# There also appears to be a limit of how many calls per 2 hour time period.
# Being authenticated lowers this limit.
_REQUEST_DELAY_DICT: Dict[str, float] = {_KRAKEN: 5.1, _BITFINEX: 5.0}

# CSV Pricing classes
_CSV_PRICING_DICT: Dict[str, Any] = {_KRAKEN: KrakenCsvPricing}

# Alternative Markets and exchanges for stablecoins or untradeable assets
_ALT_MARKET_EXCHANGES_DICT: Dict[str, str] = {
    "ASTUSDT": _OKEX,
    "ARKKRW": _UPBIT,
    "ATDUSDT": _GATE,
    "BETHETH": _BINANCE,
    "BNBUSDT": _BINANCEUS,
    "BSVUSDT": _GATE,
    "BOBAUSDT": _GATE,
    "BUSDUSDT": _BINANCE,
    "CAKEUSDT": _BINANCE,
    "CYBERUSDT": _BINANCE,
    "EDGUSDT": _GATE,
    "ETHWUSD": _KRAKEN,
    "MAVUSDT": _BINANCE,
    "NEXOUSDT": _BITFINEX,  # To be replaced with Huobi once a CSV plugin is available
    "OPUSDT": _BINANCE,
    "RVNUSDT": _BINANCE,
    "SEIUSDT": _BINANCE,
    "SGBUSD": _KRAKEN,
    "SOLOUSDT": _GATE,  # To be replaced with Binance or Huobi once a CSV plugin is available
    "SWEATUSDT": _GATE,
    "USDTUSD": _KRAKEN,
    "XYMUSDT": _GATE,
}

_ALT_MARKET_BY_BASE_DICT: Dict[str, str] = {
    "AST": "USDT",
    "ARK": "KRW",
    "ATD": "USDT",
    "BETH": "ETH",
    "BNB": "USDT",
    "BOBA": "USDT",
    "BSV": "USDT",
    "BUSD": "USDT",
    "CAKE": "USDT",
    "CYBER": "USDT",
    "EDG": "USDT",
    "ETHW": "USD",
    "MAV": "USDT",
    "NEXO": "USDT",
    "OP": "USDT",
    "RVN": "USDT",
    "SEI": "USDT",
    "SGB": "USD",
    "SOLO": "USDT",
    "SWEAT": "USDT",
    "USDT": "USD",
    "XYM": "USDT",
}

# Sometimes an indirect route (eg. BTC -> USDT -> USD) exists before a native one (eg. BTC -> USD)
# We need to force routing in these cases.
_FORCE_ROUTING: Set[str] = {"OPUSD"}

# Priority for quote asset. If asset is not listed it will be filtered out.
# In principle this should be fiat in order of trade volume and then stable coins in order of trade volume
_QUOTE_PRIORITY: Dict[str, float] = {
    "USD": 1,
    "JPY": 2,
    "KRW": 3,
    "EUR": 4,
    "GBP": 5,
    "AUD": 6,
    "USDT": 7,
    "USDC": 8,
    "BUSD": 9,
    "TUSD": 10,
    "OUSD": 11,
}

# Time constants
_MS_IN_SECOND: int = 1000

# Cache
_CACHE_INTERVAL: int = 200

# Djikstra weights
# Priority should go to quote assets listed above, then other assets, and finally alternatives
STANDARD_WEIGHT: float = 50
_ALTERNATIVE_MARKET_WEIGHT: float = 51

STANDARD_INCREMENT: float = 1

_KRAKEN_PRICE_EXPLAINATION_URL: str = "https://github.com/eprbell/dali-rp2/blob/main/docs/configuration_file.md#a-special-note-for-prices-from-kraken-exchange"

# Used to mark an alias used for all exchanges
_UNIVERSAL: str = "UNIVERSAL"

# First on the list has the most priority
# This is hard-coded for now based on volume of each of these markets for BTC on Coinmarketcap.com
# Any change to this priority should be documented in "docs/configuration_file.md"
FIAT_PRIORITY: Dict[str, float] = {
    "USD": 1,
    "EUR": 2,
    "JPY": 3,
    "KRW": 4,
    "GBP": 5,
    "CAD": 6,
    "AUD": 7,
    "CHF": 8,
}

# If Exchangerates.host is not available or the user does not have an access key, we can use this list
DEFAULT_FIAT_LIST: List[str] = ["AUD", "CAD", "CHF", "EUR", "GBP", "JPY", "NZD", "USD"]

# How much padding in weeks should we add to the graph to catch airdropped or new assets that don't yet have a market
MARKET_PADDING_IN_WEEKS: int = 4

DAYS_IN_WEEK: int = 7

class AssetPairAndHistoricalPrice(NamedTuple):
    from_asset: str
    to_asset: str
    exchange: str
    historical_data: Optional[HistoricalBar] = None


class ExchangeNameAndClass(NamedTuple):
    name: str
    klass: Any


class AbstractCcxtPairConverterPlugin(AbstractPairConverterPlugin):
    def __init__(
        self,
        historical_price_type: str,
        default_exchange: Optional[str] = None,
        exchange_locked: Optional[bool] = None,
        untradeable_assets: Optional[str] = None,
        aliases: Optional[str] = None,
        cache_modifier: Optional[str] = None,
    ) -> None:
        exchange_cache_modifier = "_".join(default_exchange.replace(" ", "_") if default_exchange and exchange_locked else "")
        cache_modifier = cache_modifier if cache_modifier else ""
        self.__cache_modifier = "_".join(x for x in [exchange_cache_modifier, cache_modifier] if x)

        super().__init__(historical_price_type=historical_price_type)
        self._logger: logging.Logger = create_logger(f"{self.name()}/{historical_price_type}")

        self.__exchanges: Dict[str, Exchange] = {}
        self.__exchange_markets: Dict[str, Dict[str, List[str]]] = {}
        self.__exchange_locked: bool = exchange_locked if exchange_locked is not None else False
        self.__default_exchange: str = _DEFAULT_EXCHANGE if default_exchange is None else default_exchange

        # CSV Reader variables
        self.__csv_pricing_dict: Dict[str, Any] = _CSV_PRICING_DICT
        self.__default_csv_reader: ExchangeNameAndClass = ExchangeNameAndClass(_KRAKEN, _CSV_PRICING_DICT[_KRAKEN])
        self.__exchange_csv_reader: Dict[str, Any] = {}

        # key: name of exchange, value: AVLTree of all snapshots of the graph
        # TO BE IMPLEMENTED - Combine all graphs into one graph where assets can 'teleport' between exchanges
        #   This will eliminate the need for markets and this dict, replacing it with just one AVLTree
        self.__exchange_2_graph_tree: Dict[str, AVLTree[datetime, MappedGraph[str]]] = {}
        self.__exchange_last_request: Dict[str, float] = {}
        self._manifest: Optional[TransactionManifest] = None
        self.__transaction_count: int = 0
        if exchange_locked:
            self._logger.debug("Routing locked to single exchange %s.", self.__default_exchange)
        else:
            self._logger.debug("Default exchange assigned as %s. _DEFAULT_EXCHANGE is %s", self.__default_exchange, _DEFAULT_EXCHANGE)
        self.__kraken_warning: bool = False
        self.__untradeable_assets: Set[str] = set(untradeable_assets.split(", ")) if untradeable_assets is not None else set()
        self.__aliases: Optional[Dict[str, Dict[Alias, RP2Decimal]]] = None if aliases is None else self._process_aliases(aliases)
        self._fiat_priority: Dict[str, float] = FIAT_PRIORITY
        self._fiat_list: List[str] = DEFAULT_FIAT_LIST

    def _add_bar_to_cache(self, key: AssetPairAndTimestamp, historical_bar: HistoricalBar) -> None:
        self._cache[self._floor_key(key)] = historical_bar

    def _get_bar_from_cache(self, key: AssetPairAndTimestamp) -> Optional[HistoricalBar]:
        return self._cache.get(self._floor_key(key))

    # All bundle timestamps have 1 millisecond added to them, so will not conflict with the floored timestamps of single bars
    def _add_bundle_to_cache(self, key: AssetPairAndTimestamp, historical_bars: List[HistoricalBar]) -> None:
        self._cache[key] = historical_bars

    def _get_bundle_from_cache(self, key: AssetPairAndTimestamp) -> Optional[List[HistoricalBar]]:
        return cast(List[HistoricalBar], self._cache.get(key))

    # The most granular pricing available is 1 minute, to reduce the size of cache and increase the reuse of pricing data
    def _floor_key(self, key: AssetPairAndTimestamp, daily: bool = False) -> AssetPairAndTimestamp:
        raw_timestamp: datetime = key.timestamp
        floored_timestamp: datetime
        if daily:
            floored_timestamp = raw_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            floored_timestamp = raw_timestamp - timedelta(
                minutes=raw_timestamp.minute % 1, seconds=raw_timestamp.second, microseconds=raw_timestamp.microsecond
            )
        floored_key: AssetPairAndTimestamp = AssetPairAndTimestamp(
            timestamp=floored_timestamp,
            from_asset=key.from_asset,
            to_asset=key.to_asset,
            exchange=key.exchange,
        )

        return floored_key

    def name(self) -> str:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def cache_key(self) -> str:
        return self.name() + "_" + self.__cache_modifier if self.__cache_modifier else self.name()

    def optimize(self, transaction_manifest: TransactionManifest) -> None:
        self._manifest = transaction_manifest

    @property
    def exchanges(self) -> Dict[str, Exchange]:
        return self.__exchanges

    @property
    def exchange_markets(self) -> Dict[str, Dict[str, List[str]]]:
        return self.__exchange_markets

    @property
    def exchange_2_graph_tree(self) -> Dict[str, AVLTree[datetime, MappedGraph[str]]]:
        return self.__exchange_2_graph_tree

    @property
    def fiat_list(self) -> List[str]:
        return self._fiat_list

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        self._logger.debug("Converting %s to %s", from_asset, to_asset)

        # If the assets are the same, return a bar with a price of 1
        if from_asset == to_asset:
            return HistoricalBar(
                duration=timedelta(seconds=604800),
                timestamp=timestamp,
                open=RP2Decimal(1),
                high=RP2Decimal(1),
                low=RP2Decimal(1),
                close=RP2Decimal(1),
                volume=ZERO,
            )

        # If both assets are fiat, skip further processing
        if self._is_fiat_pair(from_asset, to_asset):
            return self._get_fiat_exchange_rate(timestamp, from_asset, to_asset)

        if exchange not in self.__exchange_2_graph_tree:
            self._cache_graph_snapshots(exchange)

        current_markets = self.__exchange_markets[exchange]
        current_graph = self.__exchange_2_graph_tree[exchange].find_max_value_less_than(timestamp)
        if current_graph is None:
            raise RP2RuntimeError(
                "Internal error: The graph snapshot does not exist. It appears that an attempt is being made to route "
                "a price before the graph has been optimized, or an incorrect manifest was sent to the CCXT pair converter."
            )
        from_asset_vertex: Optional[Vertex[str]] = current_graph.get_vertex(from_asset)
        to_asset_vertex: Optional[Vertex[str]] = current_graph.get_vertex(to_asset)
        market_symbol = from_asset + to_asset
        result: Optional[HistoricalBar] = None
        pricing_path: Optional[Iterator[Vertex[str]]] = None

        # TO BE IMPLEMENTED - bypass routing if conversion can be done with one market on the exchange
        if market_symbol in current_markets and market_symbol not in _FORCE_ROUTING:
            self._logger.debug("Found market - %s on single exchange, skipping routing.", market_symbol)
            result = self.find_historical_bar(from_asset, to_asset, timestamp, current_markets[market_symbol][0])
            return result
        # else:
        # Graph building goes here.

        if not from_asset_vertex or not to_asset_vertex:
            if from_asset in self.__untradeable_assets:
                self._logger.info("Untradeable asset found - %s. Assigning ZERO price.", from_asset)
                return HistoricalBar(
                    duration=timedelta(seconds=604800),
                    timestamp=timestamp,
                    open=ZERO,
                    high=ZERO,
                    low=ZERO,
                    close=ZERO,
                    volume=ZERO,
                )
            raise RP2RuntimeError(
                f"The asset {from_asset}({from_asset_vertex}) or {to_asset}({to_asset_vertex}) is missing from {exchange} graph for {timestamp}"
            )

        pricing_path = current_graph.dijkstra(from_asset_vertex, to_asset_vertex, False)

        if pricing_path is None:
            self._logger.debug("No path found for %s to %s. Please open an issue at %s.", from_asset, to_asset, self.issues_url)
            return None

        pricing_path_list: List[str] = [v.name for v in pricing_path]
        self._logger.debug("Found path - %s", pricing_path_list)

        for asset in pricing_path_list:
            if not current_graph.is_optimized(asset):
                raise RP2RuntimeError(f"Internal Error: The asset {asset} is not optimized.")

        conversion_route: List[AssetPairAndHistoricalPrice] = []
        last_node: Optional[str] = None
        hop_bar: Optional[HistoricalBar] = None

        # Build conversion stack, we will iterate over this to find the price for each conversion
        # Then multiply them together to get our final price.
        for node in pricing_path_list:
            if last_node:
                conversion_route.append(
                    AssetPairAndHistoricalPrice(
                        from_asset=last_node,
                        to_asset=node,
                        exchange=_ALIAS if current_graph.is_alias(last_node, node) else current_markets[(last_node + node)][0],
                        historical_data=None,
                    )
                )

            last_node = node

        for i, hop_data in enumerate(conversion_route):
            if self._is_fiat_pair(hop_data.from_asset, hop_data.to_asset):
                hop_bar = self._get_fiat_exchange_rate(timestamp, hop_data.from_asset, hop_data.to_asset)
            elif hop_data.exchange == _ALIAS:
                hop_bar = current_graph.get_alias_bar(hop_data.from_asset, hop_data.to_asset, timestamp)
            else:
                hop_bar = self.find_historical_bar(hop_data.from_asset, hop_data.to_asset, timestamp, hop_data.exchange)

            if hop_bar is not None:
                # Replacing an immutable attribute
                conversion_route[i] = conversion_route[i]._replace(historical_data=hop_bar)
            else:
                self._logger.debug(
                    """No pricing data found for hop. This could be caused by airdropped
                    coins that do not have a market yet. Market - %s%s, Timestamp - %s, Exchange - %s""",
                    hop_data.from_asset,
                    hop_data.to_asset,
                    timestamp,
                    hop_data.exchange,
                )
            if result is not None:
                # TO BE IMPLEMENTED - override Historical Bar * to multiply two bars?
                result = HistoricalBar(
                    duration=max(result.duration, hop_bar.duration),  # type: ignore
                    timestamp=timestamp,
                    open=(result.open * hop_bar.open),  # type: ignore
                    high=(result.high * hop_bar.high),  # type: ignore
                    low=(result.low * hop_bar.low),  # type: ignore
                    close=(result.close * hop_bar.close),  # type: ignore
                    volume=(result.volume + hop_bar.volume),  # type: ignore
                )
            else:
                result = hop_bar

        return result

    def find_historical_bar(self, from_asset: str, to_asset: str, timestamp: datetime, exchange: str) -> Optional[HistoricalBar]:
        key: AssetPairAndTimestamp = AssetPairAndTimestamp(timestamp, from_asset, to_asset, exchange)
        historical_bar: Optional[HistoricalBar] = self._get_bar_from_cache(key)

        if historical_bar is not None:
            self._logger.debug("Retrieved cache for %s/%s->%s for %s", timestamp, from_asset, to_asset, exchange)
            return historical_bar

        historical_bars: Optional[List[HistoricalBar]] = self.find_historical_bars(from_asset, to_asset, timestamp, exchange)

        if historical_bars:
            returned_bar: HistoricalBar = historical_bars[0]
            if (timestamp - returned_bar.timestamp).total_seconds() > _TIME_GRANULARITY_STRING_TO_SECONDS[_ONE_DAY]:
                raise RP2ValueError(
                    "Internal error: The time difference between the requested and returned timestamps is "
                    "greater than a day. The graph probably hasn't been optimized."
                )
            self._add_bar_to_cache(key=key, historical_bar=returned_bar)
            return returned_bar
        return None

    def find_historical_bars(
        self, from_asset: str, to_asset: str, timestamp: datetime, exchange: str, all_bars: bool = False, timespan: str = _MINUTE
    ) -> Optional[List[HistoricalBar]]:
        result: List[HistoricalBar] = []
        retry_count: int = 0
        self.__transaction_count += 1
        if timespan in _TIME_GRANULARITY_SET:
            if exchange in _NONSTANDARD_GRANULARITY_EXCHANGE_SET:
                retry_count = _TIME_GRANULARITY_DICT[exchange].index(timespan)
            else:
                retry_count = _TIME_GRANULARITY.index(timespan)
        else:
            raise RP2ValueError("Internal error: Invalid time span passed to find_historical_bars.")
        current_exchange: Any = self.__exchanges[exchange]
        ms_timestamp: int = int(timestamp.timestamp() * _MS_IN_SECOND)
        csv_pricing: Any = self.__csv_pricing_dict.get(exchange)
        csv_reader: Any = None

        if self.__exchange_csv_reader.get(exchange):
            csv_reader = self.__exchange_csv_reader[exchange]
        elif csv_pricing == self.__default_csv_reader.klass and self.__exchange_csv_reader.get(self.__default_csv_reader.name) is not None:
            csv_reader = self.__exchange_csv_reader.get(self.__default_csv_reader.name)
        elif csv_pricing is not None:
            csv_reader = csv_pricing(self._manifest)

            if csv_pricing == self.__default_csv_reader.klass:
                self.__exchange_csv_reader[self.__default_csv_reader.name] = csv_reader

        if csv_reader:
            csv_bar: Optional[List[HistoricalBar]]
            if all_bars:
                csv_bar = csv_reader.find_historical_bars(from_asset, to_asset, timestamp, True, _ONE_WEEK)
            else:
                csv_bar = [csv_reader.find_historical_bar(from_asset, to_asset, timestamp)]

            # We might want to add a function that adds bars to cache here.

            self.__exchange_csv_reader[exchange] = csv_reader
            if csv_bar is not None and csv_bar[0] is not None:
                if all_bars:
                    timestamp = csv_bar[-1].timestamp + timedelta(milliseconds=1)
                    ms_timestamp = int(timestamp.timestamp() * _MS_IN_SECOND)
                    self._logger.debug(
                        "Retrieved bars up to %s from cache for %s/%s for %s. Continuing with REST API.",
                        str(ms_timestamp),
                        from_asset,
                        to_asset,
                        exchange,
                    )
                    result = csv_bar
                else:
                    self._logger.debug("Retrieved bar from cache - %s for %s/%s->%s for %s", csv_bar, timestamp, from_asset, to_asset, exchange)
                    return csv_bar

        within_last_week: bool = False

        # Get bundles of bars if they exist, saving us from making a call to the API
        if all_bars:
            cached_bundle: Optional[List[HistoricalBar]] = self._get_bundle_from_cache(AssetPairAndTimestamp(timestamp, from_asset, to_asset, exchange))
            if cached_bundle:
                result.extend(cached_bundle)
                timestamp = cached_bundle[-1].timestamp + timedelta(milliseconds=1)
                ms_timestamp = int(timestamp.timestamp() * _MS_IN_SECOND)

            # If the bundle of bars is within the last week, we don't need to pull new optimization data.
            if result and (datetime.now(timezone.utc) - result[-1].timestamp).total_seconds() > _TIME_GRANULARITY_STRING_TO_SECONDS[_ONE_WEEK]:
                within_last_week = True

        while (retry_count < len(_TIME_GRANULARITY_DICT.get(exchange, _TIME_GRANULARITY))) and not within_last_week:
            timeframe: str = _TIME_GRANULARITY_DICT.get(exchange, _TIME_GRANULARITY)[retry_count]
            request_count: int = 0
            historical_data: List[List[Union[int, float]]] = []

            # Most exceptions are caused by request limits of the underlying APIs
            while request_count < 9:
                try:
                    # Excessive calls to the API within a certain window might get an IP temporarily banned
                    if self._get_request_delay(exchange) > 0:
                        current_time = time()
                        second_delay = max(0, self._get_request_delay(exchange) - (current_time - self.__exchange_last_request.get(exchange, 0)))
                        self._logger.debug("Delaying for %s seconds", second_delay)
                        sleep(second_delay)
                        self.__exchange_last_request[exchange] = time()

                    # this is where we pull the historical prices from the underlying exchange
                    if all_bars:
                        historical_data = current_exchange.fetchOHLCV(f"{from_asset}/{to_asset}", timeframe, ms_timestamp, 1500)
                        if len(historical_data) > 0:
                            ms_timestamp = int(historical_data[-1][0]) + 1
                    else:
                        historical_data = current_exchange.fetchOHLCV(f"{from_asset}/{to_asset}", timeframe, ms_timestamp, 1)
                        self._logger.debug(
                            "Got historical_data: %s with ms_timestamp - %s with exchange %s with timeframe - %s",
                            historical_data,
                            ms_timestamp,
                            type(current_exchange),
                            timeframe,
                        )
                    break
                except ExchangeError as exc:
                    self._logger.debug("ExchangeError exception from server. Exception - %s", exc)
                    sleep(0.1)
                    break
                except DDoSProtection as exc:
                    self._logger.debug(
                        "DDosProtection exception from server, most likely too many requests. Making another attempt after 0.1 second delay. Exception - %s",
                        exc,
                    )
                    # logger INFO for retry?
                    sleep(0.1)
                    request_count += 3
                except (ExchangeNotAvailable, NetworkError, RequestTimeout) as exc_na:
                    request_count += 1
                    if request_count > 9:
                        if exchange == _BINANCE:
                            self._logger.info(
                                """
                                Binance server unavailable. Try a non-Binance locked exchange pair converter.
                                Saving to cache and exiting.
                                """
                            )
                        else:
                            self._logger.info("Maximum number of retries reached. Saving to cache and exiting.")
                        self.save_historical_price_cache()
                        raise RP2RuntimeError("Server error") from exc_na

                    self._logger.debug("Server not available. Making attempt #%s of 10 after a ten second delay. Exception - %s", request_count, exc_na)
                    sleep(10)

            if len(historical_data) > 0:
                returned_timestamp = datetime.fromtimestamp(int(historical_data[0][0]) / _MS_IN_SECOND, timezone.utc)
                if (returned_timestamp - timestamp).total_seconds() > _TIME_GRANULARITY_STRING_TO_SECONDS[timeframe] and not all_bars:
                    if retry_count == len(_TIME_GRANULARITY_DICT.get(exchange, _TIME_GRANULARITY)) - 1:  # If this is the last try
                        self._logger.info(
                            "For %s/%s requested candle for %s (ms %s) doesn't match the returned timestamp %s. It is assumed the asset was not tradeable at "
                            "the time of acquisition, so the first weekly candle is used for pricing. Please check the price of %s at %s.",
                            from_asset,
                            to_asset,
                            timestamp,
                            ms_timestamp,
                            returned_timestamp,
                            from_asset,
                            timestamp,
                        )
                    else:
                        self._logger.debug(
                            "For %s/%s requested candle for %s (ms %s), but got %s. Continuing with larger timeframe.",
                            from_asset,
                            to_asset,
                            timestamp,
                            ms_timestamp,
                            returned_timestamp,
                        )
                        retry_count += 1
                        continue

                # If this isn't the smallest timeframe, which is 1m on most exchanges, and this isn't the weekly candle which is the maximum timeframe
                # and addressed with the message above.
                if retry_count > 0 and timeframe != "1w":
                    if exchange == "Kraken" and not self.__kraken_warning:
                        self._logger.info(
                            "Prices from the Kraken exchange for the latest quarter may not be accurate until CSV data is available. For more "
                            "information visit the following URL: %s",
                            _KRAKEN_PRICE_EXPLAINATION_URL,
                        )
                        self.__kraken_warning = True
                    elif exchange != "Kraken":  # This is a different exchange that is having pricing issues, so warn the user.
                        self._logger.info(
                            "The most accurate candle was not able to be used for pricing the asset %s at %s. \n"
                            "The %s candle for %s was used. \n"
                            "The price may be inaccurate. If you feel like you're getting this message in error, \n"
                            "please open an issue at %s",
                            from_asset,
                            timestamp,
                            timeframe,
                            returned_timestamp,
                            self.issues_url,
                        )

            # If there is no candle the list will be empty
            if historical_data:
                if not all_bars:
                    result = [
                        HistoricalBar(
                            duration=timedelta(seconds=_TIME_GRANULARITY_STRING_TO_SECONDS[timeframe]),
                            timestamp=timestamp,
                            open=RP2Decimal(str(historical_data[0][1])),
                            high=RP2Decimal(str(historical_data[0][2])),
                            low=RP2Decimal(str(historical_data[0][3])),
                            close=RP2Decimal(str(historical_data[0][4])),
                            volume=RP2Decimal(str(historical_data[0][5])),
                        )
                    ]
                    break

                for historical_bar in historical_data:
                    result.append(
                        HistoricalBar(
                            duration=timedelta(seconds=_TIME_GRANULARITY_STRING_TO_SECONDS[timeframe]),
                            timestamp=datetime.fromtimestamp(int(historical_bar[0]) / _MS_IN_SECOND, timezone.utc),
                            open=RP2Decimal(str(historical_bar[1])),
                            high=RP2Decimal(str(historical_bar[2])),
                            low=RP2Decimal(str(historical_bar[3])),
                            close=RP2Decimal(str(historical_bar[4])),
                            volume=RP2Decimal(str(historical_bar[5])),
                        )
                    )
            elif all_bars:
                self._add_bundle_to_cache(AssetPairAndTimestamp(timestamp, from_asset, to_asset, exchange), result)
                break  # If historical_data is empty we have hit the end of records and need to return
            else:
                retry_count += 1  # If the singular bar was not found, we need to repeat with a wider timespan

        if self.__transaction_count % _CACHE_INTERVAL == 0:
            self.save_historical_price_cache()

        return result

    def _add_alternative_markets(self, graph: MappedGraph[str], current_markets: Dict[str, List[str]]) -> None:
        for base_asset, quote_asset in _ALT_MARKET_BY_BASE_DICT.items():
            alt_market = base_asset + quote_asset
            alt_exchange_name = _ALT_MARKET_EXCHANGES_DICT[alt_market]

            # TO BE IMPLEMENTED - Add markets to a priority queue inside MappedGraph to prioritize higher volume exchanges
            current_markets[alt_market] = [alt_exchange_name]

            # Cache the exchange so that we can pull prices from it later
            if alt_exchange_name not in self.__exchanges:
                self._logger.debug("Added Alternative Exchange: %s", alt_exchange_name)
                alt_exchange: Exchange = _EXCHANGE_DICT[alt_exchange_name]()
                self.__exchanges[alt_exchange_name] = alt_exchange

            # If the asset name doesn't exist, the MappedGraph will create a vertex with that name and add it to the graph
            # If it does exist it will look it up in the dictionary by name and add the neighbor to that vertex.
            if base_asset not in self.__untradeable_assets:
                self._logger.debug("Added %s:%s to graph.", base_asset, quote_asset)
                graph.add_neighbor(base_asset, quote_asset, _ALTERNATIVE_MARKET_WEIGHT)

    def _cache_graph_snapshots(self, exchange: str) -> None:
        # TO BE IMPLEMENTED - If asset is missing from manifest, warn user and reoptimize.
        if self.__exchange_2_graph_tree.get(exchange):
            raise RP2ValueError(
                f"Internal Error: You have already generated graph snapshots for exchange - {exchange}. " f"Optimization can only be performed once."
            )

        if self._manifest:
            unoptimized_graph: MappedGraph[str] = self._generate_unoptimized_graph(exchange)
        else:
            # TO BE IMPLEMENTED - Set a default start time of the earliest possible crypto trade.
            raise RP2ValueError("Internal error: No manifest provided for the CCXT pair converter plugin. Unable to optimize the graph.")

        # Key: name of asset being optimized, value: key -> neighboring asset, value -> weight of the connection
        optimizations: Dict[datetime, Dict[str, Dict[str, float]]]
        optimizations = self._optimize_assets_for_exchange(
            unoptimized_graph,
            self._manifest.first_transaction_datetime,
            self._manifest.assets,
            exchange,
        )
        self._logger.debug("Optimizations created for graph: %s", optimizations)

        exchange_tree: AVLTree[datetime, MappedGraph[str]] = AVLTree[datetime, MappedGraph[str]]()
        pruned_graph: MappedGraph[str] = unoptimized_graph.prune_graph(optimizations[next(iter(optimizations))])
        for timestamp, optimization in optimizations.items():
            # Since weeks don't align across exchanges, optimizations from previous graphs, which may correspond to different
            # sources with different starting days for weeks (e.g. source A starts on Monday, source B starts on Thursday)
            previous_graph: Optional[MappedGraph[str]] = exchange_tree.find_max_value_less_than(timestamp)
            graph_snapshot: MappedGraph[str]
            if previous_graph:
                graph_snapshot = previous_graph.clone_with_optimization(optimization)
            else:
                graph_snapshot = pruned_graph.clone_with_optimization(optimization)
            exchange_tree.insert_node(timestamp, graph_snapshot)
            self._logger.debug("Added graph snapshot AVLTree for %s for timestamp: %s", exchange, timestamp)

        self.__exchange_2_graph_tree[exchange] = exchange_tree

        # Add unoptimized_graph to the last week?

    def _find_following_monday(self, timestamp: datetime) -> datetime:
        following_monday: datetime = timestamp + timedelta(days=-timestamp.weekday(), weeks=1)
        return following_monday.replace(hour=0, minute=0, second=0, microsecond=0)

    # Isolated for testing
    def _get_pricing_exchange_for_exchange(self, exchange: str) -> str:
        if exchange == Keyword.UNKNOWN.value or exchange not in _EXCHANGE_DICT or self.__exchange_locked:
            if self.__exchange_locked:
                self._logger.debug("Price routing locked to %s type for %s.", self.__default_exchange, exchange)
            else:
                self._logger.debug("Using default exchange %s type for %s.", self.__default_exchange, exchange)

            csv_pricing_class: Any = self.__csv_pricing_dict.get(self.__default_exchange)
            if csv_pricing_class:
                self.__default_csv_reader = ExchangeNameAndClass(self.__default_exchange, csv_pricing_class)
                self.__csv_pricing_dict[exchange] = self.__default_csv_reader.klass
            exchange = self.__default_exchange

        # The exchange could have been added as an alt; if so markets wouldn't have been built
        if exchange not in self.__exchange_2_graph_tree or exchange not in self.__exchange_markets:
            if self.__exchange_locked:
                exchange = self.__default_exchange
            elif exchange not in _EXCHANGE_DICT:
                raise RP2ValueError(f"WARNING: Unrecognized Exchange: {exchange}. Please open an issue at {self.issues_url}")

        return exchange

    def _generate_unoptimized_graph(self, exchange: str) -> MappedGraph[str]:
        pricing_exchange: str = self._get_pricing_exchange_for_exchange(exchange)
        exchange_name: str = exchange

        if exchange_name not in self.__exchanges:
            # initializes the cctx exchange instance which is used to get the historical data
            # https://docs.ccxt.com/en/latest/manual.html#notes-on-rate-limiter
            self._logger.debug("Trying to instantiate exchange %s", exchange)
            current_exchange = _EXCHANGE_DICT[pricing_exchange]({"enableRateLimit": True})
        else:
            current_exchange = self.__exchanges[exchange_name]

        # key: market, value: exchanges where the market is available in order of priority
        current_markets: Dict[str, List[str]] = {}
        self._logger.debug("Creating graph for %s", pricing_exchange)
        current_graph: MappedGraph[str] = MappedGraph[str](exchange_name, aliases=self.__aliases)

        for alias in current_graph.aliases:
            current_markets[f"{alias.from_asset}{alias.to_asset}"] = [exchange]

        for market in filter(lambda x: x[_TYPE] == "spot" and x[_QUOTE] in _QUOTE_PRIORITY, current_exchange.fetch_markets()):
            self._logger.debug("Market: %s", market)

            current_markets[f"{market[_BASE]}{market[_QUOTE]}"] = [exchange]

            # TO BE IMPLEMENTED - lazy build graph only if needed

            # If the asset name doesn't exist, the MappedGraph will create a vertex with that name and add it to the graph
            # If it does exist it will look it up in the dictionary by name and add the neighbor to that vertex.
            current_graph.add_neighbor(market[_BASE], market[_QUOTE], _QUOTE_PRIORITY.get(market[_QUOTE], STANDARD_WEIGHT))

        # Add alternative markets if they don't exist
        if not self.__exchange_locked:
            self._logger.debug("Adding alternative markets to %s graph.", exchange)
            self._add_alternative_markets(current_graph, current_markets)

        self._add_fiat_edges_to_graph(current_graph, current_markets)
        self._logger.debug("Created unoptimized graph for %s : %s", exchange, current_graph)
        self.__exchanges[exchange_name] = current_exchange
        self.__exchange_markets[exchange_name] = current_markets

        return current_graph

    # Isolated to be mocked
    def _get_request_delay(self, exchange: str) -> float:
        return _REQUEST_DELAY_DICT.get(exchange, 0)

    def _optimize_assets_for_exchange(
        self, unoptimized_graph: MappedGraph[str], start_date: datetime, assets: Set[str], exchange: str
    ) -> Dict[datetime, Dict[str, Dict[str, float]]]:
        optimization_candidates: Set[Vertex[str]] = set()
        market_starts: Dict[str, Dict[str, datetime]] = {}
        # Weekly candles can start on any weekday depending on the exchange, we pull a week early to make sure we pull a full week.
        week_start_date = self._get_previous_monday(start_date)

        # Gather all valid candidates for optimization
        for asset in assets:
            current_vertex: Optional[Vertex[str]] = unoptimized_graph.get_vertex(asset)

            # Some assets might not be available on this particular exchange
            if current_vertex is None:
                continue

            optimization_candidates.add(current_vertex)

            # Find all the neighbors of this vertex and all their neighbors as a set
            children: Optional[Set[Vertex[str]]] = unoptimized_graph.get_all_children_of_vertex(current_vertex)
            if children:
                self._logger.debug("For vertex - %s, found all the children - %s", current_vertex.name, [child.name for child in children])
                optimization_candidates.update(children)

        # This prevents the algo from optimizing fiat assets which do not need optimization
        self._logger.debug("Checking if any of the following candidates are optimized - %s", [candidate.name for candidate in optimization_candidates])
        unoptimized_assets = {candidate.name for candidate in optimization_candidates if not unoptimized_graph.is_optimized(candidate.name)}
        self._logger.debug("Found unoptimized assets %s", unoptimized_assets)

        child_bars: Dict[str, Dict[str, List[HistoricalBar]]] = {}
        optimizations: Dict[datetime, Dict[str, Dict[str, float]]] = {}
        optimizations[week_start_date] = {}

        # Alternative market correction
        for asset in self.__untradeable_assets:
            optimizations[week_start_date][asset] = {}

        # Retrieve historical week bars/candles to use for optimization
        for child_name in unoptimized_assets:
            child_bars[child_name] = {}
            bar_check: Optional[List[HistoricalBar]] = None
            market_starts[child_name] = {}
            child_vertex: Optional[Vertex[str]] = unoptimized_graph.get_vertex(child_name)
            child_neighbors: Iterator[Vertex[str]] = child_vertex.neighbors if child_vertex is not None else iter([])
            for neighbor in child_neighbors:
                if neighbor in optimization_candidates:
                    bar_check = self.find_historical_bars(
                        child_name, neighbor.name, week_start_date, self.__exchange_markets[exchange][child_name + neighbor.name][0], True, _ONE_WEEK
                    )

                # if not None or empty list []
                if bar_check:
                    # We pad the first part of the graph in case an asset has been airdropped or otherwise given to a user before a
                    # market becomes available. Later, when the price is retrieved, the timestamps won't match and the user will be warned.
                    no_market_padding: HistoricalBar = HistoricalBar(
                        duration=bar_check[0].duration,
                        timestamp=bar_check[0].timestamp - timedelta(weeks=MARKET_PADDING_IN_WEEKS),  # Make this a parameter users can set?
                        open=bar_check[0].open,
                        high=bar_check[0].high,
                        low=bar_check[0].low,
                        close=bar_check[0].close,
                        volume=bar_check[0].volume,
                    )
                    bar_check = [no_market_padding] + bar_check

                    child_bars[child_name][neighbor.name] = bar_check
                    timestamp_diff: float = (child_bars[child_name][neighbor.name][0].timestamp - start_date).total_seconds()

                    # Find the start of the market if it is after the first transaction
                    if timestamp_diff > _TIME_GRANULARITY_STRING_TO_SECONDS[_ONE_WEEK]:
                        market_starts[child_name][neighbor.name] = child_bars[child_name][neighbor.name][0].timestamp
                    else:
                        market_starts[child_name][neighbor.name] = week_start_date - timedelta(weeks=1)
                else:
                    # This is a bogus market, either the exchange is misreporting it or it is not available from first transaction datetime
                    # By setting the start date far into the future this market will be deleted from the graph snapshots
                    market_starts[child_name][neighbor.name] = datetime.now() + relativedelta(years=100)

        # Save all the bundles of bars we just retrieved
        self.save_historical_price_cache()

        # Convert bar dict into dict with optimizations
        # First we sort the bars
        for crypto_asset, neighbor_assets in child_bars.items():
            for neighbor_asset, historical_bars in neighbor_assets.items():
                for historical_bar in historical_bars:
                    timestamp: datetime = historical_bar.timestamp
                    volume: RP2Decimal = historical_bar.volume

                    # sort bars first by timestamp, then asset, then the asset's neighbor and it's volume
                    if timestamp not in optimizations:
                        optimizations[timestamp] = {}
                    if crypto_asset not in optimizations[timestamp]:
                        optimizations[timestamp][crypto_asset] = {}

                    # This deletes markets before they start by setting the weight to a negative
                    if timestamp < market_starts[crypto_asset].get(neighbor_asset, start_date):
                        self._logger.debug("Optimization for %s to %s at %s is -1.0", crypto_asset, neighbor_asset, timestamp)
                        optimizations[timestamp][crypto_asset][neighbor_asset] = -1.0
                    else:
                        optimizations[timestamp][crypto_asset][neighbor_asset] = float(volume)
                        self._logger.debug("Optimization for %s to %s at %s is %s", crypto_asset, neighbor_asset, timestamp, volume)

        # Sort the optimizations by timestamp
        # while caching the graph snapshots, the partial optimizations will be layered on to the previous
        # timestamp's optimizations
        sorted_optimizations = dict(sorted(optimizations.items(), key=lambda x: x[0]))

        # Copy over assets from previous timestamps so there are no holes in the graph
        composite_optimizations: Dict[datetime, Dict[str, Dict[str, float]]] = {}
        previous_timestamp: Optional[datetime] = None

        for timestamp, optimization_dict in sorted_optimizations.items():
            if previous_timestamp is None:
                composite_optimizations[timestamp] = optimization_dict
            else:
                composite_optimizations[timestamp] = sorted_optimizations[previous_timestamp].copy()
                composite_optimizations[timestamp].update(sorted_optimizations[timestamp])

        previous_assets: Optional[Dict[str, Dict[str, float]]] = None
        timestamps_to_delete: List[datetime] = []
        # Next, we assign weights based on the rank of the volume
        for timestamp, snapshot_assets in composite_optimizations.items():
            for asset, neighbors in snapshot_assets.items():
                ranked_neighbors: Dict[str, float] = dict(sorted(iter(neighbors.items()), key=lambda x: x[1], reverse=True))
                weight: float = 1.0
                for neighbor_name, neighbor_volume in ranked_neighbors.items():
                    if neighbor_volume != -1.0:
                        neighbors[neighbor_name] = weight
                        weight += 1.0
                        self._logger.debug("Optimization for %s to %s at %s is %s", asset, neighbor_name, timestamp, neighbors[neighbor_name])
                    else:
                        neighbors[neighbor_name] = -1.0

            # mark duplicate successive snapshots
            if snapshot_assets == previous_assets:
                timestamps_to_delete.append(timestamp)

            previous_assets = snapshot_assets

        # delete duplicates
        for timestamp in timestamps_to_delete:
            del composite_optimizations[timestamp]

        return composite_optimizations

    def _process_aliases(self, aliases: str) -> Dict[str, Dict[Alias, RP2Decimal]]:
        alias_list: List[str] = aliases.split(";")
        processed_aliases: Dict[str, Dict[Alias, RP2Decimal]] = {}

        for alias in alias_list:
            alias_properties: List[str] = alias.split(",")
            if alias_properties[0] in _EXCHANGE_DICT or alias_properties[0] == _UNIVERSAL:
                exchange: str = alias_properties[0]
            else:
                raise RP2ValueError(f"Exchange - {alias_properties[0]} is not supported at this time. Check the spelling of the exchange.")

            processed_aliases.setdefault(exchange, {}).update({Alias(alias_properties[1], alias_properties[2]): RP2Decimal(alias_properties[3])})

        return processed_aliases

    def _get_previous_monday(self, date: datetime) -> datetime:
        days_behind = (date.weekday() + 1) % DAYS_IN_WEEK
        return date - timedelta(days=days_behind)

    def _get_fiat_exchange_rate(self, timestamp: datetime, from_asset: str, to_asset: str) -> Optional[HistoricalBar]:
        raise NotImplementedError("The _get_fiat_exchange_rate method must be overridden.")

    def _build_fiat_list(self) -> None:
        raise NotImplementedError("The _build_fiat_list method must be overridden.")

    def _is_fiat(self, asset: str) -> bool:
        if not self._fiat_list:
            self._build_fiat_list()

        return asset in self._fiat_list

    def _is_fiat_pair(self, from_asset: str, to_asset: str) -> bool:
        return self._is_fiat(from_asset) and self._is_fiat(to_asset)

    def _add_fiat_edges_to_graph(self, graph: MappedGraph[str], markets: Dict[str, List[str]]) -> None:
        if not self._fiat_list:
            self._build_fiat_list()

        for fiat in self._fiat_list:
            to_fiat_list: Dict[str, None] = dict.fromkeys(self._fiat_list.copy())
            del to_fiat_list[fiat]
            # We don't want to add a fiat vertex here because that would allow a double hop on fiat (eg. USD -> KRW -> JPY)
            if graph.get_vertex(fiat):
                for to_be_added_fiat in to_fiat_list:
                    graph.add_fiat_neighbor(
                        fiat,
                        to_be_added_fiat,
                        self._fiat_priority.get(fiat, STANDARD_WEIGHT),
                        True,  # use set optimization
                    )

                LOGGER.debug("Added to assets for %s: %s", fiat, to_fiat_list)

            for to_fiat in to_fiat_list:
                fiat_market = f"{fiat}{to_fiat}"
                markets[fiat_market] = [_FIAT_EXCHANGE]
