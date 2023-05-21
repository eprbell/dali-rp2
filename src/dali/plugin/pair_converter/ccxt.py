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

import logging
from datetime import datetime, timedelta
from inspect import Signature, signature
from time import sleep, time
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Union

from ccxt import (
    DDoSProtection,
    Exchange,
    ExchangeError,
    ExchangeNotAvailable,
    NetworkError,
    RequestTimeout,
    binance,
    coinbasepro,
    gateio,
    huobi,
    kraken,
    upbit,
)
from prezzemolo.vertex import Vertex
from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2RuntimeError

from dali.abstract_pair_converter_plugin import (
    AbstractPairConverterPlugin,
    AssetPairAndTimestamp,
    MappedGraph,
)
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.plugin.pair_converter.csv.kraken import Kraken as KrakenCsvPricing

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
_TIME_GRANULARITY: List[str] = [_MINUTE, _FIVE_MINUTE, _FIFTEEN_MINUTE, _ONE_HOUR, _FOUR_HOUR, _ONE_DAY]
_TIME_GRANULARITY_STRING_TO_SECONDS: Dict[str, int] = {
    _MINUTE: 60,
    _FIVE_MINUTE: 300,
    _FIFTEEN_MINUTE: 900,
    _ONE_HOUR: 3600,
    _FOUR_HOUR: 14400,
    _SIX_HOUR: 21600,
    _ONE_DAY: 86400,
}

# Currently supported exchanges
_BINANCE: str = "Binance.com"
_COINBASE_PRO: str = "Coinbase Pro"
_GATE: str = "Gate"
_HUOBI: str = "Huobi"
_KRAKEN: str = "Kraken"
_UPBIT: str = "Upbit"
_FIAT_EXCHANGE: str = "Exchangerate.host"
_DEFAULT_EXCHANGE: str = _KRAKEN
_EXCHANGE_DICT: Dict[str, Any] = {_BINANCE: binance, _COINBASE_PRO: coinbasepro, _GATE: gateio, _HUOBI: huobi, _KRAKEN: kraken, _UPBIT: upbit}
_TIME_GRANULARITY_DICT: Dict[str, List[str]] = {
    _COINBASE_PRO: [_MINUTE, _FIVE_MINUTE, _FIFTEEN_MINUTE, _ONE_HOUR, _SIX_HOUR, _ONE_DAY],
}


# Delay in fractional seconds before making a request to avoid too many request errors
# Kraken states it has a limit of 1 call per second, but this doesn't seem to be correct.
# It appears Kraken public API is limited to around 12 calls per minute.
# There also appears to be a limit of how many calls per 2 hour time period.
# Being authenticated lowers this limit.
_REQUEST_DELAYDICT: Dict[str, float] = {_KRAKEN: 5.1}

# CSV Pricing classes
_CSV_PRICING_DICT: Dict[str, Any] = {_KRAKEN: KrakenCsvPricing}

# Alternative Markets and exchanges for stablecoins or untradeable assets
_ALT_MARKET_EXCHANGES_DICT: Dict[str, str] = {
    "ARKKRW": _UPBIT,
    "XYMUSDT": _GATE,
    "ATDUSDT": _GATE,
    "BETHETH": _BINANCE,
    "BNBUSDT": _BINANCE,
    "BSVUSDT": _GATE,
    "BOBAUSDT": _GATE,
    "BUSDUSDT": _BINANCE,
    "EDGUSDT": _GATE,
    "ETHWUSD": _KRAKEN,
    "NEXOUSDT": _BINANCE,
    "SGBUSD": _KRAKEN,
    "SOLOUSDT": _HUOBI,
    "USDTUSD": _KRAKEN,
}

_ALT_MARKET_BY_BASE_DICT: Dict[str, str] = {
    "ARK": "KRW",
    "XYM": "USDT",
    "ATD": "USDT",
    "BETH": "ETH",
    "BNB": "USDT",
    "BOBA": "USDT",
    "BSV": "USDT",
    "BUSD": "USDT",
    "EDG": "USDT",
    "ETHW": "USD",
    "NEXO": "USDT",
    "SGB": "USD",
    "SOLO": "USDT",
    "USDT": "USD",
}

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

# CSV Reader
_GOOGLE_API_KEY: str = "google_api_key"

# Djikstra weights
# Priority should go to quote assets listed above, then other assets, and finally alternatives
_STANDARD_WEIGHT: float = 50
_ALTERNATIVE_MARKET_WEIGHT: float = 51


class AssetPairAndHistoricalPrice(NamedTuple):
    from_asset: str
    to_asset: str
    exchange: str
    historical_data: Optional[HistoricalBar] = None


class PairConverterPlugin(AbstractPairConverterPlugin):
    def __init__(
        self,
        historical_price_type: str,
        default_exchange: Optional[str] = None,
        fiat_priority: Optional[str] = None,
        google_api_key: Optional[str] = None,
        exchange_locked: Optional[bool] = None,
    ) -> None:
        exchange_cache_modifier = default_exchange.replace(" ", "_") if default_exchange and exchange_locked else ""
        fiat_priority_cache_modifier = fiat_priority if fiat_priority else ""
        self.__cache_modifier = "_".join(x for x in [exchange_cache_modifier, fiat_priority_cache_modifier] if x)

        super().__init__(historical_price_type=historical_price_type, fiat_priority=fiat_priority)
        self.__logger: logging.Logger = create_logger(f"{self.name()}/{historical_price_type}")

        self.__exchanges: Dict[str, Exchange] = {}
        self.__exchange_markets: Dict[str, Dict[str, List[str]]] = {}
        self.__google_api_key: Optional[str] = google_api_key
        self.__exchange_locked: bool = exchange_locked if exchange_locked is not None else False
        self.__default_exchange: str = _DEFAULT_EXCHANGE if default_exchange is None else default_exchange
        self.__exchange_csv_reader: Dict[str, Any] = {}
        # key: name of exchange, value: graph that prioritizes that exchange
        self.__exchange_graphs: Dict[str, MappedGraph[str]] = {}
        self.__exchange_last_request: Dict[str, float] = {}
        if exchange_locked:
            self.__logger.debug("Routing locked to single exchange %s.", self.__default_exchange)
        else:
            self.__logger.debug("Default exchange assigned as %s. _DEFAULT_EXCHANGE is %s", self.__default_exchange, _DEFAULT_EXCHANGE)

    def name(self) -> str:
        return "CCXT-converter"

    def cache_key(self) -> str:
        return self.name() + "_" + self.__cache_modifier if self.__cache_modifier else self.name()

    @property
    def exchanges(self) -> Dict[str, Exchange]:
        return self.__exchanges

    @property
    def exchange_markets(self) -> Dict[str, Dict[str, List[str]]]:
        return self.__exchange_markets

    @property
    def exchange_graphs(self) -> Dict[str, MappedGraph[str]]:
        return self.__exchange_graphs

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        self.__logger.debug("Converting %s to %s", from_asset, to_asset)

        # If both assets are fiat, skip further processing
        if self._is_fiat_pair(from_asset, to_asset):
            return self._get_fiat_exchange_rate(timestamp, from_asset, to_asset)

        if exchange == Keyword.UNKNOWN.value or exchange not in _EXCHANGE_DICT or self.__exchange_locked:
            if self.__exchange_locked:
                self.__logger.debug("Price routing locked to %s type for %s.", self.__default_exchange, exchange)
            else:
                self.__logger.debug("Using default exchange %s type for %s.", self.__default_exchange, exchange)
            exchange = self.__default_exchange

        # The exchange could have been added as an alt; if so markets wouldn't have been built
        if exchange not in self.__exchanges or exchange not in self.__exchange_markets:
            if self.__exchange_locked:
                self._add_exchange_to_memcache(self.__default_exchange)
            elif exchange in _EXCHANGE_DICT:
                self._add_exchange_to_memcache(exchange)
            else:
                self.__logger.error("WARNING: Unrecognized Exchange: %s. Please open an issue at %s", exchange, self.issues_url)
                return None

        current_markets = self.__exchange_markets[exchange]
        current_graph = self.__exchange_graphs[exchange]
        from_asset_vertex: Optional[Vertex[str]] = current_graph.get_vertex(from_asset)
        to_asset_vertex: Optional[Vertex[str]] = current_graph.get_vertex(to_asset)
        market_symbol = from_asset + to_asset
        result: Optional[HistoricalBar] = None
        pricing_path: Optional[Iterator[Vertex[str]]] = None

        # TO BE IMPLEMENTED - bypass routing if conversion can be done with one market on the exchange
        if market_symbol in current_markets and (exchange in current_markets[market_symbol]):
            self.__logger.debug("Found market - %s on single exchange, skipping routing.", market_symbol)
            result = self.find_historical_bar(from_asset, to_asset, timestamp, exchange)
            return result
        # else:
        # Graph building goes here.

        if not from_asset_vertex or not to_asset_vertex:
            raise RP2RuntimeError(f"The asset {from_asset} or {to_asset} is missing from graph")
        pricing_path = current_graph.dijkstra(from_asset_vertex, to_asset_vertex, False)

        if pricing_path is None:
            self.__logger.debug("No path found for %s to %s. Please open an issue at %s.", from_asset, to_asset, self.issues_url)
            return None

        pricing_path_list: List[str] = [v.name for v in pricing_path]
        self.__logger.debug("Found path - %s", pricing_path_list)

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
                        exchange=current_markets[(last_node + node)][0],
                        historical_data=None,
                    )
                )

            last_node = node

        for i, hop_data in enumerate(conversion_route):
            if self._is_fiat_pair(hop_data.from_asset, hop_data.to_asset):
                hop_bar = self._get_fiat_exchange_rate(timestamp, hop_data.from_asset, hop_data.to_asset)
            else:
                hop_bar = self.find_historical_bar(hop_data.from_asset, hop_data.to_asset, timestamp, hop_data.exchange)

            if hop_bar is not None:
                # Replacing an immutable attribute
                conversion_route[i] = conversion_route[i]._replace(historical_data=hop_bar)
            else:
                self.__logger.debug(
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
        result: Optional[HistoricalBar] = None
        retry_count: int = 0
        current_exchange: Any = self.__exchanges[exchange]
        ms_timestamp: int = int(timestamp.timestamp() * _MS_IN_SECOND)
        key: AssetPairAndTimestamp = AssetPairAndTimestamp(timestamp, from_asset, to_asset, exchange)
        historical_bar: Optional[HistoricalBar] = self._get_bar_from_cache(key)
        csv_pricing: Any = _CSV_PRICING_DICT.get(exchange)
        csv_reader: Any = None

        if historical_bar is not None:
            self.__logger.debug("Retrieved cache for %s/%s->%s for %s", timestamp, from_asset, to_asset, exchange)
            return historical_bar

        if self.__exchange_csv_reader.get(exchange):
            csv_reader = self.__exchange_csv_reader[exchange]
        elif csv_pricing is not None:
            csv_signature: Signature = signature(csv_pricing)

            # a Google API key is necessary to interact with Google Drive since Google restricts API calls to avoid spam, etc...
            if _GOOGLE_API_KEY in csv_signature.parameters:
                if self.__google_api_key is not None:
                    csv_reader = csv_pricing(self.__google_api_key)
                else:
                    self.__logger.info(
                        "Google API Key is not set. Setting the Google API key in the CCXT pair converter plugin could speed up pricing resolution"
                    )
            else:
                csv_reader = csv_pricing()

        if csv_reader:
            csv_bar: Optional[HistoricalBar] = csv_reader.find_historical_bar(from_asset, to_asset, timestamp)

            if csv_bar:
                self._add_bar_to_cache(key=AssetPairAndTimestamp(timestamp, from_asset, to_asset, exchange), historical_bar=csv_bar)

            historical_bar = self._get_bar_from_cache(key)
            self.__exchange_csv_reader[exchange] = csv_reader
            if historical_bar is not None:
                self.__logger.debug("Retrieved bar cache - %s for %s/%s->%s for %s", historical_bar, key.timestamp, key.from_asset, key.to_asset, key.exchange)
                return historical_bar

        while retry_count < len(_TIME_GRANULARITY_DICT.get(exchange, _TIME_GRANULARITY)):
            timeframe: str = _TIME_GRANULARITY_DICT.get(exchange, _TIME_GRANULARITY)[retry_count]
            request_count: int = 0
            historical_data: List[List[Union[int, float]]] = []

            # Most exceptions are caused by request limits of the underlying APIs
            while request_count < 9:
                try:
                    # Excessive calls to the API within a certain window might get an IP temporarily banned
                    if _REQUEST_DELAYDICT.get(exchange, 0) > 0:
                        current_time = time()
                        second_delay = max(0, _REQUEST_DELAYDICT[exchange] - (current_time - self.__exchange_last_request.get(exchange, 0)))
                        self.__logger.debug("Delaying for %s seconds", second_delay)
                        sleep(second_delay)
                        self.__exchange_last_request[exchange] = time()

                    # this is where we pull the historical prices from the underlying exchange
                    historical_data = current_exchange.fetchOHLCV(f"{from_asset}/{to_asset}", timeframe, ms_timestamp, 1)
                    break
                except (DDoSProtection, ExchangeError) as exc:
                    self.__logger.debug(
                        "Exception from server, most likely too many requests. Making another attempt after 0.1 second delay. Exception - %s", exc
                    )
                    # logger INFO for retry?
                    sleep(0.1)
                    request_count += 3
                except (ExchangeNotAvailable, NetworkError, RequestTimeout) as exc_na:
                    request_count += 1
                    if request_count > 9:
                        if exchange == _BINANCE:
                            self.__logger.info(
                                """
                                Binance server unavailable. Try a non-Binance locked exchange pair converter.
                                Saving to cache and exiting.
                                """
                            )
                        else:
                            self.__logger.info("Maximum number of retries reached. Saving to cache and exiting.")
                        self.save_historical_price_cache()
                        raise RP2RuntimeError("Server error") from exc_na

                    self.__logger.debug("Server not available. Making attempt #%s of 10 after a ten second delay. Exception - %s", request_count, exc_na)
                    sleep(10)

            # If there is no candle the list will be empty
            if historical_data:
                result = HistoricalBar(
                    duration=timedelta(seconds=_TIME_GRANULARITY_STRING_TO_SECONDS[timeframe]),
                    timestamp=timestamp,
                    open=RP2Decimal(str(historical_data[0][1])),
                    high=RP2Decimal(str(historical_data[0][2])),
                    low=RP2Decimal(str(historical_data[0][3])),
                    close=RP2Decimal(str(historical_data[0][4])),
                    volume=RP2Decimal(str(historical_data[0][5])),
                )
                break

            retry_count += 1

        # Save the individual pair to cache
        if result is not None:
            self._add_bar_to_cache(key, result)

        return result

    def _add_alternative_markets(self, graph: MappedGraph[str], current_markets: Dict[str, List[str]]) -> None:
        for base_asset, quote_asset in _ALT_MARKET_BY_BASE_DICT.items():
            alt_market = base_asset + quote_asset
            alt_exchange_name = _ALT_MARKET_EXCHANGES_DICT[alt_market]

            # TO BE IMPLEMENTED - Add markets to a priority queue inside MappedGraph to prioritize higher volume exchanges
            current_markets[alt_market] = [alt_exchange_name]

            # Cache the exchange so that we can pull prices from it later
            if alt_exchange_name not in self.__exchanges:
                self.__logger.debug("Added Alternative Exchange: %s", alt_exchange_name)
                alt_exchange: Exchange = _EXCHANGE_DICT[alt_exchange_name]()
                self.__exchanges[alt_exchange_name] = alt_exchange

            # If the asset name doesn't exist, the MappedGraph will create a vertex with that name and add it to the graph
            # If it does exist it will look it up in the dictionary by name and add the neighbor to that vertex.
            graph.add_neighbor(base_asset, quote_asset, _ALTERNATIVE_MARKET_WEIGHT)

    def _add_exchange_to_memcache(self, exchange: str) -> None:
        if exchange not in self.__exchanges:
            # initializes the cctx exchange instance which is used to get the historical data
            # https://docs.ccxt.com/en/latest/manual.html#notes-on-rate-limiter
            current_exchange: Exchange = _EXCHANGE_DICT[exchange]({"enableRateLimit": True})
        else:
            current_exchange = self.__exchanges[exchange]

        # key: market, value: exchanges where the market is available in order of priority
        current_markets: Dict[str, List[str]] = {}
        current_graph: MappedGraph[str] = MappedGraph[str]()

        for market in filter(lambda x: x[_TYPE] == "spot" and x[_QUOTE] in _QUOTE_PRIORITY, current_exchange.fetch_markets()):
            self.__logger.debug("Market: %s", market)

            current_markets[f"{market[_BASE]}{market[_QUOTE]}"] = [exchange]

            # TO BE IMPLEMENTED - lazy build graph only if needed

            # If the asset name doesn't exist, the MappedGraph will create a vertex with that name and add it to the graph
            # If it does exist it will look it up in the dictionary by name and add the neighbor to that vertex.
            current_graph.add_neighbor(market[_BASE], market[_QUOTE], _QUOTE_PRIORITY.get(market[_QUOTE], _STANDARD_WEIGHT))

        # Add alternative markets if they don't exist
        if not self.__exchange_locked:
            self._add_alternative_markets(current_graph, current_markets)

        self._add_fiat_edges_to_graph(current_graph, current_markets)
        self.__logger.debug("Added graph for %s : %s", current_exchange, current_graph)
        self.__exchanges[exchange] = current_exchange
        self.__exchange_markets[exchange] = current_markets
        self.__exchange_graphs[exchange] = current_graph
