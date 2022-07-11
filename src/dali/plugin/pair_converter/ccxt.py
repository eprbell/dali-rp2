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

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, NamedTuple, Optional

from ccxt import binance, kraken, liquid
from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin
from dali.bfs import BFS
from dali.historical_bar import HistoricalBar

# Native format keywords
_ID: str = "id"
_BASE: str =  "base"
_QUOTE: str = "quote"

# Time in ms
_MINUTE: str = "1m"
_FIVE_MINUTE: str = "5m"
_FIFTEEN_MINUTE: str = "15m"
_ONE_HOUR: str = "1h"
_FOUR_HOUR: str = "4h"
_ONE_DAY: str = "1d"
_TIMEGRANULARITY: List[str] = [_MINUTE, _FIVE_MINUTE, _FIFTEEN_MINUTE, _ONE_HOUR, _FOUR_HOUR, _ONE_DAY]
_TIMEGRANULARITY_IN_SECONDS: List[int] = [60, 300, 900, 3600, 14400, 86400]

# Currently supported exchanges
_BINANCE: str = "Binance.com"
_KRAKEN: str = "Kraken"
_LIQUID: str = "Liquid"
_FIAT_EXCHANGE: str = "Exchangerate.host"
_EXCHANGEDICT: Dict[str, Any] = {_BINANCE: binance, _KRAKEN: kraken, _LIQUID: liquid}

# Alternative Markets and exchanges for stablecoins or untradeable assets
_ALTMARKET_EXCHANGESDICT: Dict[str, str] = {"USDTUSD":_KRAKEN, "SOLOXRP":_LIQUID}
_ALTMARKET_BY_BASEDICT: Dict[str, str] = {"USDT":"USD","SOLO":"XRP"}

# Time constants
_MS_IN_SECOND: int = 1000


class StableCoin(NamedTuple):
    symbol: str
    fiat_code: str


class AssetPairAndTimestamp(NamedTuple):
    timestamp: datetime
    from_asset: str
    to_asset: str
    exchange: str


class AssetPairAndHistoricalPrice(NamedTuple):
    from_asset: str
    to_asset: str
    exchange: str
    historical_data: Optional[HistoricalBar] = None


# Default Stable coin for each exchange
# This should be the stable coin with the most volume on the exchange
_DEFAULTSTABLEDICT = {
    _BINANCE: StableCoin(symbol="USDT", fiat_code="USD"),
}


class PairConverterPlugin(AbstractPairConverterPlugin):

    # pricing_alt allows for the user to config specific markets to be used to price an asset that doesn't have
    # a stable coin or fiat market
    def __init__(self, historical_price_type: str) -> None:
        super().__init__(historical_price_type=historical_price_type)
        self.__logger: logging.Logger = create_logger(f"{self.name()}/{historical_price_type}")

        self.__exchanges: Dict[str, Any] = {}
        self.__exchange_markets: Dict[str, Dict[str, List[str]]] = {}
        self.__exchange_graphs: Dict[str, Dict[str, List[str]]] = {}

    def name(self) -> str:
        return "CCXT-converter"

    def cache_key(self) -> str:
        return self.name()

    @property
    def exchanges(self):
        return self.__exchanges

    @property
    def exchange_markets(self):
        return self.__exchange_markets

    @property
    def exchange_graphs(self):
        return self.__exchange_graphs


    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        self.__logger.debug("Converting %s to %s", from_asset, to_asset)

        # If both assets are fiat, skip further processing
        if self.this_is_fiat_pair(from_asset, to_asset):
            return self.get_fiat_exchange_rate(timestamp, from_asset, to_asset)

        # Caching of exchanges
        if exchange not in self.__exchanges:
            if exchange in _EXCHANGEDICT:
                current_exchange: Any = _EXCHANGEDICT[exchange]()
                # key: market, value: exchanges where the market is available in order of priority
                current_markets: Dict[str, List[str]] = {}
                current_graph: Dict[str, List[str]] = {}

                for market in current_exchange.fetch_markets():
                    self.__logger.debug("Market: %s", market)
                    current_markets[market[_ID]] = [exchange]

                    # TO BE IMPLEMENTED - lazy build graph only if needed

                    # Add the quote asset to the graph if it isn't there already.
                    if current_graph.get(market[_BASE]) and (market[_QUOTE] not in current_graph[market[_BASE]]):
                        current_graph[market[_BASE]].append(market[_QUOTE])
                    else:
                        current_graph[market[_BASE]] = [market[_QUOTE]]

                # TO BE IMPLEMENTED - possibly sort the lists to put the main stable coin first.

                # Add alternative markets if they don't exist
                for base_asset, quote_asset in _ALTMARKET_BY_BASEDICT.items():
                    alt_market = base_asset + quote_asset
                    current_markets[alt_market] = [_ALTMARKET_EXCHANGESDICT[alt_market]]

                    if current_graph.get(base_asset) and (quote_asset not in current_graph[base_asset]):
                        current_graph[base_asset].append(quote_asset)
                    else:
                        current_graph[base_asset] = [quote_asset]

                self.add_fiat_graph_to(current_graph, current_markets)
                self.__logger.debug("Added graph for %s : %s", current_exchange, current_graph)
                self.__exchanges[exchange] = current_exchange
                self.__exchange_markets[exchange] = current_markets
                self.__exchange_graphs[exchange] = current_graph

            else:
                self.__logger.error("WARNING: Unrecognized Exchange: %s. Please open an issue at %s", exchange, self.ISSUES_URL)
                return None
        else:
            current_exchange = self.__exchanges[exchange]
            current_markets = self.__exchange_markets[exchange]
            current_graph = self.__exchange_graphs[exchange]


        market_symbol =  from_asset + to_asset
        result: Optional[HistoricalBar] = None

        # TO BE IMPLEMENTED - bypass routing if conversion can be done with one market on the exchange
        if market_symbol in current_markets and (exchange in current_markets[market_symbol]):
            result = self.find_historical_bar(from_asset, to_asset, timestamp, exchange)
            return result
        # else:
            # Graph building goes here.

        pricing_path: Optional[List[str]] = BFS.bfs_cyclic(current_graph, from_asset, to_asset)
        if pricing_path is None:
            self.__logger.debug("No path found for %s to %s. Please open an issue at %s.",
                from_asset, to_asset, self.ISSUES_URL)
            return None

        self.__logger.debug("Found path - %s", pricing_path)

        conversion_route: List[AssetPairAndHistoricalPrice] = []
        last_node: Optional[str] = None
        hop_bar: Optional[HistoricalBar] = None

        # Build conversion stack, we will iterate over this to find the price for each conversion
        # Then multiply them together to get our final price.
        for node in pricing_path:

            if last_node:
                conversion_route.append(AssetPairAndHistoricalPrice(
                    from_asset=last_node,
                    to_asset=node,
                    exchange=current_markets[(last_node + node)][0],
                    historical_data=None,
                ))

            last_node = node

        for i, hop_data in enumerate(conversion_route):
            if self.this_is_fiat_pair(hop_data.from_asset, hop_data.to_asset):
                hop_bar = self.get_fiat_exchange_rate(timestamp, hop_data.from_asset, hop_data.to_asset)
            else:
                hop_bar = self.find_historical_bar(hop_data.from_asset, hop_data.to_asset, timestamp, hop_data.exchange)

            if hop_bar is not None:
                # Replacing an immutable attribute
                conversion_route[i] = conversion_route[i]._replace(historical_data=hop_bar)
            else:
                self.__logger.error("Internal Error: Market not found for hop.")

            if result is not None:
                # TO BE IMPLEMENTED - override Historical Bar * to multiply two bars?
                result = HistoricalBar(
                    duration=max(result.duration, hop_bar.duration), # type: ignore
                    timestamp=timestamp,
                    open=(result.open * hop_bar.open),      # type: ignore
                    high=(result.high * hop_bar.high),      # type: ignore
                    low=(result.low * hop_bar.low),         # type: ignore
                    close=(result.close * hop_bar.close),   # type: ignore
                    volume=(result.volume + hop_bar.volume),# type: ignore
                )
            else:
                result = hop_bar

        return result

    def find_historical_bar(self, from_asset: str, to_asset: str, timestamp: datetime, exchange: str) -> Optional[HistoricalBar]:
        result: Optional[HistoricalBar] = None
        retry_count: int = 0
        current_exchange: Any = self.__exchanges[exchange]
        ms_timestamp: int = int(timestamp.timestamp() * _MS_IN_SECOND)

        while retry_count < len(_TIMEGRANULARITY):

            timeframe = _TIMEGRANULARITY[retry_count]
            historical_data = current_exchange.fetchOHLCV(f"{from_asset}/{to_asset}", timeframe, ms_timestamp, 1)
            # If there is no candle the list will be empty
            if historical_data:
                result = HistoricalBar(
                    duration=timedelta(seconds=_TIMEGRANULARITY_IN_SECONDS[retry_count]),
                    timestamp=timestamp,
                    open=RP2Decimal(str(historical_data[0][1])),
                    high=RP2Decimal(str(historical_data[0][2])),
                    low=RP2Decimal(str(historical_data[0][3])),
                    close=RP2Decimal(str(historical_data[0][4])),
                    volume=RP2Decimal(str(historical_data[0][5])),
                )
                break

            retry_count += 1

        return result
