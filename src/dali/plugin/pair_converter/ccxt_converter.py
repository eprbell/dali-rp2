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

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, NamedTuple, Optional, Set, Union

from ccxt import binance
from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin
from dali.historical_bar import HistoricalBar

# Native format keywords
_ID: str = "id"

# Time in ms
_MINUTE: str = "1m"
_FIVEMINUTE: str = "5m"
_FIFTEENMINUTE: str = "15m"
_ONEHOUR: str = "1h"
_FOURHOUR: str = "4h"
_ONEDAY: str = "1d"
_TIMEGRANULARITY: List[str] = [_MINUTE, _FIVEMINUTE, _FIFTEENMINUTE, _ONEHOUR, _FOURHOUR, _ONEDAY]
_TIMEGRANULARITY_IN_S: List[int] = [60, 300, 900, 3600, 14400, 86400]

# Currently supported exchanges
_BINANCE: str = "Binance.com"
_EXCHANGEDICT: Dict[str, Any] = {_BINANCE: binance}

# Time constants
_MS_IN_SECOND: int = 1000


class StableCoin(NamedTuple):
    symbol: str
    fiat_code: str


# Default Stable coin for each exchange
# This should be the stable coin with the most volume on the exchange
_DEFAULTSTABLEDICT = {
    _BINANCE: StableCoin(symbol="USDT", fiat_code="USD"),
}


class PairConverterPlugin(AbstractPairConverterPlugin):

    # pricing_alt allows for the user to config specific markets to be used to price an asset that doesn't have
    # a stable coin or fiat market
    def __init__(self, historical_price_type: str, pricing_alt: Optional[str] = None) -> None:
        super().__init__(historical_price_type=historical_price_type)
        self.__logger: logging.Logger = create_logger(f"CCXT converter using {historical_price_type}")
        self.pricing_alt: Dict[str, str] = {}
        if pricing_alt is not None:
            self.pricing_alt = json.loads(pricing_alt)

        self.exchanges: Dict[str, Any] = {}
        self.exchange_markets: Dict[str, List[str]] = {}
        self.exchange_quote_assets: Dict[str, Set[str]] = {}

    def name(self) -> str:
        return "CCXT-converter"

    def cache_key(self) -> str:
        return self.name()

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:

        # If both assets are fiat, skip further processing
        if from_asset in self.fiat_list and to_asset in self.fiat_list:
            return self.get_fiat_exchange_rate(timestamp, from_asset, to_asset)

        # Caching of exchanges
        if exchange not in self.exchanges:
            if exchange in _EXCHANGEDICT:
                current_exchange: Any = _EXCHANGEDICT[exchange]()
                current_markets: List[str] = []

                for market in current_exchange.fetch_markets():
                    self.__logger.debug("Market: %s", market)
                    current_markets.append(market[_ID])
                self.exchanges[exchange] = current_exchange
                self.exchange_markets[exchange] = current_markets
            else:
                self.__logger.error("WARNING: Unrecognized Exchange: %s. Please open an issue at %s", exchange, self.ISSUES_URL)
                return None
        else:
            current_exchange = self.exchanges[exchange]
            current_markets = self.exchange_markets[exchange]

        ms_timestamp: int = int(timestamp.timestamp() * _MS_IN_SECOND)
        historical_data: List[List[Union[float, int]]] = []
        bridge_asset: Optional[str] = None
        bridge_fiat: Optional[str] = None

        market_symbol = from_asset + to_asset

        # Use the default stable coin if the fiat market doesn't exist
        # If stable coin market doesn't exist find alt trading pair
        if market_symbol not in current_markets:
            if _DEFAULTSTABLEDICT[exchange].fiat_code == to_asset:
                to_asset = _DEFAULTSTABLEDICT[exchange].symbol
                market_symbol = from_asset + to_asset
            elif to_asset in self.fiat_list:
                bridge_fiat = _DEFAULTSTABLEDICT[exchange].symbol
                market_symbol = from_asset + bridge_fiat

        result: Optional[HistoricalBar] = None
        retry_count: int = 0

        # If the asset is not priced in the exchange's default stable coin, alternative pricing must be used.
        if market_symbol not in current_markets:

            if from_asset in self.pricing_alt:
                market_symbol = self.pricing_alt[from_asset]

                # Retrieve the intermediate to_asset from the market given in the ini file.
                bridge_asset = market_symbol[len(from_asset) :]

            else:
                # Search for any market that has from_asset as a base asset
                regex = re.compile(f"{from_asset}.*")
                pricing_alt_markets: List[str] = list(filter(regex.match, current_markets))
                if pricing_alt_markets:
                    bridge_asset = pricing_alt_markets[0]

                # No alternative market was found.
                # This means the asset is on the user's wallet on the exchange, but not traded on the exchange
                # one possible cause is that the asset was an airdrop that is not currently being traded
                # on this exchange.
                # Maybe we can pull the price from another exchange?
                else:
                    return None

            # Check for the bridge fiat if one exists, otherwise use the to_asset
            market_symbol = bridge_asset + (to_asset if bridge_fiat is None else bridge_fiat)
            if market_symbol not in current_markets:
                if bridge_fiat is not None:
                    bridge_fiat = _DEFAULTSTABLEDICT[exchange].symbol
                    market_symbol = bridge_asset + bridge_fiat
                else:
                    to_asset = _DEFAULTSTABLEDICT[exchange].symbol
                    market_symbol = bridge_asset + to_asset

                if market_symbol not in current_markets:
                    self.__logger.error("Internal Error: Too many pricing alternatives for asset: %s. Please open an issue at %s", from_asset, self.ISSUES_URL)

            # Alt pricing
            while retry_count < len(_TIMEGRANULARITY):

                timeframe = _TIMEGRANULARITY[retry_count]
                bridge_data: List[List[Union[float, int]]] = current_exchange.fetchOHLCV(f"{from_asset}/{bridge_asset}", timeframe, ms_timestamp, 1)
                # [
                #     [
                #         1504541580000, // UTC timestamp in milliseconds, integer
                #         4235.4,        // (O)pen price, float
                #         4240.6,        // (H)ighest price, float
                #         4230.0,        // (L)owest price, float
                #         4230.7,        // (C)losing price, float
                #         37.72941911    // (V)olume (in terms of the base currency), float
                #     ],
                #     ...
                # ]

                if bridge_data:
                    historical_data = current_exchange.fetchOHLCV(
                        f"{bridge_asset}/{(to_asset if bridge_fiat is None else bridge_fiat)}", timeframe, ms_timestamp, 1
                    )
                    if historical_data:
                        result = HistoricalBar(
                            duration=timedelta(seconds=_TIMEGRANULARITY_IN_S[retry_count]),
                            timestamp=datetime.fromtimestamp(ms_timestamp / _MS_IN_SECOND),
                            open=RP2Decimal(str(historical_data[0][1])) * RP2Decimal(str(bridge_data[0][1])),
                            high=RP2Decimal(str(historical_data[0][2])) * RP2Decimal(str(bridge_data[0][2])),
                            low=RP2Decimal(str(historical_data[0][3])) * RP2Decimal(str(bridge_data[0][3])),
                            close=RP2Decimal(str(historical_data[0][4])) * RP2Decimal(str(bridge_data[0][4])),
                            volume=RP2Decimal(str(bridge_data[0][5])),
                        )
                        break

                retry_count += 1

        # standard pricing
        else:

            while retry_count < len(_TIMEGRANULARITY):

                timeframe = _TIMEGRANULARITY[retry_count]
                historical_data = current_exchange.fetchOHLCV(f"{from_asset}/{(to_asset if bridge_fiat is None else bridge_fiat)}", timeframe, ms_timestamp, 1)
                # If there is no candle the list will be empty
                if historical_data:
                    result = HistoricalBar(
                        duration=timedelta(seconds=_TIMEGRANULARITY_IN_S[retry_count]),
                        timestamp=datetime.fromtimestamp(ms_timestamp / _MS_IN_SECOND),
                        open=RP2Decimal(str(historical_data[0][1])),
                        high=RP2Decimal(str(historical_data[0][2])),
                        low=RP2Decimal(str(historical_data[0][3])),
                        close=RP2Decimal(str(historical_data[0][4])),
                        volume=RP2Decimal(str(historical_data[0][5])),
                    )
                    break

                retry_count += 1

        if bridge_fiat is not None:
            fiat_exchange_rate: Optional[HistoricalBar] = self.get_fiat_exchange_rate(timestamp, bridge_fiat, to_asset)
            if fiat_exchange_rate is not None:
                result = HistoricalBar(
                    duration=result.duration,  # type: ignore
                    timestamp=result.timestamp,  # type: ignore
                    open=result.open * fiat_exchange_rate.open,  # type: ignore
                    high=result.high * fiat_exchange_rate.high,  # type: ignore
                    low=result.low * fiat_exchange_rate.low,  # type: ignore
                    close=result.close * fiat_exchange_rate.close,  # type: ignore
                    volume=result.volume,  # type: ignore
                )

        return result
