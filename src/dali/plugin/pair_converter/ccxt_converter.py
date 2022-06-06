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

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, NamedTuple, Optional

from rp2.rp2_decimal import RP2Decimal

from ccxt import binance, coinbase, coinbasepro
from rp2.logger import create_logger

from dali.historical_bar import HistoricalBar
from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin

# Native format keywords
_ID: str = "id"

# Time in ms
_MINUTE: str = "1m"
_FIVEMINUTE: str  = "5m"
_FIFTEENMINUTE: str = "15m"
_ONEHOUR: str = "1h"
_FOURHOUR: str = "4h"
_ONEDAY: str = "1d"
_TIMEGRANULARITY: List[int] = [_MINUTE, _FIVEMINUTE, _FIFTEENMINUTE, _ONEHOUR, _FOURHOUR, _ONEDAY]
_TIMEGRANULARITY_IN_S: List[int] =[60, 300, 900, 3600, 14400, 86400]

# Currently supported exchanges
_BINANCE: str = "Binance.com" 
_EXCHANGEDICT: Dict[str, Any] = {_BINANCE:binance}

# Time constants
_MS_IN_SECOND: int = 1000

class StableCoin(NamedTuple):
    symbol: str
    fiat_code: str

# Default Stable coins
# This should be the stable coin with the most volume on the exchange
_DEFAULTSTABLEDICT = {
    _BINANCE:StableCoin(symbol="USDT", fiat_code="USD"),
} 

class PairConverterPlugin(AbstractPairConverterPlugin):

    # pylint: disable=no-self-use
    def __init__(self, historical_price_type: str) -> None:
        super().__init__(historical_price_type=historical_price_type)
        self.__logger: logging.Logger = create_logger(f"CCXT converter using {historical_price_type}")
        self.exchanges: Dict[Any] = {}
        self.exchange_markets: Dict[str, List[str]] = {}

    def name(self) -> str:
        return "CCXT-converter"

    def cache_key(self) -> str:
        return self.name()

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:

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

        result: Optional[HistoricalBar] = None

        ms_timestamp: int = int(timestamp.timestamp() * _MS_IN_SECOND) 
        retry_count: int = 0
        historical_data: List[float,int] = []

        market_symbol: str = from_asset + to_asset
        stable_used: bool = False

        # Use the default stable coin if the fiat market doesn't exist
        if market_symbol not in current_markets:
            to_asset: str = _DEFAULTSTABLEDICT[exchange].symbol
            stable_used = True

        while retry_count < len(_TIMEGRANULARITY):

                timeframe = _TIMEGRANULARITY[retry_count]
                historical_data: HistoricalBar = current_exchange.fetchOHLCV(f"{from_asset}/{to_asset}", timeframe, ms_timestamp, 1)
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

                # If there is no candle the list will be empty
                if historical_data: 
                    result = HistoricalBar(
                        duration=_TIMEGRANULARITY_IN_S[retry_count],
                        timestamp=datetime.fromtimestamp(ms_timestamp / _MS_IN_SECOND),
                        open=RP2Decimal(str(historical_data[0][1])),
                        high=RP2Decimal(str(historical_data[0][2])),
                        low=RP2Decimal(str(historical_data[0][3])),
                        close=RP2Decimal(str(historical_data[0][4])),
                        volume=RP2Decimal(str(historical_data[0][5])),
                    )
                    break
                else:
                    retry_count += 1

        return result    
