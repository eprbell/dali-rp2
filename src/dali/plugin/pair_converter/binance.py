# Copyright 2022 mbianco
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
from typing import List, Optional

from rp2.rp2_decimal import RP2Decimal

from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin
from dali.historical_bar import HistoricalBar

import requests
import json

class PairConverterPlugin(AbstractPairConverterPlugin):
    def name(self) -> str:
        return "Binance.com"

    def cache_key(self) -> str:
        return self.name()

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        """
        Example API data structure:
        [
            [
                1499040000000,      // Open time
                "0.01634790",       // Open
                "0.80000000",       // High
                "0.01575800",       // Low
                "0.01577100",       // Close
                "148976.11427815",  // Volume
                1499644799999,      // Close time
                "2434.19055334",    // Quote asset volume
                308,                // Number of trades
                "1756.87402397",    // Taker buy base asset volume
                "28.46694368",      // Taker buy quote asset volume
                "17928899.62484339" // Ignore.
            ]
        ]
        """

        # binance has more tokens in USDT, which should be equal to the USD price
        if to_asset == "USD":
            to_asset = "USDT"

        symbol = f"{from_asset}{to_asset}"
        twelve_hours = 60 * 60 * 12
        utc_timestamp = int(timestamp.astimezone(timezone.utc).timestamp())

        response = requests.get(f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=12h&startTime={utc_timestamp}&limit=1")
        response_json = json.loads(response.text)

        return HistoricalBar(
            duration=timedelta(seconds=twelve_hours),
            timestamp=response_json[0][0],
            open=RP2Decimal(str(response_json[0][1])),
            high=RP2Decimal(str(response_json[0][2])),
            low=RP2Decimal(str(response_json[0][3])),
            close=RP2Decimal(str(response_json[0][4])),
            volume=RP2Decimal(str(response_json[0][5])),
        )





