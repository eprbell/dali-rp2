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

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from Historic_Crypto import HistoricalData
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin
from dali.historical_bar import HistoricalBar


class PairConverterPlugin(AbstractPairConverterPlugin):
    def name(self) -> str:
        return "Historic-Crypto"

    def cache_key(self) -> str:
        return self.name()

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        result: Optional[HistoricalBar] = None
        time_granularity: List[int] = [60, 300, 900, 3600, 21600, 86400]
        # Coinbase API expects UTC timestamps only, see the forum discussion here:
        # https://forums.coinbasecloud.dev/t/invalid-end-on-product-candles-endpoint/320
        utc_timestamp = timestamp.astimezone(timezone.utc)
        from_timestamp: str = utc_timestamp.strftime("%Y-%m-%d-%H-%M")
        retry_count: int = 0

        while retry_count < len(time_granularity):
            try:
                seconds = time_granularity[retry_count]
                to_timestamp: str = (utc_timestamp + timedelta(seconds=seconds)).strftime("%Y-%m-%d-%H-%M")
                historical_data = HistoricalData(f"{from_asset}-{to_asset}", seconds, from_timestamp, to_timestamp, verbose=False).retrieve_data()
                historical_data.index = historical_data.index.tz_localize("UTC")  # The returned timestamps in the index are timezone naive
                historical_data_series = historical_data.reset_index().iloc[0]
                result = HistoricalBar(
                    duration=timedelta(seconds=seconds),
                    timestamp=historical_data_series.time,
                    open=RP2Decimal(str(historical_data_series.open)),
                    high=RP2Decimal(str(historical_data_series.high)),
                    low=RP2Decimal(str(historical_data_series.low)),
                    close=RP2Decimal(str(historical_data_series.close)),
                    volume=RP2Decimal(str(historical_data_series.volume)),
                )
                break
            except ValueError:
                retry_count += 1

        return result
