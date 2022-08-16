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
    # def __init__(self, historical_price_type: str, fiat_priority: Optional[str] = None) -> None:
    #     super().__init__(historical_price_type=historical_price_type, fiat_priority=fiat_priority)

    def name(self) -> str:
        return "Manual-Crypto"

    def cache_key(self) -> str:
        return self.name()

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        time_granularity: List[int] = [60, 300, 900, 3600, 21600, 86400]
        utc_timestamp = timestamp.astimezone(timezone.utc)
        from_timestamp: str = utc_timestamp.strftime("%Y-%m-%d-%H-%M")

        asset_key = f"{from_asset}-{to_asset}"

        # TODO this data would be pulled from a ini/json file. What's missing here is time ranges.
        #      we should probably support time ranges in the manual data structure. Maybe it makes sense
        #      to have a CSV file for each asset pair with the time ranges?
        manual_data = {"EOP-USD": "0.00005", "EON-USD": "0.35", "BCHABC-USD": "0.01", "GUSD-USD": "1"}

        # TODO we probably shouldn't use a default price, and it probably makes sense just to hard fail
        price = "1.0"

        if asset_key not in manual_data:
            # TODO should probably throw an exception and hard fail here
            self.logger.error(f"No manual data for {asset_key}")
        else:
            price = manual_data[asset_key]

        return HistoricalBar(
            # day-long time range
            duration=timedelta(seconds=86400),
            timestamp=timestamp,
            # TODO same price is used for all price dimensions for a specific day, not great, but probably fine for this use case?
            open=RP2Decimal(price),
            high=RP2Decimal(price),
            low=RP2Decimal(price),
            close=RP2Decimal(price),
            volume=RP2Decimal(price),
        )
