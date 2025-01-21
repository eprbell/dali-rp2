# Copyright 2024 orientalperil. Neal Chambers
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
from typing import Dict, Optional

from coinbase.rest import RESTClient
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin
from dali.historical_bar import HistoricalBar
from dali.logger import LOGGER
from dali.transaction_manifest import TransactionManifest

TIME_GRANULARITY: Dict[str, int] = {
    "ONE_MINUTE": 60,
    "FIVE_MINUTE": 300,
    "FIFTEEN_MINUTE": 900,
    "THIRTY_MINUTE": 1800,
    "ONE_HOUR": 3600,
    "TWO_HOUR": 7200,
    "SIX_HOUR": 21600,
    "ONE_DAY": 86400,
}


class PairConverterPlugin(AbstractPairConverterPlugin):
    def __init__(self, historical_price_type: str, api_key: Optional[str] = None, api_secret: Optional[str] = None) -> None:
        super().__init__(historical_price_type)
        self._authorized: bool = False
        if api_key is not None and api_secret is not None:
            self.client = RESTClient(api_key=api_key, api_secret=api_secret)
            self._authorized = True
        else:
            self.client = RESTClient()
            LOGGER.info(
                "API key and API secret were not provided for the Coinbase Advanced Pair Converter Plugin. "
                "Requests will be throttled. For faster price resolution, please provide a valid "
                "API key and secret in the Dali-rp2 configuration file."
            )

    def name(self) -> str:
        return "dali_dali_coinbase"

    def cache_key(self) -> str:
        return self.name()

    def optimize(self, transaction_manifest: TransactionManifest) -> None:
        pass

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        result: Optional[HistoricalBar] = None
        utc_timestamp = timestamp.astimezone(timezone.utc)
        start = utc_timestamp.replace(second=0)
        end = start
        retry_count: int = 0

        while retry_count < len(TIME_GRANULARITY):
            try:
                granularity = list(TIME_GRANULARITY.keys())[retry_count]
                if self._authorized:
                    candle = self.client.get_candles(f"{from_asset}-{to_asset}", str(start.timestamp()), str(end.timestamp()), granularity).to_dict()[
                        "candles"
                    ][0]
                else:
                    candle = self.client.get_public_candles(f"{from_asset}-{to_asset}", str(start.timestamp()), str(end.timestamp()), granularity).to_dict()[
                        "candles"
                    ][0]
                candle_start = datetime.fromtimestamp(int(candle["start"]), timezone.utc)
                result = HistoricalBar(
                    duration=timedelta(seconds=TIME_GRANULARITY[granularity]),
                    timestamp=candle_start,
                    open=RP2Decimal(candle["open"]),
                    high=RP2Decimal(candle["high"]),
                    low=RP2Decimal(candle["low"]),
                    close=RP2Decimal(candle["close"]),
                    volume=RP2Decimal(candle["volume"]),
                )
            except ValueError:
                retry_count += 1

        return result
