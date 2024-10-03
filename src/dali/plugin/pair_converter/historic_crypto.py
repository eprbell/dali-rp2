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

# pylint: disable=not-callable

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from types import MethodType
from typing import Dict, List, Optional

import pandas as pd
import requests
from Historic_Crypto import HistoricalData
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin
from dali.historical_bar import HistoricalBar
from dali.transaction_manifest import TransactionManifest

GRANULARITY: Dict[int, str] = {
    60: "ONE_MINUTE",
    300: "FIVE_MINUTE",
    900: "FIFTEEN_MINUTE",
    1800: "THIRTY_MINUTE",
    3600: "ONE_HOUR",
    7200: "TWO_HOUR",
    21600: "SIX_HOUR",
    86400: "ONE_DAY",
}


class PairConverterPlugin(AbstractPairConverterPlugin):
    def name(self) -> str:
        return "Historic-Crypto"

    def cache_key(self) -> str:
        return self.name()

    def optimize(self, transaction_manifest: TransactionManifest) -> None:
        pass

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
                historical_data_retriever = HistoricalData(f"{from_asset}-{to_asset}", seconds, from_timestamp, to_timestamp, verbose=False)
                # Monkey Patch with new URL and response handling
                historical_data_retriever.retrieve_data = MethodType(PairConverterPlugin.retrieve_data, historical_data_retriever)  # type: ignore
                historical_data = historical_data_retriever.retrieve_data()
                historical_data.index = historical_data.index.tz_localize("UTC")  # The returned timestamps in the index are timezone naive
                historical_data_series = historical_data.reset_index().iloc[0]
                result = HistoricalBar(
                    duration=timedelta(seconds=seconds),
                    timestamp=historical_data_series.start,
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

    # Monkey Patching for the HistoricalData class in the Historic_Crypto module
    @staticmethod
    def retrieve_data(self: HistoricalData) -> pd.DataFrame:  # pylint: disable=bad-staticmethod-argument
        """This function returns the data."""
        if self.verbose:
            print("Formatting Dates.")

        self._ticker_checker()

        start = datetime.strptime(self.start_date, "%Y-%m-%d-%H-%M")
        end = datetime.strptime(self.end_date, "%Y-%m-%d-%H-%M")

        start_date_timestamp = int(datetime.strptime(self.start_date, "%Y-%m-%d-%H-%M").replace(tzinfo=timezone.utc).timestamp())
        end_date_timestamp = int(datetime.strptime(self.end_date, "%Y-%m-%d-%H-%M").replace(tzinfo=timezone.utc).timestamp())

        request_volume = (end_date_timestamp - start_date_timestamp) / self.granularity

        if request_volume <= 300:
            response = requests.get(
                (
                    f"https://api.coinbase.com/api/v3/brokerage/market/products/{self.ticker}/candles?"
                    f"start={start_date_timestamp}&end={end_date_timestamp}&"
                    f"granularity={GRANULARITY[self.granularity]}"
                ),
                timeout=(5, 10),
            )
            if response.status_code in [200, 201, 202, 203, 204]:
                if self.verbose:
                    print("Retrieved Data from Coinbase Pro API.")
                data = pd.DataFrame(json.loads(response.text).get("candles", []))
                data.columns = ["start", "low", "high", "open", "close", "volume"]
                data["start"] = pd.to_numeric(data["start"])
                data["start"] = pd.to_datetime(data["start"], unit="s")

                data = data[data["start"].between(start, end)]
                print(f"Data between Start and End: {data}")
                data.set_index("start", drop=True, inplace=True)
                data.sort_index(ascending=True, inplace=True)
                data.drop_duplicates(subset=None, keep="first", inplace=True)
                if self.verbose:
                    print("Returning data.")
                return data
            if response.status_code in [400, 401, 404]:
                if self.verbose:
                    print(f"Status Code: {response.status_code}, malformed request to the CoinBase Pro API.")
                sys.exit()
            elif response.status_code in [403, 500, 501]:
                if self.verbose:
                    print(f"Status Code: {response.status_code}, could not connect to the CoinBase Pro API.")
                sys.exit()
            else:
                if self.verbose:
                    print(f"Status Code: {response.status_code}, error in connecting to the CoinBase Pro API.")
                sys.exit()
        else:
            # The api limit:
            max_per_mssg = 300
            data = pd.DataFrame()
            for i in range(int(request_volume / max_per_mssg) + 1):
                provisional_start = start + timedelta(0, i * (self.granularity * max_per_mssg))
                provisional_start_timestamp = int(datetime.strptime(self._date_cleaner(provisional_start), "%Y-%m-%dT%H:%M:%S").timestamp())
                provisional_end = start + timedelta(0, (i + 1) * (self.granularity * max_per_mssg))
                provisional_end_timestamp = int(datetime.strptime(self._date_cleaner(provisional_end), "%Y-%m-%dT%H:%M:%S").timestamp())

                print("Provisional Start: {provisional_start}")
                print("Provisional End: {provisional_end}")
                response = requests.get(
                    (
                        f"https://api.coinbase.com/api/v3/brokerage/market/products/{self.ticker}/candles?"
                        f"start={provisional_start_timestamp}&end={provisional_end_timestamp}&"
                        f"granularity={GRANULARITY[self.granularity]}"
                    ),
                    timeout=(5, 10),
                )

                if response.status_code in [200, 201, 202, 203, 204]:
                    if self.verbose:
                        print(f"Data for chunk {i+1} of {(int(request_volume / max_per_mssg) + 1)} extracted")
                    dataset = pd.DataFrame(json.loads(response.text).get("candles", []))
                    if not dataset.empty:
                        data = data.append(dataset)
                        time.sleep(1)
                    else:
                        print(
                            f"CoinBase Pro API did not have available data for '{self.ticker}' beginning at {self.start_date}."
                            f"Trying a later date:'{provisional_start}'"
                        )
                        time.sleep(1)
                elif response.status_code in [400, 401, 404]:
                    if self.verbose:
                        print(f"Status Code: {response.status_code}, malformed request to the CoinBase Pro API.")
                    sys.exit()
                elif response.status_code in [403, 500, 501]:
                    if self.verbose:
                        print(f"Status Code: {response.status_code}, could not connect to the CoinBase Pro API.")
                    sys.exit()
                else:
                    if self.verbose:
                        print(f"Status Code: {response.status_code}, error in connecting to the CoinBase Pro API.")
                    sys.exit()
            data.columns = ["start", "low", "high", "open", "close", "volume"]
            data["start"] = pd.to_numeric(data["start"])
            data["start"] = pd.to_datetime(data["start"], unit="s")
            data = data[data["start"].between(start, end)]
            data.set_index("start", drop=True, inplace=True)
            data.sort_index(ascending=True, inplace=True)
            data.drop_duplicates(subset=None, keep="first", inplace=True)
            return data
