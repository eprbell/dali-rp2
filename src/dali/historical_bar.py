# Copyright 2022 Steve Davis
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


from datetime import datetime, timedelta
from typing import NamedTuple, Type, TypeVar, cast

import pandas as pd

from dali.dali_configuration import Keyword

T = TypeVar("T", bound="HistoricalBar")


class HistoricalBar(NamedTuple):
    """A single bar of historical market data."""

    duration: timedelta
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def from_historic_crypto_series(cls: Type[T], duration: timedelta, historic_crypto_series: pd.Series) -> T:
        """Initialize with a row of a dataframe returned by the Historic_Crypto package, which will be a series."""
        # Note: series attributes are not known until runtime, hence the type ignores.
        # Note: prices from the Historic_Crypto API are of type numpy.float64.
        return cls(
            duration=duration,
            timestamp=historic_crypto_series.time,  # type: ignore
            open=historic_crypto_series.open,  # type: ignore
            high=historic_crypto_series.high,  # type: ignore
            low=historic_crypto_series.low,  # type: ignore
            close=historic_crypto_series.close,  # type: ignore
            volume=historic_crypto_series.volume,  # type: ignore
        )

    @classmethod
    def from_historic_crypto_dataframe(cls: Type[T], duration: timedelta, df_historic_crypto: pd.DataFrame) -> T:
        """Initialize with only the 1st row of the dataframe returned by the Historic_Crypto package."""
        df_historic_crypto.index = df_historic_crypto.index.tz_localize("UTC")  # The returned timestamps in the index are timezone naive
        historic_crypto_series: pd.Series = df_historic_crypto.reset_index().iloc[0]
        return cls.from_historic_crypto_series(duration, historic_crypto_series)

    def derive_transaction_price(self, transaction_timestamp: datetime, historical_price_type: str) -> float:
        """Derive a transaction price from a historical bar."""
        price: float = cast(float, None)
        if historical_price_type == Keyword.HISTORICAL_PRICE_OPEN.value:
            price = self.open
        elif historical_price_type == Keyword.HISTORICAL_PRICE_HIGH.value:
            price = self.high
        elif historical_price_type == Keyword.HISTORICAL_PRICE_LOW.value:
            price = self.low
        elif historical_price_type == Keyword.HISTORICAL_PRICE_CLOSE.value:
            price = self.close
        elif historical_price_type == Keyword.HISTORICAL_PRICE_NEAREST.value:
            bar_start_timestamp = self.timestamp
            bar_end_timestamp = self.timestamp + self.duration
            start_timedelta = abs(transaction_timestamp - bar_start_timestamp)
            end_timedelta = abs(transaction_timestamp - bar_end_timestamp)
            price = self.open if start_timedelta < end_timedelta else self.close
        else:
            raise ValueError(f"Unrecognized historical_price_type '{historical_price_type}'")
        return price
