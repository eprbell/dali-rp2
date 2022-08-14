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
from typing import NamedTuple, cast

from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2ValueError

from dali.configuration import Keyword


class HistoricalBar(NamedTuple):
    """A single bar of historical market data."""

    duration: timedelta
    timestamp: datetime
    open: RP2Decimal
    high: RP2Decimal
    low: RP2Decimal
    close: RP2Decimal
    volume: RP2Decimal

    def derive_transaction_price(self, transaction_timestamp: datetime, historical_price_type: str) -> RP2Decimal:
        """Derive a transaction price from a historical bar."""
        price: RP2Decimal = cast(RP2Decimal, None)
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
            raise RP2ValueError(f"Unrecognized historical_price_type '{historical_price_type}'")
        return price
