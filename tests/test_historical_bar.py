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


from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import pytest
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2ValueError

from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar

# Test values
BAR_SECONDS: int = 60
BAR_START_TIMESTAMP: datetime = datetime(2022, 1, 1, hour=13, minute=30).replace(tzinfo=timezone.utc)
BAR_OPEN: RP2Decimal = RP2Decimal("1.0")
BAR_HIGH: RP2Decimal = RP2Decimal("3.0")
BAR_LOW: RP2Decimal = RP2Decimal("0.5")
BAR_CLOSE: RP2Decimal = RP2Decimal("2.0")
DONT_CARE_TIMESTAMP = datetime(2022, 2, 2, hour=2, minute=2).replace(tzinfo=timezone.utc)


@pytest.fixture  # type: ignore
def bar() -> HistoricalBar:  # pylint: disable=C0104, W0621
    """Create a HistoricalBar to test with."""
    # Construct a hardcoded dataframe matching the type returned by Historic_Crypto.HistoricalData.
    return HistoricalBar(
        duration=timedelta(seconds=BAR_SECONDS),
        timestamp=BAR_START_TIMESTAMP,
        open=BAR_OPEN,
        high=BAR_HIGH,
        low=BAR_LOW,
        close=BAR_CLOSE,
        volume=ZERO,
    )


def test_historical_bar_is_frozen(bar: HistoricalBar) -> None:  # pylint: disable=C0104, W0621
    """Verify HistoricalBar data elements cannot be changed."""
    # These tests will pass for either NamedTuple rasing AttributeError or frozen dataclass raising FrozenInstanceError
    with pytest.raises((AttributeError, FrozenInstanceError)):
        bar.duration = None  # type: ignore
    with pytest.raises((AttributeError, FrozenInstanceError)):
        bar.timestamp = None  # type: ignore
    with pytest.raises((AttributeError, FrozenInstanceError)):
        bar.open = None  # type: ignore
    with pytest.raises((AttributeError, FrozenInstanceError)):
        bar.high = None  # type: ignore
    with pytest.raises((AttributeError, FrozenInstanceError)):
        bar.low = None  # type: ignore
    with pytest.raises((AttributeError, FrozenInstanceError)):
        bar.close = None  # type: ignore
    with pytest.raises((AttributeError, FrozenInstanceError)):
        bar.volume = None  # type: ignore


def test_derive_transaction_price_invalid(bar: HistoricalBar) -> None:  # pylint: disable=C0104, W0621
    """Verify invalid price selection parameter raises an exception."""
    with pytest.raises(RP2ValueError):
        bar.derive_transaction_price(DONT_CARE_TIMESTAMP, "Invalid")


def test_derive_transaction_price_open(bar: HistoricalBar) -> None:  # pylint: disable=C0104, W0621
    assert BAR_OPEN == bar.derive_transaction_price(DONT_CARE_TIMESTAMP, Keyword.HISTORICAL_PRICE_OPEN.value)


def test_derive_transaction_price_high(bar: HistoricalBar) -> None:  # pylint: disable=C0104, W0621
    assert BAR_HIGH == bar.derive_transaction_price(DONT_CARE_TIMESTAMP, Keyword.HISTORICAL_PRICE_HIGH.value)


def test_derive_transaction_price_low(bar: HistoricalBar) -> None:  # pylint: disable=C0104, W0621
    assert BAR_LOW == bar.derive_transaction_price(DONT_CARE_TIMESTAMP, Keyword.HISTORICAL_PRICE_LOW.value)


def test_derive_transaction_price_close(bar: HistoricalBar) -> None:  # pylint: disable=C0104, W0621
    assert BAR_CLOSE == bar.derive_transaction_price(DONT_CARE_TIMESTAMP, Keyword.HISTORICAL_PRICE_CLOSE.value)


@pytest.mark.parametrize("seconds", [-1, 0, 1, int(BAR_SECONDS / 2) - 1])  # test various time offsets after bar start timestamp
def test_derive_transaction_price_nearest_open(bar: HistoricalBar, seconds: int) -> None:  # pylint: disable=C0104, W0621
    transaction_timestamp = (BAR_START_TIMESTAMP + timedelta(seconds=seconds)).replace(tzinfo=timezone.utc)
    assert BAR_OPEN == bar.derive_transaction_price(transaction_timestamp, Keyword.HISTORICAL_PRICE_NEAREST.value)


@pytest.mark.parametrize(  # test various time offsets after bar start timestamp
    "seconds", [int(BAR_SECONDS / 2) + 1, BAR_SECONDS - 1, BAR_SECONDS, BAR_SECONDS + 1]
)
def test_derive_transaction_price_nearest_close(bar: HistoricalBar, seconds: int) -> None:  # pylint: disable=C0104, W0621
    transaction_timestamp = (BAR_START_TIMESTAMP + timedelta(seconds=seconds)).replace(tzinfo=timezone.utc)
    assert BAR_CLOSE == bar.derive_transaction_price(transaction_timestamp, Keyword.HISTORICAL_PRICE_NEAREST.value)
