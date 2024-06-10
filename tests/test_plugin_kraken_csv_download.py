# Copyright 2023 Neal Chambers
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

# pylint: disable=protected-access

from datetime import datetime, timezone
from os import listdir, makedirs, path, remove, unlink
from typing import Any, List, Optional

from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2RuntimeError

from dali.cache import CACHE_DIR
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.in_transaction import InTransaction
from dali.plugin.pair_converter.csv.kraken import Kraken
from dali.transaction_manifest import TransactionManifest

_CACHE_DIRECTORY: str = "output/kraken_test"

# Fake Transaction
FAKE_TRANSACTION: InTransaction = InTransaction(
    plugin="Plugin",
    unique_id=Keyword.UNKNOWN.value,
    raw_data="raw",
    timestamp=datetime.fromtimestamp(1504541500, timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z"),
    asset="BTC",
    exchange="Kraken",
    holder="test",
    transaction_type=Keyword.BUY.value,
    spot_price=Keyword.UNKNOWN.value,
    crypto_in="1",
    crypto_fee=None,
    fiat_in_no_fee=None,
    fiat_in_with_fee=None,
    fiat_fee=None,
    notes="notes",
)


class TestKrakenCsvDownload:
    def test_chunking(self, mocker: Any) -> None:
        kraken_csv = Kraken(transaction_manifest=TransactionManifest([FAKE_TRANSACTION], 1, "USD"))

        if not path.exists(_CACHE_DIRECTORY):
            makedirs(_CACHE_DIRECTORY)

        # Flush test cache directory
        for filename in listdir(_CACHE_DIRECTORY):
            file_path = path.join(_CACHE_DIRECTORY, filename)
            try:
                if path.isfile(file_path):
                    unlink(file_path)
            except RP2RuntimeError:
                pass

        cache_path = path.join(CACHE_DIR, kraken_csv.cache_key())
        if path.exists(cache_path):
            remove(cache_path)

        mocker.patch.object(kraken_csv, "_Kraken__CACHE_DIRECTORY", _CACHE_DIRECTORY)
        if not path.exists(_CACHE_DIRECTORY):
            makedirs(_CACHE_DIRECTORY)

        mocker.patch.object(kraken_csv, "_Kraken__UNIFIED_CSV_FILE", "input/USD_OHLCVT_test.zip")

        test_bar: Optional[HistoricalBar] = kraken_csv.find_historical_bar("USDT", "USD", datetime.fromtimestamp(1601856000))
        files: List[str] = listdir(_CACHE_DIRECTORY)

        # Test if proper price was retrieved and file was chunked
        assert test_bar
        assert test_bar.low == RP2Decimal("1.778")
        assert "USDTUSD_1594080000_5.csv.gz" in files
        assert "USDTUSD_1296000000_10080.csv.gz" in files

        test_bar = kraken_csv.find_historical_bar("USDT", "USD", datetime.fromtimestamp(1601683300))

        # Check if price for longer time span was retrieved even though the timestamp doesn't exist in the csv
        # Also that proper price was retrieved from chunked files in the cache folder
        assert test_bar
        assert test_bar.low == RP2Decimal("1.6668")

        test_bars = kraken_csv.find_historical_bars("USDT", "USD", datetime.fromtimestamp(1601856000, timezone.utc), True, "1w")

        assert test_bars
        test_bar = test_bars[0]

        # Test to make sure it only emulates full weeks
        assert len(test_bars) == 5
        assert test_bar.low == RP2Decimal("1.9999")
        assert test_bar.volume == RP2Decimal("5999.9997")
