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

from datetime import datetime
from os import listdir, makedirs, path, remove, unlink
from typing import Any, List, Optional

from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2RuntimeError

from dali.cache import CACHE_DIR
from dali.historical_bar import HistoricalBar
from dali.plugin.pair_converter.csv.kraken import Kraken

_CACHE_DIRECTORY: str = "output/kraken_test"


class TestKrakenCsvDownload:
    def test_chunking(self, mocker: Any) -> None:
        kraken_csv = Kraken("whatever")

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
        with open("input/USD_OHLCVT_test.zip", "rb") as file:
            mocker.patch.object(kraken_csv, "_google_file_to_bytes").return_value = file.read()

        test_bar: Optional[HistoricalBar] = kraken_csv.find_historical_bar("USDT", "USD", datetime.fromtimestamp(1601856000))
        files: List[str] = listdir(_CACHE_DIRECTORY)

        # Test if proper price was retrieved and file was chunked
        assert test_bar
        assert test_bar.low == RP2Decimal("1.778")
        assert "USDTUSD_1594080000_5.csv.gz" in files

        test_bar = kraken_csv.find_historical_bar("USDT", "USD", datetime.fromtimestamp(1601683300))

        # Check if price for longer time span was retrieved even though the timestamp doesn't exist in the csv
        # Also that proper price was retrieved from chunked files in the cache folder
        assert test_bar
        assert test_bar.low == RP2Decimal("1.6668")
