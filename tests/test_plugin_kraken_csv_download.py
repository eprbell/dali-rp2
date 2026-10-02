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

import json
import tracemalloc
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from os import listdir, makedirs, path, remove, unlink
from pathlib import Path
from shutil import copyfile
from typing import Any, Dict, List, Optional, Tuple
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import pytest
import requests
from pytest_mock import MockerFixture
from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2RuntimeError, RP2ValueError

import dali.plugin.pair_converter.csv.kraken as kraken_module
from dali.cache import CACHE_DIR
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.in_transaction import InTransaction
from dali.plugin.pair_converter.csv.kraken import Kraken
from dali.transaction_manifest import TransactionManifest

_CACHE_DIRECTORY: str = "output/kraken_test"
_UNIFIED_TEST_FILE: str = "input/USD_OHLCVT_test.zip"

# Last 1 minute candle in the USDTUSD_1.csv of _UNIFIED_TEST_FILE, and a newer one delivered by an update file
_LAST_UNIFIED_ROW: str = "1604448120,1.9999,1.9999,1.9999,1.9999,1999.9999,99\n"
_UPDATE_TIMESTAMP: int = 1604448180
_UPDATE_ROW: str = f"{_UPDATE_TIMESTAMP},2.1111,2.2222,2.0000,2.1234,21.21,21\n"
# Candle for a pair that isn't in _UNIFIED_TEST_FILE
_NEW_PAIR_ROW: str = f"{_UPDATE_TIMESTAMP},13500.1,13500.2,13500.0,13500.1,1.5,3\n"

# Synthetic XBTUSD candles in every Kraken timeframe up to the end of 2020, and a quarterly update that continues them from
# 2020-12-31 23:30 (30 minutes of overlap) and adds MATICUSD. All timeframes are aggregated from the same 1 minute candles.
# Finer timeframes cover a shorter history to keep the files small, and there were no XBTUSD trades at 2021-01-01 00:03.
_UNIFIED_FIXTURE: str = "input/Kraken_OHLCVT_unified_test.zip"
_UPDATE_FIXTURE: str = "input/Kraken_OHLCVT_update_test.zip"

# Kraken publishes its complete OHLCVT history as one zip file split into parts, which are listed in a checksum file
_KRAKEN_ASSETS_URL: str = "https://assets.kraken.com/marketing/institutions/"
_KRAKEN_CHECKSUMS_URL: str = _KRAKEN_ASSETS_URL + "OHLCVT_Full_PARTS_SHA256SUMS.txt"
# Kraken's public endpoint for the trades after a point in time
_KRAKEN_TRADES_URL: str = "https://api.kraken.com/0/public/Trades"
# 2021-01-01 00:00 UTC, right after the end of _UNIFIED_FIXTURE
_JANUARY_1: int = 1609459200
# Timeframes (in minutes) the plugin prices from
_PRICED_TIMEFRAMES: List[int] = [1, 5, 15, 60, 720, 1440]
# Prices of markets whose assets Kraken names differently from CCXT, which names the assets DaLI prices
_KRAKEN_NAMED_PRICES: Dict[str, str] = {
    "ETHXBT": "0.05",
    "XDGUSD": "0.07",
    "LUNA2USD": "1.3179",
    "LUNAUSD": "0.00015966",
    "REPV2USD": "12.5",
    "REPUSD": "11.0",
    "USTUSD": "0.02",
}

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


def _manifest() -> TransactionManifest:
    return TransactionManifest([FAKE_TRANSACTION], 1, "USD")


def _write_zip(zip_path: Path, csv_files: Dict[str, str]) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path, "w", ZIP_DEFLATED) as zip_file:
        for file_name, contents in csv_files.items():
            zip_file.writestr(file_name, contents)


def _read_zip(zip_path: Path) -> Dict[str, str]:
    with ZipFile(zip_path) as zip_file:
        return {file_name: zip_file.read(file_name).decode(encoding="utf-8") for file_name in zip_file.namelist()}


# The CSVs of a pair in every timeframe the plugin prices from, made from its 1 minute candles (time -> price).
# Each longer candle takes the price of its first minute with trades.
def _pair_csv_files(pair: str, prices: Dict[int, str]) -> Dict[str, str]:
    csv_files: Dict[str, str] = {}
    for minutes in _PRICED_TIMEFRAMES:
        candles: Dict[int, str] = {}
        for time, price in sorted(prices.items()):
            candles.setdefault(time - time % (minutes * 60), price)
        csv_files[f"{pair}_{minutes}.csv"] = "".join(f"{time},{price},{price},{price},{price},1,1\n" for time, price in candles.items())
    return csv_files


# Byte range [start, end) of each file's local header and data inside a zip file
def _file_spans(zip_path: str) -> Dict[str, Tuple[int, int]]:
    with ZipFile(zip_path) as zip_file:
        infos: List[ZipInfo] = sorted(zip_file.infolist(), key=lambda info: info.header_offset)
        ends: List[int] = [info.header_offset for info in infos[1:]] + [zip_file.start_dir]
    return {info.filename: (info.header_offset, end) for info, end in zip(infos, ends)}


class _FakeResponse:
    def __init__(self, status_code: int, content: bytes = b"", headers: Optional[Dict[str, str]] = None) -> None:
        self.status_code: int = status_code
        self.content: bytes = content
        self.headers: Dict[str, str] = headers if headers is not None else {}

    @property
    def text(self) -> str:
        return self.content.decode(encoding="utf-8")

    def json(self) -> Any:
        return json.loads(self.content)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


# Stands in for Kraken's servers: serves zip files split into parts plus the checksum list of the latest one, and trades
# like Kraken's public Trades endpoint. Records every request and every byte range that is downloaded.
class _FakeKrakenServer:
    def __init__(self, part_size: int = 1000) -> None:
        self.requests: List[str] = []
        self.downloaded: List[Tuple[int, int]] = []  # [start, end) offsets inside the joined zip file
        self.unreachable: bool = False
        self.trades_unreachable: bool = False
        self.rate_limited_trade_requests: int = 0  # How many of the next Trades requests get Kraken's rate limit error
        self.__part_size: int = part_size
        self.__parts: Dict[str, bytearray] = {}
        self.__part_offsets: Dict[str, int] = {}
        self.__latest_parts: List[str] = []
        self.__checksums: str = ""
        self.__trades: Dict[str, List[Tuple[int, str, str]]] = {}  # Pair -> (time in nanoseconds, price, volume)

    # Trades of a pair, as (time in seconds, price, volume)
    def add_trades(self, pair: str, trades: List[Tuple[float, str, str]]) -> None:
        self.__trades.setdefault(pair, []).extend((round(time * 1_000_000_000), price, volume) for time, price, volume in trades)
        self.__trades[pair].sort()

    def publish(self, release: str, zip_path: str) -> None:
        data: bytes = Path(zip_path).read_bytes()
        self.__latest_parts = []
        for number, start in enumerate(range(0, len(data), self.__part_size)):
            url: str = f"{_KRAKEN_ASSETS_URL}{release}.zip.part{number:02d}"
            self.__parts[url] = bytearray(data[start : start + self.__part_size])
            self.__part_offsets[url] = start
            self.__latest_parts.append(url)
        self.__checksums = "".join(f"{sha256(self.__parts[url]).hexdigest()}  {url.rsplit('/', 1)[1]}\n" for url in self.__latest_parts)

    # Flips a byte of the latest release, as if it got corrupted on the way
    def corrupt(self, offset: int) -> None:
        for url in self.__latest_parts:
            if self.__part_offsets[url] <= offset < self.__part_offsets[url] + len(self.__parts[url]):
                self.__parts[url][offset - self.__part_offsets[url]] ^= 0xFF

    def head(self, url: str, **_kwargs: Any) -> _FakeResponse:
        self.__request(url)
        if url not in self.__parts:
            return _FakeResponse(404)
        return _FakeResponse(200, headers={"Content-Length": str(len(self.__parts[url]))})

    def get(self, url: str, headers: Optional[Dict[str, str]] = None, params: Optional[Dict[str, Any]] = None, **_kwargs: Any) -> _FakeResponse:
        self.__request(url)
        if url == _KRAKEN_TRADES_URL:
            return self.__serve_trades(params or {})
        if url == _KRAKEN_CHECKSUMS_URL:
            return _FakeResponse(200, self.__checksums.encode(encoding="utf-8"))
        if url not in self.__parts:
            return _FakeResponse(404)
        part: bytearray = self.__parts[url]
        first, last = (int(value) for value in (headers or {})["Range"].removeprefix("bytes=").split("-"))
        body: bytes = bytes(part[first : last + 1])
        self.downloaded.append((self.__part_offsets[url] + first, self.__part_offsets[url] + first + len(body)))
        return _FakeResponse(206, body, {"Content-Range": f"bytes {first}-{first + len(body) - 1}/{len(part)}"})

    def is_downloaded(self, span: Tuple[int, int]) -> bool:
        return any(first < span[1] and span[0] < last for first, last in self.downloaded)

    # Like Kraken, "since" is a time in seconds or the nanosecond cursor returned as "last", and at most 1000 trades are returned
    def __serve_trades(self, params: Dict[str, Any]) -> _FakeResponse:
        if self.trades_unreachable:
            raise requests.ConnectionError(f"Can't reach {_KRAKEN_TRADES_URL}")
        if self.rate_limited_trade_requests > 0:
            self.rate_limited_trade_requests -= 1
            return self.__json({"error": ["EGeneral:Too many requests"]})
        if params["pair"] not in self.__trades:
            return self.__json({"error": ["EQuery:Unknown asset pair"]})
        since: int = int(params["since"])
        trades: List[Tuple[int, str, str]] = self.__trades[params["pair"]]
        # A time in seconds includes the trades from that second on, a cursor only the trades after it
        first_included: int = since * 1_000_000_000 if since < 10**12 else since + 1
        newer: List[Tuple[int, str, str]] = [trade for trade in trades if trade[0] >= first_included]
        page: List[Tuple[int, str, str]] = newer[: min(int(params.get("count", 1000)), 1000)]
        rows: List[List[Any]] = [[price, volume, time / 1_000_000_000, "b", "l", "", number] for number, (time, price, volume) in enumerate(page)]
        return self.__json({"error": [], "result": {f"X{params['pair']}": rows, "last": str(page[-1][0] if page else since)}})

    @staticmethod
    def __json(body: Dict[str, Any]) -> _FakeResponse:
        return _FakeResponse(200, json.dumps(body).encode(encoding="utf-8"))

    def __request(self, url: str) -> None:
        if self.unreachable:
            raise requests.ConnectionError(f"Can't reach {url}")
        self.requests.append(url)


# Fails any test that would reach the real Kraken server
class _NoNetwork:
    def head(self, url: str, **_kwargs: Any) -> None:
        raise AssertionError(f"Unexpected network access to {url}")

    def get(self, url: str, **_kwargs: Any) -> None:
        raise AssertionError(f"Unexpected network access to {url}")


@pytest.fixture(name="unified_csv_file")
def unified_csv_file_fixture(tmp_path: Path, mocker: MockerFixture) -> Path:
    # Point every file the plugin touches into tmp_path, so tests never read or clobber a real download or cache.
    # Directories are not created, just like on a first run.
    cache_directory: Path = tmp_path / "kraken"
    csv_directory: Path = cache_directory / "csv"
    unified_csv_file: Path = csv_directory / "Kraken_OHLCVT.zip"

    mocker.patch("dali.cache.CACHE_DIR", str(tmp_path / "dali_cache"))
    mocker.patch.object(Kraken, "_Kraken__CACHE_DIRECTORY", f"{cache_directory}/")
    mocker.patch.object(Kraken, "_Kraken__CSV_DIRECTORY", f"{csv_directory}/")
    mocker.patch.object(Kraken, "_Kraken__UNIFIED_CSV_FILE", str(unified_csv_file))

    # Never reach the real Kraken server
    mocker.patch("dali.plugin.pair_converter.csv.kraken.Session", return_value=_NoNetwork())

    return unified_csv_file


@pytest.fixture(name="kraken_server")
def kraken_server_fixture(unified_csv_file: Path, mocker: MockerFixture) -> _FakeKrakenServer:
    # Starts without a local unified file, so prices come from Kraken's server
    assert not unified_csv_file.exists()
    server: _FakeKrakenServer = _FakeKrakenServer()
    mocker.patch("dali.plugin.pair_converter.csv.kraken.Session", return_value=server)
    return server


@pytest.fixture(name="kraken_trades")
def kraken_trades_fixture(kraken_server: _FakeKrakenServer, mocker: MockerFixture) -> _FakeKrakenServer:
    # The release ends with 2020, so later times can only be priced from Kraken's trades
    kraken_server.publish("Kraken_OHLCVT_Full_2020Q4", _UNIFIED_FIXTURE)
    # Don't wait between requests the way Kraken's rate limit requires
    mocker.patch("dali.plugin.pair_converter.csv.kraken_trades.sleep")
    return kraken_server


@pytest.fixture(name="merged_kraken_csv")
def merged_kraken_csv_fixture(unified_csv_file: Path, tmp_path: Path) -> Kraken:
    unified_csv_file.parent.mkdir(parents=True)
    copyfile(_UNIFIED_FIXTURE, unified_csv_file)
    # The merge deletes the update file, so merge a copy
    update_file: Path = tmp_path / "update.zip"
    copyfile(_UPDATE_FIXTURE, update_file)
    return Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)


class TestKrakenCsvDownload:
    def test_chunking(self, mocker: Any) -> None:
        # Offline mode checks that the local unified file exists when it starts, so point it at the test file first
        mocker.patch.object(Kraken, "_Kraken__UNIFIED_CSV_FILE", "input/USD_OHLCVT_test.zip")
        kraken_csv = Kraken(transaction_manifest=TransactionManifest([FAKE_TRANSACTION], 1, "USD"), offline=True)

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


class TestKrakenCsvUpdateFile:
    def test_update_file_is_merged_into_unified_file(self, unified_csv_file: Path, tmp_path: Path) -> None:
        _write_zip(
            unified_csv_file,
            {
                "XBTUSD_1.csv": "1601855760,1.1,1.2,1.0,1.1,10,1\n",
                "ETHUSD_1.csv": "1601855760,2.1,2.2,2.0,2.1,20,2\n",
            },
        )
        update_file: Path = tmp_path / "update.zip"
        _write_zip(
            update_file,
            {
                "XBTUSD_1.csv": "1601855820,1.3,1.4,1.2,1.3,30,3\n",
                "SOLUSD_1.csv": "1601855820,3.1,3.2,3.0,3.1,40,4\n",
            },
        )

        Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)

        # Update rows are appended to existing pairs, new pairs are added and untouched pairs are kept
        assert _read_zip(unified_csv_file) == {
            "XBTUSD_1.csv": "1601855760,1.1,1.2,1.0,1.1,10,1\n1601855820,1.3,1.4,1.2,1.3,30,3\n",
            "ETHUSD_1.csv": "1601855760,2.1,2.2,2.0,2.1,20,2\n",
            "SOLUSD_1.csv": "1601855820,3.1,3.2,3.0,3.1,40,4\n",
        }
        # The update file is removed so the merge isn't repeated on the next run
        assert not update_file.exists()

    def test_update_file_refreshes_already_chunked_pairs(self, unified_csv_file: Path, tmp_path: Path) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_TEST_FILE, unified_csv_file)

        # First run chunks and caches USDTUSD, whose 1 minute candles end at _LAST_UNIFIED_ROW
        assert Kraken(transaction_manifest=_manifest(), offline=True).find_historical_bar("USDT", "USD", datetime.fromtimestamp(1601856000, timezone.utc))

        # Second run brings in a newer candle through an update file
        update_file: Path = tmp_path / "update.zip"
        _write_zip(update_file, {"USDTUSD_1.csv": _UPDATE_ROW})
        kraken_csv = Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)

        # The new candle must be found, not a stale lower resolution one from the old chunks
        test_bar: Optional[HistoricalBar] = kraken_csv.find_historical_bar("USDT", "USD", datetime.fromtimestamp(_UPDATE_TIMESTAMP, timezone.utc))
        assert test_bar
        assert test_bar.duration == timedelta(minutes=1)
        assert test_bar.close == RP2Decimal("2.1234")

    def test_offline_mode_without_a_local_unified_file_stops_with_an_error(self, unified_csv_file: Path, tmp_path: Path) -> None:
        update_file: Path = tmp_path / "update.zip"
        _write_zip(update_file, {"USDTUSD_1.csv": _UPDATE_ROW})

        with pytest.raises(RP2RuntimeError, match="Kraken_OHLCVT.zip"):
            Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)

        # The update file is kept, so it can be merged once the local unified file is in place
        assert update_file.exists()
        assert not unified_csv_file.exists()

    def test_update_file_needs_offline_mode(self, unified_csv_file: Path, tmp_path: Path) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_FIXTURE, unified_csv_file)
        update_file: Path = tmp_path / "update.zip"
        _write_zip(update_file, {"XBTUSD_1.csv": _UPDATE_ROW})

        # Update files are only merged into the local unified file of offline mode. Online, Kraken's latest release is used instead.
        with pytest.raises(RP2ValueError, match="kraken_csv_offline"):
            Kraken(transaction_manifest=_manifest(), update_file=str(update_file))

        assert update_file.exists()

    def test_failed_merge_leaves_unified_file_intact(self, unified_csv_file: Path, tmp_path: Path, mocker: MockerFixture) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_TEST_FILE, unified_csv_file)
        original_contents: Dict[str, str] = _read_zip(unified_csv_file)
        update_file: Path = tmp_path / "update.zip"
        _write_zip(update_file, {"USDTUSD_1.csv": _UPDATE_ROW})

        # Simulate running out of disk space as soon as anything is written into a zip
        open_zip_entry = ZipFile.open

        def open_zip_entry_without_disk_space(zip_file: ZipFile, name: Any, *args: Any, **kwargs: Any) -> Any:
            if (args[0] if args else kwargs.get("mode", "r")) == "w":
                raise OSError(28, "No space left on device")
            return open_zip_entry(zip_file, name, *args, **kwargs)

        mocker.patch.object(ZipFile, "open", open_zip_entry_without_disk_space)

        with pytest.raises(OSError, match="No space left on device"):
            Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)

        # The 4+ GB unified file must survive a failed merge, the update file must be kept for a retry
        # and no partially written temporary file may be left behind
        assert _read_zip(unified_csv_file) == original_contents
        assert update_file.exists()
        assert listdir(unified_csv_file.parent) == [unified_csv_file.name]

    def test_update_file_merge_streams_instead_of_loading_into_memory(self, unified_csv_file: Path, tmp_path: Path) -> None:
        # About 10 MB uncompressed, but compresses to almost nothing, so the test stays fast
        large_csv: str = _LAST_UNIFIED_ROW * 200_000
        _write_zip(unified_csv_file, {"USDTUSD_1.csv": large_csv})
        update_file: Path = tmp_path / "update.zip"
        _write_zip(update_file, {"USDTUSD_1.csv": _UPDATE_ROW})

        tracemalloc.start()
        try:
            Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)
            _, peak_memory = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        # The real unified file is 4+ GB, so the merge must never hold a whole CSV in memory
        assert peak_memory < len(large_csv) // 4

    def test_update_rows_start_on_a_new_line(self, unified_csv_file: Path, tmp_path: Path) -> None:
        # The last row of the unified CSV has no trailing newline
        _write_zip(unified_csv_file, {"USDTUSD_1.csv": _LAST_UNIFIED_ROW.rstrip("\n")})
        update_file: Path = tmp_path / "update.zip"
        _write_zip(update_file, {"USDTUSD_1.csv": _UPDATE_ROW})

        Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)

        assert _read_zip(unified_csv_file)["USDTUSD_1.csv"] == _LAST_UNIFIED_ROW + _UPDATE_ROW

    def test_update_rows_already_in_unified_file_are_skipped(self, unified_csv_file: Path, tmp_path: Path) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_TEST_FILE, unified_csv_file)
        original_csv: str = _read_zip(unified_csv_file)["USDTUSD_1.csv"]
        # The update overlaps the last two rows of the unified file, e.g. because the unified file was refreshed after the update came out
        update_file: Path = tmp_path / "update.zip"
        _write_zip(update_file, {"USDTUSD_1.csv": "1604448060,1.9999,1.9999,1.9999,1.9999,1999.9999,99\n" + _LAST_UNIFIED_ROW + _UPDATE_ROW})

        Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)

        assert _read_zip(unified_csv_file)["USDTUSD_1.csv"] == original_csv + _UPDATE_ROW

    def test_merging_the_same_update_file_twice_changes_nothing(self, unified_csv_file: Path, tmp_path: Path) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_TEST_FILE, unified_csv_file)
        update_file: Path = tmp_path / "update.zip"
        update_csv_files: Dict[str, str] = {"USDTUSD_1.csv": _UPDATE_ROW, "XBTUSD_1.csv": _NEW_PAIR_ROW}
        _write_zip(update_file, update_csv_files)
        Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)
        merged_contents: Dict[str, str] = _read_zip(unified_csv_file)

        # E.g. the previous run was interrupted after the merge, but before the update file was deleted
        _write_zip(update_file, update_csv_files)
        Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)

        assert _read_zip(unified_csv_file) == merged_contents

    def test_update_csv_files_in_a_folder_are_matched_by_file_name(self, unified_csv_file: Path, tmp_path: Path) -> None:
        _write_zip(unified_csv_file, {"USDTUSD_1.csv": _LAST_UNIFIED_ROW})
        update_file: Path = tmp_path / "update.zip"
        _write_zip(update_file, {"Kraken_OHLCVT_Q4_2020/USDTUSD_1.csv": _UPDATE_ROW, "Kraken_OHLCVT_Q4_2020/XBTUSD_1.csv": _NEW_PAIR_ROW})

        Kraken(transaction_manifest=_manifest(), update_file=str(update_file), offline=True)

        # The plugin only looks for CSVs at the top level of the unified file
        assert _read_zip(unified_csv_file) == {"USDTUSD_1.csv": _LAST_UNIFIED_ROW + _UPDATE_ROW, "XBTUSD_1.csv": _NEW_PAIR_ROW}


class TestKrakenCsvUpdateFileFixtures:
    @pytest.mark.usefixtures("merged_kraken_csv")
    def test_merge_keeps_every_candle_once_in_order(self, unified_csv_file: Path) -> None:
        unified_contents: Dict[str, str] = _read_zip(Path(_UNIFIED_FIXTURE))
        update_contents: Dict[str, str] = _read_zip(Path(_UPDATE_FIXTURE))
        merged_contents: Dict[str, str] = _read_zip(unified_csv_file)

        assert merged_contents.keys() == unified_contents.keys() | update_contents.keys()
        for file_name, merged_csv in merged_contents.items():
            rows: List[str] = merged_csv.splitlines()
            timestamps: List[int] = [int(row.split(",")[0]) for row in rows]
            # In chronological order without duplicates, even where the update overlaps the unified file
            assert timestamps == sorted(set(timestamps)), file_name
            assert set(rows) == set(unified_contents.get(file_name, "").splitlines()) | set(update_contents.get(file_name, "").splitlines()), file_name

    def test_prices_come_from_both_sides_of_the_boundary(self, merged_kraken_csv: Kraken) -> None:
        # Last 1 minute candle of the unified file
        test_bar: Optional[HistoricalBar] = merged_kraken_csv.find_historical_bar("BTC", "USD", datetime(2020, 12, 31, 23, 59, tzinfo=timezone.utc))
        assert test_bar
        assert test_bar.duration == timedelta(minutes=1)
        assert test_bar.close == RP2Decimal("28990.0")

        # First new 1 minute candle of the update file
        test_bar = merged_kraken_csv.find_historical_bar("BTC", "USD", datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc))
        assert test_bar
        assert test_bar.duration == timedelta(minutes=1)
        assert test_bar.close == RP2Decimal("28986.5")

    def test_minute_without_trades_is_priced_from_the_five_minute_candle(self, merged_kraken_csv: Kraken) -> None:
        test_bar: Optional[HistoricalBar] = merged_kraken_csv.find_historical_bar("BTC", "USD", datetime(2021, 1, 1, 0, 3, tzinfo=timezone.utc))

        assert test_bar
        assert test_bar.duration == timedelta(minutes=5)
        assert test_bar.timestamp == datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert test_bar.close == RP2Decimal("28961.9")

    def test_pair_only_in_the_update_file_can_be_priced(self, merged_kraken_csv: Kraken) -> None:
        test_bar: Optional[HistoricalBar] = merged_kraken_csv.find_historical_bar("MATIC", "USD", datetime(2021, 1, 10, 12, 0, tzinfo=timezone.utc))

        # The finest MATICUSD candles the fixture has for this date are 12 hour candles
        assert test_bar
        assert test_bar.duration == timedelta(hours=12)
        assert test_bar.close == RP2Decimal("0.029514")

    def test_weekly_candle_spans_the_boundary(self, merged_kraken_csv: Kraken) -> None:
        test_bars: Optional[List[HistoricalBar]] = merged_kraken_csv.find_historical_bars("BTC", "USD", datetime(2020, 12, 28, tzinfo=timezone.utc), True, "1w")

        assert test_bars
        assert test_bars[0].timestamp == datetime(2020, 12, 28, tzinfo=timezone.utc)
        # Sum of the daily volumes of 2020-12-28 to 2020-12-31 from the unified file and 2021-01-01 to 2021-01-03 from the update file
        assert test_bars[0].volume == RP2Decimal("32822.59186282")


class TestKrakenCsvRemoteRelease:
    def test_prices_are_read_from_kraken_by_downloading_only_the_priced_pair(self, kraken_server: _FakeKrakenServer, unified_csv_file: Path) -> None:
        kraken_server.publish("Kraken_OHLCVT_Full_2021Q1", _UPDATE_FIXTURE)

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest()).find_historical_bar(
            "BTC", "USD", datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
        )

        assert test_bar
        assert test_bar.duration == timedelta(minutes=1)
        assert test_bar.close == RP2Decimal("28986.5")
        # Only the XBTUSD CSVs the plugin prices from were downloaded (not MATICUSD, nor the 30 and 240 minute ones)
        priced_files: List[str] = [f"XBTUSD_{minutes}.csv" for minutes in _PRICED_TIMEFRAMES]
        for file_name, span in _file_spans(_UPDATE_FIXTURE).items():
            assert kraken_server.is_downloaded(span) == (file_name in priced_files), file_name
        # The release isn't saved as a local unified file
        assert not unified_csv_file.exists()

    def test_route_selection_downloads_only_daily_candles(self, kraken_server: _FakeKrakenServer) -> None:
        kraken_server.publish("Kraken_OHLCVT_Full_2021Q1", _UPDATE_FIXTURE)

        test_bars: Optional[List[HistoricalBar]] = Kraken(transaction_manifest=_manifest()).find_historical_bars(
            "BTC", "USD", datetime(2021, 1, 4, tzinfo=timezone.utc), True, "1w"
        )

        # Weekly candles are emulated from daily candles, which is all that picking routes needs
        assert test_bars
        assert test_bars[0].timestamp == datetime(2021, 1, 4, tzinfo=timezone.utc)
        for file_name, span in _file_spans(_UPDATE_FIXTURE).items():
            assert kraken_server.is_downloaded(span) == (file_name == "XBTUSD_1440.csv"), file_name

    def test_pairs_are_downloaded_once_per_release(self, kraken_server: _FakeKrakenServer) -> None:
        kraken_server.publish("Kraken_OHLCVT_Full_2021Q1", _UPDATE_FIXTURE)
        assert Kraken(transaction_manifest=_manifest()).find_historical_bar("BTC", "USD", datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc))
        kraken_server.requests.clear()

        # The next run prices from the cached candles
        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest()).find_historical_bar(
            "BTC", "USD", datetime(2021, 1, 1, 0, 3, tzinfo=timezone.utc)
        )

        assert test_bar
        assert test_bar.close == RP2Decimal("28961.9")
        # Only the checksum list is fetched, to check whether Kraken published a new release
        assert kraken_server.requests == [_KRAKEN_CHECKSUMS_URL]

    def test_new_release_replaces_pairs_read_from_the_previous_one(self, kraken_server: _FakeKrakenServer) -> None:
        kraken_server.publish("Kraken_OHLCVT_Full_2020Q4", _UNIFIED_FIXTURE)
        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest()).find_historical_bar(
            "BTC", "USD", datetime(2020, 12, 31, 23, 59, tzinfo=timezone.utc)
        )
        assert test_bar
        assert test_bar.close == RP2Decimal("28990.0")

        # Kraken publishes the next quarter, which the XBTUSD candles cached from the previous release don't have
        kraken_server.publish("Kraken_OHLCVT_Full_2021Q1", _UPDATE_FIXTURE)
        test_bar = Kraken(transaction_manifest=_manifest()).find_historical_bar("BTC", "USD", datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc))

        assert test_bar
        assert test_bar.duration == timedelta(minutes=1)
        assert test_bar.close == RP2Decimal("28986.5")

    def test_local_unified_file_is_ignored_when_online(self, kraken_server: _FakeKrakenServer, unified_csv_file: Path) -> None:
        kraken_server.publish("Kraken_OHLCVT_Full_2021Q1", _UPDATE_FIXTURE)
        # E.g. an old unified file downloaded by a previous version of the plugin, which ends with 2020
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_FIXTURE, unified_csv_file)

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest()).find_historical_bar(
            "BTC", "USD", datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc)
        )

        # Priced from Kraken's latest release, and the local file is left alone
        assert test_bar
        assert test_bar.duration == timedelta(minutes=1)
        assert test_bar.close == RP2Decimal("28986.5")
        assert kraken_server.downloaded
        assert unified_csv_file.exists()

    def test_unreachable_kraken_stops_with_an_error(self, kraken_server: _FakeKrakenServer) -> None:
        kraken_server.publish("Kraken_OHLCVT_Full_2021Q1", _UPDATE_FIXTURE)
        kraken_server.unreachable = True

        # Kraken's REST API isn't accurate enough to fall back to, so DaLI stops rather than use a worse price
        with pytest.raises(RP2RuntimeError, match="Kraken"):
            Kraken(transaction_manifest=_manifest()).find_historical_bar("BTC", "USD", datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc))

    def test_corrupted_download_stops_with_an_error(self, kraken_server: _FakeKrakenServer) -> None:
        kraken_server.publish("Kraken_OHLCVT_Full_2021Q1", _UPDATE_FIXTURE)
        start, _ = _file_spans(_UPDATE_FIXTURE)["XBTUSD_1.csv"]
        kraken_server.corrupt(start + 100)  # Past the local file header, inside the compressed data

        # Every CSV's CRC-32 is checked, so a bad download can't turn into a wrong price
        with pytest.raises(RP2RuntimeError, match="Kraken"):
            Kraken(transaction_manifest=_manifest()).find_historical_bar("BTC", "USD", datetime(2021, 1, 1, 0, 0, tzinfo=timezone.utc))


class TestKrakenTrades:
    def test_times_after_the_release_are_priced_from_kraken_trades(self, kraken_trades: _FakeKrakenServer) -> None:
        kraken_trades.add_trades(
            "XBTUSD",
            [
                (_JANUARY_1 + 5, "29000.0", "0.5"),
                (_JANUARY_1 + 20, "29010.5", "0.25"),
                (_JANUARY_1 + 55, "29005.2", "1.0"),
                (_JANUARY_1 + 65, "29100.0", "0.1"),
            ],
        )

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest()).find_historical_bar(
            "BTC", "USD", datetime.fromtimestamp(_JANUARY_1 + 30, timezone.utc)
        )

        # The candle of the trades in that minute. The last trade is in the next minute.
        assert test_bar == HistoricalBar(
            duration=timedelta(minutes=1),
            timestamp=datetime.fromtimestamp(_JANUARY_1, timezone.utc),
            open=RP2Decimal("29000.0"),
            high=RP2Decimal("29010.5"),
            low=RP2Decimal("29000.0"),
            close=RP2Decimal("29005.2"),
            volume=RP2Decimal("1.75"),
        )

    def test_minute_without_trades_uses_the_next_timeframe_with_trades(self, kraken_trades: _FakeKrakenServer) -> None:
        kraken_trades.add_trades("XBTUSD", [(_JANUARY_1 + 125, "29050.0", "0.3")])

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest()).find_historical_bar(
            "BTC", "USD", datetime.fromtimestamp(_JANUARY_1 + 190, timezone.utc)
        )

        # There were no trades at 00:03, so like with the CSV candles the 5 minute candle is used
        assert test_bar
        assert test_bar.duration == timedelta(minutes=5)
        assert test_bar.timestamp == datetime.fromtimestamp(_JANUARY_1, timezone.utc)
        assert test_bar.close == RP2Decimal("29050.0")

    def test_busy_minutes_are_read_across_several_requests(self, kraken_trades: _FakeKrakenServer) -> None:
        # 2,500 trades in one minute, while Kraken returns at most 1,000 per request
        kraken_trades.add_trades("XBTUSD", [(_JANUARY_1 + number * 0.02, f"{29000 + number / 100:.2f}", "0.001") for number in range(2500)])
        kraken_trades.add_trades("XBTUSD", [(_JANUARY_1 + 61, "1.0", "1.0")])

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest()).find_historical_bar(
            "BTC", "USD", datetime.fromtimestamp(_JANUARY_1 + 10, timezone.utc)
        )

        assert test_bar
        assert test_bar.close == RP2Decimal("29024.99")
        assert test_bar.high == RP2Decimal("29024.99")
        assert test_bar.volume == RP2Decimal("2.5")

    def test_pair_listed_after_the_release_is_priced_from_trades(self, kraken_trades: _FakeKrakenServer) -> None:
        kraken_trades.add_trades("SOLUSD", [(_JANUARY_1 + 10, "1.52", "40")])

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest()).find_historical_bar(
            "SOL", "USD", datetime.fromtimestamp(_JANUARY_1, timezone.utc)
        )

        assert test_bar
        assert test_bar.close == RP2Decimal("1.52")

    def test_times_covered_by_the_release_are_not_priced_from_trades(self, kraken_trades: _FakeKrakenServer) -> None:
        # A made-up trade in the release's last minute, which must not be used
        kraken_trades.add_trades("XBTUSD", [(_JANUARY_1 - 30, "1.0", "1.0")])

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest()).find_historical_bar(
            "BTC", "USD", datetime(2020, 12, 31, 23, 59, tzinfo=timezone.utc)
        )

        assert test_bar
        assert test_bar.close == RP2Decimal("28990.0")
        assert _KRAKEN_TRADES_URL not in kraken_trades.requests

    def test_pair_kraken_does_not_trade_has_no_price(self, kraken_trades: _FakeKrakenServer) -> None:
        # Neither in the release nor known to Kraken's Trades endpoint: nothing to price from, which isn't an error
        assert Kraken(transaction_manifest=_manifest()).find_historical_bar("DOGE", "USD", datetime.fromtimestamp(_JANUARY_1, timezone.utc)) is None
        assert _KRAKEN_TRADES_URL in kraken_trades.requests

    def test_rate_limited_requests_are_retried(self, kraken_trades: _FakeKrakenServer) -> None:
        kraken_trades.add_trades("XBTUSD", [(_JANUARY_1 + 5, "29000.0", "0.5")])
        kraken_trades.rate_limited_trade_requests = 2

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest()).find_historical_bar(
            "BTC", "USD", datetime.fromtimestamp(_JANUARY_1, timezone.utc)
        )

        assert test_bar
        assert test_bar.close == RP2Decimal("29000.0")

    def test_unreachable_trades_endpoint_stops_with_an_error(self, kraken_trades: _FakeKrakenServer) -> None:
        kraken_trades.trades_unreachable = True

        with pytest.raises(RP2RuntimeError, match="Kraken"):
            Kraken(transaction_manifest=_manifest()).find_historical_bar("BTC", "USD", datetime.fromtimestamp(_JANUARY_1, timezone.utc))


class TestKrakenCsvOffline:
    def test_offline_mode_reads_only_the_local_unified_file(self, kraken_server: _FakeKrakenServer, unified_csv_file: Path) -> None:
        kraken_server.publish("Kraken_OHLCVT_Full_2021Q1", _UPDATE_FIXTURE)
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_FIXTURE, unified_csv_file)

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest(), offline=True).find_historical_bar(
            "BTC", "USD", datetime(2020, 12, 31, 23, 59, tzinfo=timezone.utc)
        )

        assert test_bar
        assert test_bar.close == RP2Decimal("28990.0")
        assert not kraken_server.requests

    def test_offline_mode_stops_after_the_local_data_ends(self, kraken_server: _FakeKrakenServer, unified_csv_file: Path) -> None:
        kraken_server.publish("Kraken_OHLCVT_Full_2021Q1", _UPDATE_FIXTURE)
        kraken_server.add_trades("XBTUSD", [(_JANUARY_1 + 5, "29000.0", "0.5")])
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_FIXTURE, unified_csv_file)

        # The local data ends with 2020. Offline mode doesn't contact Kraken, and its REST API isn't accurate enough to use.
        with pytest.raises(RP2RuntimeError, match="2020-12-31"):
            Kraken(transaction_manifest=_manifest(), offline=True).find_historical_bar("BTC", "USD", datetime.fromtimestamp(_JANUARY_1, timezone.utc))
        assert not kraken_server.requests

    def test_offline_mode_stops_for_a_market_missing_from_the_local_data(self, unified_csv_file: Path) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_FIXTURE, unified_csv_file)

        with pytest.raises(RP2RuntimeError, match="SOLUSD"):
            Kraken(transaction_manifest=_manifest(), offline=True).find_historical_bar("SOL", "USD", datetime.fromtimestamp(_JANUARY_1, timezone.utc))

    def test_offline_route_selection_skips_markets_missing_from_the_local_data(self, unified_csv_file: Path) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_FIXTURE, unified_csv_file)

        # Picking routes only needs trading volumes, so a missing market isn't an error there
        kraken_csv: Kraken = Kraken(transaction_manifest=_manifest(), offline=True)
        assert kraken_csv.find_historical_bars("SOL", "USD", datetime(2020, 12, 7, tzinfo=timezone.utc), True, "1w") is None

    def test_corrupt_local_unified_file_stops_with_an_error(self, unified_csv_file: Path) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        unified_csv_file.write_bytes(b"not a zip file")

        with pytest.raises(RP2RuntimeError, match="corrupt"):
            Kraken(transaction_manifest=_manifest(), offline=True).find_historical_bar("BTC", "USD", datetime(2020, 12, 31, 23, 59, tzinfo=timezone.utc))

        # The file is the user's own copy, so it's left for them to replace
        assert unified_csv_file.exists()


class TestKrakenCsvChunksInMemory:
    @staticmethod
    def __chunk_reads(read_spy: Any) -> int:
        return sum(1 for call in read_spy.call_args_list if call.args[1] == "rt")

    def test_consecutive_prices_read_their_chunk_file_once(self, unified_csv_file: Path, mocker: MockerFixture) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_FIXTURE, unified_csv_file)
        read_spy = mocker.spy(kraken_module, "gopen")
        kraken_csv: Kraken = Kraken(transaction_manifest=_manifest(), offline=True)

        # Transactions are priced in time order, so they keep hitting the same month of 1 minute candles
        closes: List[RP2Decimal] = []
        for minute in (22, 30, 45, 58):
            test_bar: Optional[HistoricalBar] = kraken_csv.find_historical_bar("BTC", "USD", datetime(2020, 12, 31, 23, minute, tzinfo=timezone.utc))
            assert test_bar
            assert test_bar.duration == timedelta(minutes=1)
            closes.append(test_bar.close)

        assert closes == [RP2Decimal("29307.8"), RP2Decimal("29260.0"), RP2Decimal("29140.1"), RP2Decimal("28988.1")]
        assert self.__chunk_reads(read_spy) == 1

    def test_older_chunks_are_dropped_from_memory(self, unified_csv_file: Path, mocker: MockerFixture) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_FIXTURE, unified_csv_file)
        mocker.patch.object(Kraken, "_Kraken__CHUNKS_IN_MEMORY", 1)
        read_spy = mocker.spy(kraken_module, "gopen")
        kraken_csv: Kraken = Kraken(transaction_manifest=_manifest(), offline=True)

        # A 1 minute candle, then a 12 hour one from another chunk file, then the 1 minute one again
        for moment in (datetime(2020, 12, 31, 23, 58, tzinfo=timezone.utc), datetime(2020, 12, 15, 12, tzinfo=timezone.utc)) * 2:
            assert kraken_csv.find_historical_bar("BTC", "USD", moment)

        # With room for a single chunk in memory, each lookup reads its chunk file again
        assert self.__chunk_reads(read_spy) == 4


class TestKrakenCsvAssetNames:
    # CCXT names the assets DaLI prices, while Kraken's CSVs and Trades endpoint use Kraken's names for some of them
    @pytest.mark.parametrize(
        "base_asset, quote_asset, kraken_pair",
        [
            ("ETH", "BTC", "ETHXBT"),
            ("DOGE", "USD", "XDGUSD"),
            ("LUNA", "USD", "LUNA2USD"),  # Terra 2.0
            ("LUNC", "USD", "LUNAUSD"),  # Terra Classic
            ("REP", "USD", "REPV2USD"),
            ("REPV1", "USD", "REPUSD"),
            ("USTC", "USD", "USTUSD"),
        ],
    )
    def test_assets_are_priced_from_their_kraken_markets(self, unified_csv_file: Path, base_asset: str, quote_asset: str, kraken_pair: str) -> None:
        csv_files: Dict[str, str] = {}
        for pair, price in _KRAKEN_NAMED_PRICES.items():
            csv_files.update(_pair_csv_files(pair, {_JANUARY_1: price}))
        _write_zip(unified_csv_file, csv_files)

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest(), offline=True).find_historical_bar(
            base_asset, quote_asset, datetime.fromtimestamp(_JANUARY_1, timezone.utc)
        )

        assert test_bar
        assert test_bar.close == RP2Decimal(_KRAKEN_NAMED_PRICES[kraken_pair])

    def test_btc_quoted_markets_are_priced_from_the_release_and_trades(self, kraken_trades: _FakeKrakenServer, tmp_path: Path) -> None:
        release: Path = tmp_path / "release.zip"
        _write_zip(release, _pair_csv_files("ETHXBT", {_JANUARY_1 - 60: "0.0305"}))
        kraken_trades.publish("Kraken_OHLCVT_Full_2020Q4", str(release))
        kraken_trades.add_trades("ETHXBT", [(_JANUARY_1 + 5, "0.0306", "2.0")])
        kraken_csv: Kraken = Kraken(transaction_manifest=_manifest())

        # The last minute of the release
        test_bar: Optional[HistoricalBar] = kraken_csv.find_historical_bar("ETH", "BTC", datetime.fromtimestamp(_JANUARY_1 - 60, timezone.utc))
        assert test_bar
        assert test_bar.close == RP2Decimal("0.0305")

        # The first minute after it, from Kraken's trades
        test_bar = kraken_csv.find_historical_bar("ETH", "BTC", datetime.fromtimestamp(_JANUARY_1, timezone.utc))
        assert test_bar
        assert test_bar.close == RP2Decimal("0.0306")

    def test_route_selection_reads_markets_by_their_kraken_names(self, unified_csv_file: Path) -> None:
        # Daily candles from Thursday 2020-12-17 to Sunday 2020-12-27
        days: Dict[int, str] = {int(datetime(2020, 12, day, tzinfo=timezone.utc).timestamp()): "0.0305" for day in range(17, 28)}
        _write_zip(unified_csv_file, _pair_csv_files("ETHXBT", days))

        test_bars: Optional[List[HistoricalBar]] = Kraken(transaction_manifest=_manifest(), offline=True).find_historical_bars(
            "ETH", "BTC", datetime(2020, 12, 21, tzinfo=timezone.utc), True, "1w"
        )

        assert test_bars
        assert test_bars[0].timestamp == datetime(2020, 12, 21, tzinfo=timezone.utc)


class TestKrakenCsvWeeklyCandles:
    # Weekly candles are emulated from daily candles, so they start on Mondays like the weeks of the route snapshots
    @pytest.mark.parametrize("day", range(21, 28))
    def test_route_selection_starts_with_the_week_containing_the_time(self, unified_csv_file: Path, day: int) -> None:
        unified_csv_file.parent.mkdir(parents=True)
        copyfile(_UNIFIED_FIXTURE, unified_csv_file)

        # Any time from Monday 2020-12-21 to Sunday 2020-12-27
        test_bars: Optional[List[HistoricalBar]] = Kraken(transaction_manifest=_manifest(), offline=True).find_historical_bars(
            "BTC", "USD", datetime(2020, 12, day, 12, tzinfo=timezone.utc), True, "1w"
        )

        assert test_bars
        assert test_bars[0].timestamp == datetime(2020, 12, 21, tzinfo=timezone.utc)
        # Sum of the daily volumes of 2020-12-21 to 2020-12-27
        assert test_bars[0].volume == RP2Decimal("31513.74271325")

    def test_prices_never_come_from_emulated_weekly_candles(self, unified_csv_file: Path) -> None:
        # Trades on Sunday, Monday and Friday only. The weekly candle averages Monday and Friday, which isn't a price of Thursday.
        prices: Dict[int, str] = {int(datetime(2020, 12, day, tzinfo=timezone.utc).timestamp()): price for day, price in ((20, "10"), (21, "10"), (25, "20"))}
        _write_zip(unified_csv_file, _pair_csv_files("ABCUSD", prices))

        # So Thursday has no price from Kraken's CSV data, and the pair converter falls back as documented
        assert Kraken(transaction_manifest=_manifest(), offline=True).find_historical_bar("ABC", "USD", datetime(2020, 12, 24, 12, tzinfo=timezone.utc)) is None


class TestKrakenCsvChunking:
    def test_pair_whose_first_candle_starts_a_chunk_can_be_priced(self, unified_csv_file: Path) -> None:
        # Chunk files hold 30 days of 1 minute candles, starting at multiples of 30 days since the epoch: 2020-12-04 00:00 UTC is one
        chunk_start: int = 620 * 30 * 86400
        _write_zip(unified_csv_file, _pair_csv_files("ABCUSD", {chunk_start: "10", chunk_start + 60: "11"}))

        test_bar: Optional[HistoricalBar] = Kraken(transaction_manifest=_manifest(), offline=True).find_historical_bar(
            "ABC", "USD", datetime.fromtimestamp(chunk_start, timezone.utc)
        )

        assert test_bar
        assert test_bar.duration == timedelta(minutes=1)
        assert test_bar.close == RP2Decimal("10")

    def test_candles_after_a_long_gap_can_be_priced(self, unified_csv_file: Path) -> None:
        # No trades for 70 days, longer than a whole chunk file of 1 minute candles
        before_gap: int = 1600000020
        after_gap: int = before_gap + 70 * 86400
        prices: Dict[int, str] = {before_gap: "10", after_gap: "20", after_gap + 60: "21", after_gap + 120: "22"}
        _write_zip(unified_csv_file, _pair_csv_files("ABCUSD", prices))
        kraken_csv: Kraken = Kraken(transaction_manifest=_manifest(), offline=True)

        for time, price in prices.items():
            test_bar: Optional[HistoricalBar] = kraken_csv.find_historical_bar("ABC", "USD", datetime.fromtimestamp(time, timezone.utc))
            assert test_bar, time
            assert test_bar.duration == timedelta(minutes=1), time
            assert test_bar.close == RP2Decimal(price), time
