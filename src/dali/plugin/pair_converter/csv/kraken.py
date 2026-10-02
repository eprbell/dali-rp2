# Copyright 2022 macanudo527
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

# This plugin prices from the OHLCVT data Kraken publishes every quarter:
# https://support.kraken.com/hc/en-us/articles/360047124832-Downloadable-historical-OHLCVT-Open-High-Low-Close-Volume-Trades-data
# Only the CSVs of the pairs being priced are downloaded from Kraken's latest complete release (see kraken_release.py).
# If the whole release is downloaded, joined into Kraken_OHLCVT.zip and put in .dali_cache/kraken/csv/, dali-rp2 uses that file instead.

# Kraken CSV format: (epoch) timestamp, open, high, low, close, volume, trades

import logging
import zlib
from array import array
from bisect import bisect_left
from collections import OrderedDict
from csv import reader, writer
from datetime import datetime, timedelta, timezone
from gzip import open as gopen
from multiprocessing.pool import ThreadPool
from os import makedirs, path, remove, replace
from typing import IO, Dict, Generator, Iterator, List, NamedTuple, Optional, Set, Tuple, cast
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile

from requests import RequestException
from requests.sessions import Session
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError, RP2ValueError

from dali.cache import load_from_cache, save_to_cache
from dali.historical_bar import HistoricalBar
from dali.plugin.pair_converter.csv.kraken_release import KrakenRelease
from dali.plugin.pair_converter.csv.kraken_trades import KrakenTrades
from dali.transaction_manifest import TransactionManifest

# Time periods
_MS_IN_SECOND: int = 1000
_SECONDS_IN_DAY: int = 86400
_CHUNK_SIZE: int = _SECONDS_IN_DAY * 30
_SECONDS_IN_MINUTE: int = 60

# Time in minutes, used for file names
_MINUTE_IN_MINUTES: str = "1"
_FIVE_MINUTE_IN_MINUTES: str = "5"
_FIFTEEN_MINUTE_IN_MINUTES: str = "15"
_ONE_HOUR_IN_MINUTES: str = "60"
_TWELVE_HOUR_IN_MINUTES: str = "720"
_ONE_DAY_IN_MINUTES: str = "1440"
_ONE_WEEK_IN_MINUTES: str = "10080"  # Emulated
_KRAKEN_TIME_GRANULARITY: List[str] = [
    _MINUTE_IN_MINUTES,
    _FIVE_MINUTE_IN_MINUTES,
    _FIFTEEN_MINUTE_IN_MINUTES,
    _ONE_HOUR_IN_MINUTES,
    _TWELVE_HOUR_IN_MINUTES,
    _ONE_DAY_IN_MINUTES,
    _ONE_WEEK_IN_MINUTES,
]

# Time in str, which is what CCXT uses normally
# 4 hour doesn't exist in Kraken CSV. We might need to emulate it in the future
_MINUTE_IN_STR: str = "1m"
_FIVE_MINUTE_IN_STR: str = "5m"
_FIFTEEN_MINUTE_IN_STR: str = "15m"
_ONE_HOUR_IN_STR: str = "1h"
_TWELVE_HOUR_IN_STR: str = "12h"
_ONE_DAY_IN_STR: str = "1d"
_ONE_WEEK_IN_STR: str = "1w"
_CCXT_TIME_GRANULARITY: List[str] = [
    _MINUTE_IN_STR,
    _FIVE_MINUTE_IN_STR,
    _FIFTEEN_MINUTE_IN_STR,
    _ONE_HOUR_IN_STR,
    _TWELVE_HOUR_IN_STR,
    _ONE_DAY_IN_STR,
    _ONE_WEEK_IN_STR,
]


_CCXT_TIME_GRANULARITY_SET: Set[str] = set(_CCXT_TIME_GRANULARITY)

# Chunking variables
_PAIR_START: str = "start"
_PAIR_MIDDLE: str = "middle"
_PAIR_END: str = "end"
_MAX_MULTIPLIER: int = 500

# Copy chunks
_CHUNK_SIZE_BYTES: int = 32768  # 32kb

# Where the cached pairs were read from: the local unified CSV file, or else the name of a Kraken release
_LOCAL_SOURCE: str = "local unified CSV file"

# Kraken's names for the assets that CCXT, which names the assets DaLI prices, calls differently (see CCXT's commonCurrencies for Kraken).
# E.g. CCXT's LUNA is Terra 2.0, which Kraken calls LUNA2, while Kraken's LUNA is Terra Classic, which CCXT calls LUNC.
_KRAKEN_ASSETS: Dict[str, str] = {
    "BTC": "XBT",
    "DOGE": "XDG",
    "LUNA": "LUNA2",
    "LUNC": "LUNA",
    "REP": "REPV2",
    "REPV1": "REP",
    "USTC": "UST",
}

# Kraken's REST API only returns its latest 720 candles, so older prices from it come from coarse candles
_NO_REST_FALLBACK: str = "Kraken's REST API isn't accurate enough to price from instead, so try again once Kraken can be reached."
_UPDATE_FILE_NEEDS_OFFLINE: str = (
    "kraken_csv_update_file only works with kraken_csv_offline = true, because update files are merged into the local Kraken_OHLCVT.zip "
    "that offline mode uses. Without offline mode, DaLI downloads what it needs from Kraken's latest release instead."
)

DAYS_IN_WEEK: int = 7
_SECONDS_IN_WEEK: int = _SECONDS_IN_DAY * DAYS_IN_WEEK
# Emulated weekly candles start on Mondays, unlike weeks counted from the epoch, which start on Thursdays like 1970-01-01
_FIRST_MONDAY: int = _SECONDS_IN_DAY * 4


# Start of the candle of the timeframe (in minutes) that contains the timestamp
def _candle_start(timestamp: int, timeframe: str) -> int:
    if timeframe == _ONE_WEEK_IN_MINUTES:
        return timestamp - (timestamp - _FIRST_MONDAY) % _SECONDS_IN_WEEK
    return timestamp - timestamp % (int(timeframe) * _SECONDS_IN_MINUTE)


class _PairStartEnd(NamedTuple):
    end: int
    start: int


# The rows of a chunk file, which are in time order
class _Chunk:
    def __init__(self, lines: List[str]) -> None:
        self.__lines: List[str] = lines
        self.__timestamps: "array[int]" = array("q", (int(line.split(",", 1)[0]) for line in lines))

    # The row of the candle starting at timestamp, or None if there is no such candle
    def row(self, timestamp: int) -> Optional[List[str]]:
        index: int = bisect_left(self.__timestamps, timestamp)
        if index < len(self.__timestamps) and self.__timestamps[index] == timestamp:
            return self.__lines[index].split(",")
        return None

    # The rows of the candles starting at timestamp or later
    def rows_from(self, timestamp: int) -> Iterator[List[str]]:
        for line in self.__lines[bisect_left(self.__timestamps, timestamp) :]:
            yield line.split(",")


# Copies in chunks, so memory use stays flat no matter the size of the file.
# Returns the last two chunks copied, which is plenty to hold the last row of a CSV.
def _copy_stream(source: IO[bytes], destination: IO[bytes]) -> bytes:
    previous_chunk: bytes = b""
    last_chunk: bytes = b""
    while chunk := source.read(_CHUNK_SIZE_BYTES):
        destination.write(chunk)
        previous_chunk, last_chunk = last_chunk, chunk
    return previous_chunk + last_chunk


# Timestamp of the last row in the CSV data, or None if there are no rows
def _last_timestamp(csv_data: bytes) -> Optional[int]:
    last_row: bytes = csv_data.rstrip(b"\r\n").rsplit(b"\n", 1)[-1]
    return int(last_row.split(b",", 1)[0]) if last_row else None


# Copies only the CSV rows newer than timestamp (all of them if timestamp is None).
# Rows are in chronological order, so once a newer row is found the rest is copied as is.
def _copy_rows_newer_than(source: IO[bytes], destination: IO[bytes], timestamp: Optional[int]) -> None:
    if timestamp is not None:
        for row in source:
            if int(row.split(b",", 1)[0]) > timestamp:
                destination.write(row)
                break
    _copy_stream(source, destination)


# Update files are merged into the local unified CSV file, which only offline mode uses
def check_kraken_csv_options(update_file: Optional[str], offline: bool) -> None:
    if update_file is not None and not offline:
        raise RP2ValueError(_UPDATE_FILE_NEEDS_OFFLINE)


class Kraken:
    ISSUES_URL: str = "https://github.com/eprbell/dali-rp2/issues"
    __KRAKEN_OHLCVT: str = "Kraken.com_CSVOHLCVT"

    __CACHE_DIRECTORY: str = ".dali_cache/kraken/"
    __CSV_DIRECTORY: str = ".dali_cache/kraken/csv/"
    __UNIFIED_CSV_FILE: str = __CSV_DIRECTORY + "Kraken_OHLCVT.zip"
    __CACHE_KEY: str = "Kraken-csv-download"
    __SOURCE_CACHE_KEY: str = "Kraken-csv-source"

    __THREAD_COUNT: int = 3
    __CHUNKS_IN_MEMORY: int = 16

    __TIMESTAMP_INDEX: int = 0
    __OPEN: int = 1
    __HIGH: int = 2
    __LOW: int = 3
    __CLOSE: int = 4
    __VOLUME: int = 5
    __TRADES: int = 6

    __DELIMITER: str = ","

    # Online (the default), candles are downloaded from Kraken's latest release, and newer times are priced from Kraken's trades.
    # Offline, only the local unified CSV file is used, and Kraken's servers are never contacted for candles.
    def __init__(self, transaction_manifest: TransactionManifest, update_file: Optional[str] = None, offline: bool = False) -> None:
        check_kraken_csv_options(update_file, offline)
        self.__logger: logging.Logger = create_logger(self.__KRAKEN_OHLCVT)
        self.__session: Session = Session()
        self.__offline: bool = offline
        self.__cached_pairs: Dict[str, _PairStartEnd] = {}
        self.__cache_loaded: bool = False
        # Kraken's latest release, used online. None until needed.
        self.__release: Optional[KrakenRelease] = None
        # Prices times newer than Kraken's CSV data online, e.g. in the current quarter
        self.__trades: KrakenTrades = KrakenTrades(self.__session)
        # The chunk files read most recently, see __read_chunk
        self.__chunks: "OrderedDict[str, _Chunk]" = OrderedDict()

        self.__logger.debug("Assets: %s", transaction_manifest.assets)

        if not path.exists(self.__CACHE_DIRECTORY):
            makedirs(self.__CACHE_DIRECTORY)

        if not path.exists(self.__CSV_DIRECTORY):
            makedirs(self.__CSV_DIRECTORY)

        if offline and not path.exists(self.__UNIFIED_CSV_FILE):
            raise self.__missing_unified_csv_file_error()

        self.__logger.debug("Path to update file: %s", update_file)
        if update_file is not None and path.exists(update_file):
            self.__merge_update_file(update_file)

    def __missing_unified_csv_file_error(self) -> RP2RuntimeError:
        return RP2RuntimeError(
            f"kraken_csv_offline is true, but there is no {self.__UNIFIED_CSV_FILE}. Download every part of Kraken's complete OHLCVT data and join "
            "them into that file (see the docs), or turn kraken_csv_offline off to download only what's needed from Kraken."
        )

    def cache_key(self) -> str:
        return self.__CACHE_KEY

    def __load_cache(self) -> None:
        result = cast(Dict[str, _PairStartEnd], load_from_cache(self.cache_key()))
        self.__cached_pairs = result if result is not None else {}
        if self.__offline:
            return

        # Online, pairs are read from Kraken's latest release. Once Kraken publishes a new release, the pairs read from
        # the previous one (or from the local unified CSV file offline) are missing its data, so they are read again as needed.
        try:
            release: KrakenRelease = self.__get_release()
        except RP2RuntimeError as exc:
            # The cached pairs are still accurate, so they can be used. Anything else that needs Kraken's server fails later.
            self.__logger.warning("%s Using the cached Kraken data meanwhile.", exc)
            return
        if load_from_cache(self.__SOURCE_CACHE_KEY) != release.name:
            if self.__cached_pairs:
                self.__logger.info("Kraken published %s. Cached pairs will be downloaded again from it as they are needed.", release.name)
            self.__cached_pairs = {}
            save_to_cache(self.cache_key(), self.__cached_pairs)

    def __get_release(self) -> KrakenRelease:
        if self.__release is None:
            try:
                self.__release = KrakenRelease(self.__session)
            except (RequestException, RP2RuntimeError) as exc:
                raise RP2RuntimeError(f"Couldn't reach Kraken's server for its OHLCVT data ({exc}). {_NO_REST_FALLBACK}") from exc
        return self.__release

    # Splits the rows, which are in time order, into one chunk per chunk_size window of time since the epoch that has rows
    def __split_process(self, csv_file: str, chunk_size: int = _CHUNK_SIZE) -> Generator[Tuple[str, List[List[str]]], None, None]:
        chunk: List[List[str]] = []
        chunk_end: int = 0  # End of the window of the rows in chunk
        position = _PAIR_START

        for line in reader(csv_file.splitlines()):
            timestamp: int = int(line[self.__TIMESTAMP_INDEX])
            if chunk and timestamp >= chunk_end:
                yield position, chunk
                position = _PAIR_MIDDLE
                chunk = []
            if not chunk:
                # Windows without rows are skipped, so the window is the one of this row rather than the next one
                chunk_end = (timestamp // chunk_size + 1) * chunk_size
            chunk.append(line)
        if chunk:
            yield _PAIR_END, chunk

    def _split_chunks_size_n(self, file_name: str, csv_file: str, chunk_size: int = _CHUNK_SIZE) -> None:
        pair, duration_in_minutes = file_name.strip(".csv").split("_", 1)
        chunk_size *= min(int(duration_in_minutes), _MAX_MULTIPLIER)
        file_timestamp: str
        pair_start: Optional[int] = None
        pair_end: int
        pair_duration: str = pair + duration_in_minutes

        for position, chunk in self.__split_process(csv_file, chunk_size):
            file_timestamp = str((int(chunk[0][self.__TIMESTAMP_INDEX])) // chunk_size * chunk_size)
            if position == _PAIR_END:
                pair_end = int(chunk[-1][self.__TIMESTAMP_INDEX])
                if pair_start is None:
                    pair_start = int(chunk[0][self.__TIMESTAMP_INDEX])
            elif position == _PAIR_START:
                pair_start = int(chunk[0][self.__TIMESTAMP_INDEX])

            self._write_chunk_to_disk(pair, file_timestamp, duration_in_minutes, chunk)

            # Emulate 1 week candle
            if duration_in_minutes == _ONE_DAY_IN_MINUTES:
                week_chunk = []

                # Convert the first timestamp to a datetime object and find the next Monday
                first_timestamp = datetime.fromtimestamp(int(chunk[0][self.__TIMESTAMP_INDEX]), timezone.utc)
                next_monday = self._get_next_monday(first_timestamp)

                self.__logger.debug("chunking - %s, %s, %s", file_name, first_timestamp, next_monday)

                # Adjust the chunk to start from the next Monday
                adjusted_chunk = [row for row in chunk if datetime.fromtimestamp(int(row[self.__TIMESTAMP_INDEX]), timezone.utc) >= next_monday]

                i = 0
                while i < len(adjusted_chunk):

                    # When there is no volume for a day, Kraken doesn't create a row for that day
                    # So we have to find 1-7 rows that are less than or equal to a week from the start of the week
                    following_monday = next_monday + timedelta(days=7)
                    week_of_chunks = [
                        row
                        for row in adjusted_chunk[i : i + DAYS_IN_WEEK]
                        if datetime.fromtimestamp(int(row[self.__TIMESTAMP_INDEX]), timezone.utc) < following_monday
                    ]

                    # The timestamp of the first row becomes the timestamp for the weekly row
                    column_sums: List[str] = [str(int(next_monday.timestamp()))]

                    # We don't want/need to add up the timestamp column
                    for column in range(self.__OPEN, self.__TRADES + 1):
                        if len(week_of_chunks) == 0:
                            column_sums.extend(["0", "0", "0", "0", "0", "0"])
                            break

                        column_sum = str(sum((RP2Decimal(row[column]) for row in week_of_chunks), ZERO))

                        # Average all prices
                        # BUG FIX: shouldn't be averages but reflect a true candle (e.g. high should be the highest)
                        if column in range(self.__OPEN, (self.__CLOSE + 1)):
                            # Divide it by the actual number of available days
                            self.__logger.debug("column_sum: %s, len(week_of_chunks): %s", column_sum, len(week_of_chunks))
                            column_average = str(RP2Decimal(column_sum) / RP2Decimal(len(week_of_chunks)))
                            column_sums.append(column_average)
                        else:
                            column_sums.append(column_sum)
                    week_chunk.append(column_sums)
                    i += len(week_of_chunks)
                    next_monday = following_monday

                # Same file_timestamp is okay since _ONE_DAY uses _MAX_MULTIPLIER
                self._write_chunk_to_disk(pair, file_timestamp, _ONE_WEEK_IN_MINUTES, week_chunk)
                if pair_start:
                    self.__cached_pairs[pair + _ONE_WEEK_IN_MINUTES] = _PairStartEnd(start=pair_start, end=pair_end)

        if pair_start:
            self.__cached_pairs[pair_duration] = _PairStartEnd(start=pair_start, end=pair_end)

    def _write_chunk_to_disk(self, pair: str, file_timestamp: str, duration_in_minutes: str, chunk: List[List[str]]) -> None:
        chunk_filename: str = f'{pair}_{file_timestamp}_{duration_in_minutes}.{"csv.gz"}'
        chunk_filepath: str = path.join(self.__CACHE_DIRECTORY, chunk_filename)

        with gopen(chunk_filepath, "wt", encoding="utf-8", newline="") as chunk_file:
            csv_writer = writer(chunk_file)
            for row in chunk:
                csv_writer.writerow(row)

    def _retrieve_cached_bars(
        self, base_asset: str, quote_asset: str, timestamp: int, all_bars: bool = False, timespan: str = _MINUTE_IN_STR
    ) -> Optional[List[HistoricalBar]]:
        pair_name: str = base_asset + quote_asset

        if timespan in _CCXT_TIME_GRANULARITY_SET:
            retry_count: int = _CCXT_TIME_GRANULARITY.index(timespan)
        else:
            raise RP2ValueError("Internal Error: Invalid timespan passed to _retrieve_cached_bars.")

        if pair_name + _KRAKEN_TIME_GRANULARITY[retry_count] not in self.__cached_pairs:
            self.__logger.debug("No cached pair found for %s, %s", base_asset, quote_asset)
            return None

        # Prices only come from Kraken's own candles. The emulated weekly candles average the prices of a week, so they're only for picking routes.
        last_timeframe: int = len(_KRAKEN_TIME_GRANULARITY) if all_bars else len(_KRAKEN_TIME_GRANULARITY) - 1
        while retry_count < last_timeframe:
            window_start: int = self.__cached_pairs[pair_name + _KRAKEN_TIME_GRANULARITY[retry_count]].start
            window_end: int = self.__cached_pairs[pair_name + _KRAKEN_TIME_GRANULARITY[retry_count]].end

            if (timestamp < window_start or timestamp > window_end) and not all_bars:
                self.__logger.debug("Out of range - %s < %s or %s > %s", timestamp, window_start, timestamp, window_end)
                retry_count += 1
                continue

            duration_chunk_size = _CHUNK_SIZE * min(int(_KRAKEN_TIME_GRANULARITY[retry_count]), _MAX_MULTIPLIER)
            result: List[HistoricalBar] = []
            file_timestamp: int = (timestamp // duration_chunk_size) * duration_chunk_size

            # Floor the timestamp to find the price
            duration_timestamp: int = _candle_start(timestamp, _KRAKEN_TIME_GRANULARITY[retry_count])

            while file_timestamp < window_end:
                file_name: str = f"{base_asset + quote_asset}_{file_timestamp}_{_KRAKEN_TIME_GRANULARITY[retry_count]}.csv.gz"
                file_path: str = path.join(self.__CACHE_DIRECTORY, file_name)
                if all_bars:
                    self.__logger.debug(
                        "Retrieving bars for %s -> %s starting from %s from %s stamped file.", base_asset, quote_asset, duration_timestamp, file_timestamp
                    )
                else:
                    self.__logger.debug("Retrieving %s -> %s at %s from %s stamped file.", base_asset, quote_asset, duration_timestamp, file_timestamp)
                try:
                    chunk: _Chunk = self.__read_chunk(file_path)
                    if all_bars:
                        result.extend(self.__historical_bar(row, _KRAKEN_TIME_GRANULARITY[retry_count]) for row in chunk.rows_from(duration_timestamp))
                    else:
                        row: Optional[List[str]] = chunk.row(duration_timestamp)
                        if row is not None:
                            return [self.__historical_bar(row, _KRAKEN_TIME_GRANULARITY[retry_count])]
                except FileNotFoundError:
                    self.__logger.error(
                        f"No such file={file_path} (skipping) {timestamp}. Please open an issue at %s %s", self.ISSUES_URL, datetime.fromtimestamp(timestamp)
                    )

                file_timestamp = file_timestamp + duration_chunk_size if all_bars else window_end

            if result:
                return result
            retry_count += 1

        return None

    # Transactions are priced in time order, so most lookups hit a chunk file read shortly before. Keeping the most
    # recently used ones in memory saves decompressing and scanning up to a month of 1 minute candles for every price.
    def __read_chunk(self, file_path: str) -> _Chunk:
        chunk: Optional[_Chunk] = self.__chunks.get(file_path)
        if chunk is None:
            with gopen(file_path, "rt") as file:
                chunk = _Chunk(file.read().splitlines())
            self.__chunks[file_path] = chunk
            if len(self.__chunks) > self.__CHUNKS_IN_MEMORY:
                self.__chunks.popitem(last=False)
        else:
            self.__chunks.move_to_end(file_path)
        return chunk

    def __historical_bar(self, row: List[str], timeframe: str) -> HistoricalBar:
        return HistoricalBar(
            duration=timedelta(minutes=int(timeframe)),
            timestamp=datetime.fromtimestamp(int(row[self.__TIMESTAMP_INDEX]), timezone.utc),
            open=RP2Decimal(row[self.__OPEN]),
            high=RP2Decimal(row[self.__HIGH]),
            low=RP2Decimal(row[self.__LOW]),
            close=RP2Decimal(row[self.__CLOSE]),
            volume=RP2Decimal(row[self.__VOLUME]),
        )

    def find_historical_bar(self, base_asset: str, quote_asset: str, timestamp: datetime) -> Optional[HistoricalBar]:
        historical_bars: Optional[List[HistoricalBar]] = self.find_historical_bars(base_asset, quote_asset, timestamp)
        if historical_bars:
            return historical_bars[0]
        return None

    def find_historical_bars(
        self, base_asset: str, quote_asset: str, timestamp: datetime, all_bars: bool = False, timespan: str = _MINUTE_IN_STR
    ) -> Optional[List[HistoricalBar]]:
        # Kraken's CSVs and Trades endpoint use Kraken's names for assets
        base_asset = _KRAKEN_ASSETS.get(base_asset, base_asset)
        quote_asset = _KRAKEN_ASSETS.get(quote_asset, quote_asset)
        epoch_timestamp = int(timestamp.timestamp())
        self.__logger.debug("Retrieving bar for %s%s at %s", base_asset, quote_asset, epoch_timestamp)

        if not self.__cache_loaded:
            self.__logger.debug("Loading cache for Kraken CSV pair converter.")
            self.__load_cache()
            self.__cache_loaded = True

        # Picking routes only needs weekly candles, so a pair can be cached with those alone (see _unzip_and_chunk)
        pair: str = base_asset + quote_asset
        cached_timeframe: str = _ONE_WEEK_IN_MINUTES if all_bars and timespan == _ONE_WEEK_IN_STR else _MINUTE_IN_MINUTES
        if not self.__cached_pairs.get(pair + cached_timeframe) and not self._unzip_and_chunk(base_asset, quote_asset, all_bars, timespan):
            # Kraken's CSV data doesn't have the pair, e.g. because it was listed after the latest release.
            # Picking routes only needs trading volumes, so a missing market isn't an error there.
            if all_bars:
                return None
            if self.__offline:
                raise RP2RuntimeError(
                    f"Kraken's local CSV data has no {pair} market, so its prices aren't available offline. Replace {self.__UNIFIED_CSV_FILE} "
                    "with a newer release of Kraken's complete OHLCVT data, or turn kraken_csv_offline off to download what's needed from Kraken."
                )
            return self.__find_bar_in_trades(pair, epoch_timestamp)

        plural: str = "s" if all_bars else ""
        self.__logger.debug("Retrieving cached bar%s for %s, %s at %s", plural, base_asset, quote_asset, epoch_timestamp)
        bars: Optional[List[HistoricalBar]] = self._retrieve_cached_bars(base_asset, quote_asset, epoch_timestamp, all_bars, timespan)

        # Newer than Kraken's CSV data for the pair, e.g. in the current quarter
        minute_candles: Optional[_PairStartEnd] = self.__cached_pairs.get(pair + _MINUTE_IN_MINUTES)
        if bars is None and not all_bars and minute_candles is not None and epoch_timestamp > minute_candles.end:
            if self.__offline:
                raise RP2RuntimeError(
                    f"Kraken's local CSV data for {pair} ends at {datetime.fromtimestamp(minute_candles.end, timezone.utc):%Y-%m-%d %H:%M} UTC, so its "
                    f"price at {timestamp} isn't available offline. Merge Kraken's quarterly update into it with kraken_csv_update_file, or turn "
                    "kraken_csv_offline off to download newer data from Kraken."
                )
            return self.__find_bar_in_trades(pair, epoch_timestamp)
        return bars

    def __find_bar_in_trades(self, pair: str, timestamp: int) -> Optional[List[HistoricalBar]]:
        self.__logger.debug("Retrieving bar for %s at %s from Kraken's trades.", pair, timestamp)
        historical_bar: Optional[HistoricalBar] = self.__trades.find_bar(pair, timestamp)
        return [historical_bar] if historical_bar is not None else None

    def _unzip_and_chunk(self, base_asset: str, quote_asset: str, all_bars: bool = False, timespan: str = _MINUTE_IN_STR) -> bool:
        # This function was called because the trading pair hasn't been chunked yet.
        # Only the timeframes used for pricing are read. Picking routes reads the weekly candles of all the markets of an asset,
        # and those are emulated from daily candles, so that's all it needs.
        prefix: str = base_asset if all_bars else f"{base_asset}{quote_asset}_"
        timeframes: List[str] = [_ONE_DAY_IN_MINUTES] if all_bars and timespan == _ONE_WEEK_IN_STR else _KRAKEN_TIME_GRANULARITY[:-1]
        suffixes: Tuple[str, ...] = tuple(f"_{minutes}.csv" for minutes in timeframes)

        def is_needed(file_name: str) -> bool:
            return file_name.startswith(prefix) and file_name.endswith(suffixes)

        csv_files: Dict[str, str] = {}
        source: str
        if self.__offline:
            self.__logger.info("Attempting to retrieve %s%s pair from the unified Kraken CSV file.", base_asset, quote_asset)
            try:
                with ZipFile(self.__UNIFIED_CSV_FILE, "r") as zip_ref:
                    for file_name in [name for name in zip_ref.namelist() if is_needed(name)]:
                        self.__logger.debug("Reading in file %s for Kraken CSV pricing.", file_name)
                        csv_files[file_name] = zip_ref.read(file_name).decode(encoding="utf-8")
            except FileNotFoundError as exc:
                raise self.__missing_unified_csv_file_error() from exc
            except (BadZipFile, EOFError, zlib.error) as exc:
                raise RP2RuntimeError(
                    f"The unified CSV file {self.__UNIFIED_CSV_FILE} is corrupt ({exc}). Replace it with Kraken's complete OHLCVT data, or turn "
                    "kraken_csv_offline off to download only what's needed from Kraken."
                ) from exc
            source = _LOCAL_SOURCE
        else:
            release: KrakenRelease = self.__get_release()
            try:
                file_names: List[str] = [name for name in release.namelist() if is_needed(name)]
                if file_names:
                    self.__logger.info(
                        "Downloading %s candles from Kraken's %s release (%.1f MB).",
                        prefix.rstrip("_"),
                        release.name,
                        sum(release.compressed_size(file_name) for file_name in file_names) / 1e6,
                    )
                for file_name in file_names:
                    csv_files[file_name] = release.read(file_name).decode(encoding="utf-8")
            except (RequestException, RP2RuntimeError, BadZipFile, EOFError, zlib.error) as exc:
                raise RP2RuntimeError(f"Couldn't download the {prefix.rstrip('_')} candles from Kraken ({exc}). {_NO_REST_FALLBACK}") from exc
            source = release.name

        if not csv_files:
            self.__logger.debug("Market %s%s not found in Kraken files. Skipping file read.", base_asset, quote_asset)
            return False

        with ThreadPool(self.__THREAD_COUNT) as pool:
            pool.starmap(self._split_chunks_size_n, zip(list(csv_files.keys()), list(csv_files.values())))
        # Chunk files were just written, so any copies in memory may be out of date
        self.__chunks.clear()

        save_to_cache(self.cache_key(), self.__cached_pairs)
        save_to_cache(self.__SOURCE_CACHE_KEY, source)

        return True

    # Offline, Kraken's quarterly update files are merged into the local unified CSV file, so it doesn't need to be downloaded again
    def __merge_update_file(self, update_file: str) -> None:
        # Pairs chunked before the merge are missing the new data, so they need to be chunked again.
        # This is done before merging, so that a failed merge can't leave stale pairs cached.
        self.__cached_pairs = {}
        save_to_cache(self.cache_key(), self.__cached_pairs)

        self.__logger.info("Merging the update file %s into the unified CSV file. This may take a few minutes.", update_file)
        self._combine_zip_files(self.__UNIFIED_CSV_FILE, update_file)

        # Remove update file so that the merge isn't repeated
        remove(update_file)
        self.__logger.info("Merge complete. The update file %s has been deleted.", update_file)

    # Appends the CSVs in the update zip file to the ones in the unified zip file, adding any CSV that is new.
    # Update CSVs are matched by file name, even if they are in a folder, and rows the unified file already has are skipped,
    # so merging an update that overlaps the unified file, or merging the same update twice, doesn't duplicate rows.
    # The unified file is 4+ GB, so CSVs are streamed rather than loaded into memory, and the result is written to a
    # temporary file that replaces the unified file only once it's complete, so a failure can't corrupt the unified file.
    def _combine_zip_files(self, unified_zip_file: str, update_zip_file: str) -> None:
        temporary_zip_file: str = unified_zip_file + ".tmp"
        try:
            with ZipFile(unified_zip_file, "r") as unified_zip, ZipFile(update_zip_file, "r") as update_zip, ZipFile(
                temporary_zip_file, "w", ZIP_DEFLATED
            ) as combined_zip:
                # File name -> path inside the update zip file
                update_csv_files: Dict[str, str] = {path.basename(name): name for name in update_zip.namelist() if name.endswith(".csv")}

                for file_name in unified_zip.namelist():
                    with unified_zip.open(file_name) as source, combined_zip.open(file_name, "w", force_zip64=True) as destination:
                        unified_csv_tail: bytes = _copy_stream(source, destination)
                        if file_name in update_csv_files:
                            # Otherwise the last row of the unified CSV and the first row of the update would end up on the same line
                            if unified_csv_tail and not unified_csv_tail.endswith(b"\n"):
                                destination.write(b"\n")
                            with update_zip.open(update_csv_files[file_name]) as update_source:
                                _copy_rows_newer_than(update_source, destination, _last_timestamp(unified_csv_tail))

                for file_name in sorted(update_csv_files.keys() - set(unified_zip.namelist())):
                    with update_zip.open(update_csv_files[file_name]) as source, combined_zip.open(file_name, "w", force_zip64=True) as destination:
                        _copy_stream(source, destination)

            replace(temporary_zip_file, unified_zip_file)
        finally:
            # Only exists if something went wrong before the unified file was replaced
            if path.exists(temporary_zip_file):
                remove(temporary_zip_file)

    def _get_next_monday(self, date: datetime) -> datetime:
        days_ahead = (DAYS_IN_WEEK - date.weekday()) % DAYS_IN_WEEK
        if days_ahead == 0:
            days_ahead = DAYS_IN_WEEK
        return date + timedelta(days=days_ahead)
