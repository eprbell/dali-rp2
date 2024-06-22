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

# This plugin facilitates the downloading of the unified CSV located at
# https://drive.google.com/file/d/16YKyFkYlvawCHv3W7WuTFzM8RYgMRWMt/view?usp=sharing
# This link will have to be updated quarterly when Kraken releases a new file.
# Note that you can manually download the unified file as Kraken_OHLCVT.zip
# to the .dali_cache/kraken/csv/ and dali-rp2 will use that file.
# For more information on this file visit the following link:
# https://support.kraken.com/hc/en-us/articles/360047124832-Downloadable-historical-OHLCVT-Open-High-Low-Close-Volume-Trades-data

# Kraken CSV format: (epoch) timestamp, open, high, low, close, volume, trades

import logging
import re
from csv import reader, writer
from datetime import datetime, timedelta, timezone
from gzip import open as gopen
from json import JSONDecodeError
from multiprocessing.pool import ThreadPool
from os import makedirs, path, remove
from typing import Dict, Generator, List, NamedTuple, Optional, Set, Tuple, cast
from zipfile import BadZipFile, ZipFile, is_zipfile

import requests
from progressbar import ProgressBar, UnknownLength
from progressbar.widgets import AdaptiveTransferSpeed, BouncingBar, DataSize
from requests.sessions import Session
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError, RP2ValueError

from dali.cache import load_from_cache, save_to_cache
from dali.historical_bar import HistoricalBar
from dali.transaction_manifest import TransactionManifest

# Google Drive parameters
_ACCESS_NOT_CONFIGURED: str = "accessNotConfigured"
_BAD_REQUEST: str = "badRequest"
_INVALID_VALUE: str = "invalid"
_ERROR: str = "error"
_ERRORS: str = "errors"
_FILES: str = "files"

# The endpoint we will use to query Google Drive for the specific file we need
# We will also use this to request a file download.
_GOOGLE_APIS_URL: str = "https://www.googleapis.com/drive/v3/files"
_ID: str = "id"

# The URL for downloading the actual file from Google Drive with security params
_GOOGLE_DRIVE_DOWNLOAD_URL: str = "https://drive.usercontent.google.com/download"

# File ID for the unified CSV file ID. This will need to be replaced every quarter.
# File can also be manually downloaded from https://drive.google.com/file/d/11WtjXA9kvVYV9KDoebGV5U75dmcA3bJa/view?usp=sharing
_UNIFIED_CSV_FILE_ID: str = "11WtjXA9kvVYV9KDoebGV5U75dmcA3bJa"
_MESSAGE: str = "message"
_REASON: str = "reason"

# Google Drive URL Params
_ALT: str = "alt"
_API_KEY: str = "key"
_CONFIRM: str = "confirm"
_MEDIA: str = "media"
_QUERY: str = "q"

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

# Download chunks
_CHUNK_SIZE_BYTES: int = 32768  # 32kb

DAYS_IN_WEEK: int = 7

class _PairStartEnd(NamedTuple):
    end: int
    start: int


class Kraken:
    ISSUES_URL: str = "https://github.com/eprbell/dali-rp2/issues"
    DEFAULT_TIMEOUT: int = 10
    __KRAKEN_OHLCVT: str = "Kraken.com_CSVOHLCVT"

    __CACHE_DIRECTORY: str = ".dali_cache/kraken/"
    __CSV_DIRECTORY: str = ".dali_cache/kraken/csv/"
    __UNIFIED_CSV_FILE: str = __CSV_DIRECTORY + "Kraken_OHLCVT.zip"
    __CACHE_KEY: str = "Kraken-csv-download"

    __TIMEOUT: int = 30
    __THREAD_COUNT: int = 3

    __TIMESTAMP_INDEX: int = 0
    __OPEN: int = 1
    __HIGH: int = 2
    __LOW: int = 3
    __CLOSE: int = 4
    __VOLUME: int = 5
    __TRADES: int = 6

    __DELIMITER: str = ","

    def __init__(self, transaction_manifest: TransactionManifest, force_download: bool = False) -> None:
        self.__logger: logging.Logger = create_logger(self.__KRAKEN_OHLCVT)
        self.__session: Session = requests.Session()
        self.__cached_pairs: Dict[str, _PairStartEnd] = {}
        self.__cache_loaded: bool = False
        self.__force_download: bool = force_download
        self.__unchunked_assets: Set[str] = transaction_manifest.assets

        self.__logger.debug("Assets: %s", self.__unchunked_assets)

        if not path.exists(self.__CACHE_DIRECTORY):
            makedirs(self.__CACHE_DIRECTORY)

        if not path.exists(self.__CSV_DIRECTORY):
            makedirs(self.__CSV_DIRECTORY)

    def cache_key(self) -> str:
        return self.__CACHE_KEY

    def __load_cache(self) -> None:
        result = cast(Dict[str, _PairStartEnd], load_from_cache(self.cache_key()))
        self.__cached_pairs = result if result is not None else {}

    def __split_process(self, csv_file: str, chunk_size: int = _CHUNK_SIZE) -> Generator[Tuple[str, List[List[str]]], None, None]:
        chunk: List[List[str]] = []

        lines = reader(csv_file.splitlines())
        position = _PAIR_START
        next_timestamp: Optional[int] = None

        for line in lines:
            if next_timestamp is None:
                next_timestamp = ((int(line[self.__TIMESTAMP_INDEX]) + chunk_size) // chunk_size) * chunk_size

            if int(line[self.__TIMESTAMP_INDEX]) % chunk_size == 0 or int(line[self.__TIMESTAMP_INDEX]) > next_timestamp:
                yield position, chunk
                if position == _PAIR_START:
                    position = _PAIR_MIDDLE
                chunk = []
                next_timestamp += chunk_size
            chunk.append(line)
        if chunk:
            position = _PAIR_END
            yield position, chunk

    def __download_unified_csv(self) -> None:
        try:
            retry_count = 0
            while True:
                # Downloading the unified zipfile that contains all the trading pairs
                response = self.__session.get("https://docs.google.com/uc?export=download&confirm=1", params={"id": _UNIFIED_CSV_FILE_ID}, stream=True)

                # Use response.text instead of response.content since an html form will be returned that we need to grab strings from.
                html_content = response.text

                # The unified file is large (3.9gig+), so Google Drive will warn us that it can not automatically scan it for viruses.
                # Embedded in this warning is a hidden form with an id, export, confirm, and uuid tokens to submit in order to override the warning.
                # First we harvest the tokens.
                if "Google Drive - Virus scan warning" in html_content:
                    # Extract the required parameters using regular expressions
                    id_match = re.search(r'name="id"\s+value="([^"]+)"', html_content)
                    export_match = re.search(r'name="export"\s+value="([^"]+)"', html_content)
                    confirm_match = re.search(r'name="confirm"\s+value="([^"]+)"', html_content)
                    uuid_match = re.search(r'name="uuid"\s+value="([^"]+)"', html_content)

                    # Confirm they exist. This is a sanity check to verify the process has remained the same.
                    if id_match and export_match and confirm_match and uuid_match:
                        file_id = id_match.group(1)
                        export = export_match.group(1)
                        confirm = confirm_match.group(1)
                        uuid = uuid_match.group(1)
                    else:
                        raise ValueError("Failed to extract parameters from HTML")

                    # Set up the parameters for the download
                    params = {"id": file_id, "export": export, "confirm": confirm, "uuid": uuid}
                    query_string = "&".join(f"{key}={value}" for key, value in params.items())

                    # Make the request and download the file using the params harvested earlier
                    response = requests.get(_GOOGLE_DRIVE_DOWNLOAD_URL, params=params, stream=True, timeout=self.DEFAULT_TIMEOUT)
                    self.__logger.info("Downloading the unified CSV from %s?%s", _GOOGLE_DRIVE_DOWNLOAD_URL, query_string)

                with open(self.__UNIFIED_CSV_FILE, "wb") as file, ProgressBar(
                    max_value=UnknownLength, widgets=["Downloading: ", BouncingBar(), " ", DataSize(), " ", AdaptiveTransferSpeed()]
                ) as progress_bar:
                    progress_bar.start()
                    for chunk in response.iter_content(_CHUNK_SIZE_BYTES):
                        if chunk:  # Filter out keep-alive new chunks
                            file.write(chunk)
                            progress_bar.update(progress_bar.value + len(chunk))
                    progress_bar.finish()

                if is_zipfile(self.__UNIFIED_CSV_FILE):
                    break
                retry_count += 1
                if retry_count > 2:
                    raise RP2RuntimeError("Invalid zipfile. Giving up. Try again later.")
                self._remove_unified_csv_file()
                self.__logger.info("Downloaded file is invalid, trying to download again.")

        except JSONDecodeError as exc:
            self.__logger.debug("Fetching of kraken csv files failed. Try again later.")
            raise RP2RuntimeError("JSON decode error") from exc

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

        while retry_count < len(_KRAKEN_TIME_GRANULARITY):
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
            duration_timestamp: int = (timestamp // (int(_KRAKEN_TIME_GRANULARITY[retry_count]) * _SECONDS_IN_MINUTE)) * (
                int(_KRAKEN_TIME_GRANULARITY[retry_count]) * _SECONDS_IN_MINUTE
            )

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
                    with gopen(file_path, "rt") as file:
                        rows = reader(file)
                        for row in rows:
                            if all_bars and int(row[self.__TIMESTAMP_INDEX]) >= duration_timestamp:
                                result.append(
                                    HistoricalBar(
                                        duration=timedelta(minutes=int(_KRAKEN_TIME_GRANULARITY[retry_count])),
                                        timestamp=datetime.fromtimestamp(int(row[self.__TIMESTAMP_INDEX]), timezone.utc),
                                        open=RP2Decimal(row[self.__OPEN]),
                                        high=RP2Decimal(row[self.__HIGH]),
                                        low=RP2Decimal(row[self.__LOW]),
                                        close=RP2Decimal(row[self.__CLOSE]),
                                        volume=RP2Decimal(row[self.__VOLUME]),
                                    )
                                )
                            elif int(row[self.__TIMESTAMP_INDEX]) == duration_timestamp:
                                return [
                                    HistoricalBar(
                                        duration=timedelta(minutes=int(_KRAKEN_TIME_GRANULARITY[retry_count])),
                                        timestamp=datetime.fromtimestamp(int(row[self.__TIMESTAMP_INDEX]), timezone.utc),
                                        open=RP2Decimal(row[self.__OPEN]),
                                        high=RP2Decimal(row[self.__HIGH]),
                                        low=RP2Decimal(row[self.__LOW]),
                                        close=RP2Decimal(row[self.__CLOSE]),
                                        volume=RP2Decimal(row[self.__VOLUME]),
                                    )
                                ]
                except FileNotFoundError:
                    self.__logger.error(
                        f"No such file={file_path} (skipping) {timestamp}. Please open an issue at %s %s", self.ISSUES_URL, datetime.fromtimestamp(timestamp)
                    )

                file_timestamp = file_timestamp + duration_chunk_size if all_bars else window_end

            if result:
                return result
            retry_count += 1

        return None

    def find_historical_bar(self, base_asset: str, quote_asset: str, timestamp: datetime) -> Optional[HistoricalBar]:
        historical_bars: Optional[List[HistoricalBar]] = self.find_historical_bars(base_asset, quote_asset, timestamp)
        if historical_bars:
            return historical_bars[0]
        return None

    def find_historical_bars(
        self, base_asset: str, quote_asset: str, timestamp: datetime, all_bars: bool = False, timespan: str = _MINUTE_IN_STR
    ) -> Optional[List[HistoricalBar]]:
        # Kraken refers to BTC as XBT only on it's API
        if base_asset == "BTC":
            base_asset = "XBT"
        epoch_timestamp = int(timestamp.timestamp())
        self.__logger.debug("Retrieving bar for %s%s at %s", base_asset, quote_asset, epoch_timestamp)

        if not self.__cache_loaded:
            self.__logger.debug("Loading cache for Kraken CSV pair converter.")
            self.__load_cache()
            self.__cache_loaded = True

        # Attempt to load smallest duration
        if self.__cached_pairs.get(base_asset + quote_asset + _MINUTE_IN_MINUTES):
            plural: str = "s" if all_bars else ""
            self.__logger.debug("Retrieving cached bar%s for %s, %s at %s", plural, base_asset, quote_asset, epoch_timestamp)
            return self._retrieve_cached_bars(base_asset, quote_asset, epoch_timestamp, all_bars, timespan)

        if self._unzip_and_chunk(base_asset, quote_asset, all_bars):
            return self._retrieve_cached_bars(base_asset, quote_asset, epoch_timestamp, all_bars, timespan)

        return None

    def _unzip_and_chunk(self, base_asset: str, quote_asset: str, all_bars: bool = False) -> bool:
        # This function was called because the trading pair hasn't been chunked yet.
        # In order to chunk a new trading pair, we need the unified CSV file.
        if not path.exists(self.__UNIFIED_CSV_FILE) and (self.__force_download or self._prompt_download_confirmation()):
            self.__download_unified_csv()

        self.__logger.info("Attempting to retrieve %s%s pair from the unified Kraken CSV file.", base_asset, quote_asset)
        successful = False
        for _ in range(2):
            try:
                with ZipFile(self.__UNIFIED_CSV_FILE, "r") as zip_ref:
                    all_timespans_for_pair: List[str]
                    if all_bars:
                        all_timespans_for_pair = [x for x in zip_ref.namelist() if x.startswith(f"{base_asset}")]
                    else:
                        all_timespans_for_pair = [x for x in zip_ref.namelist() if x.startswith(f"{base_asset}{quote_asset}_")]

                    self.__logger.debug("Chunking: %s", all_timespans_for_pair)

                    if all_timespans_for_pair:
                        csv_files: Dict[str, str] = {}
                        for file_name in all_timespans_for_pair:
                            self.__logger.debug("Reading in file %s for Kraken CSV pricing.", file_name)
                            csv_files[file_name] = zip_ref.read(file_name).decode(encoding="utf-8")

                        with ThreadPool(self.__THREAD_COUNT) as pool:
                            pool.starmap(self._split_chunks_size_n, zip(list(csv_files.keys()), list(csv_files.values())))
                        successful = True
                        break
                    self.__logger.debug("Market %s%s not found in Kraken files. Skipping file read.", base_asset, quote_asset)
                    return False
            except BadZipFile:
                self.__logger.info("Corrupt unified CSV file found, deleting and trying again.")
                self._remove_unified_csv_file()
                if self.__force_download or self._prompt_download_confirmation():
                    self.__download_unified_csv()

        if not successful:
            raise RP2RuntimeError("CSV file is either corrupt or not available. Giving up.")

        save_to_cache(self.cache_key(), self.__cached_pairs)
        self.__unchunked_assets.discard(base_asset)
        self.__logger.debug("Leftover assets: %s", self.__unchunked_assets)
        if len(self.__unchunked_assets) == 0 and self._prompt_delete_confirmation():
            self._remove_unified_csv_file()

        return True

    def _prompt_download_confirmation(self) -> bool:
        self.__logger.info("\nIn order to provide accurate pricing from Kraken, a large (4.1+ gb) zipfile needs to be downloaded.")

        while True:
            choice = input("Do you want to download the file now?[yn]")
            if choice == "y":
                return True
            if choice == "n":
                return False
            self.__logger.info("Invalid choice. Please enter y or n.")

    def _prompt_delete_confirmation(self) -> bool:
        self.__logger.info(
            "\nAll of the CSV files for your assets have been processed. You can probably safely delete the master CSV file "
            "located at %s. However, if you add assets later, you will need to re-download the file.",
            self.__UNIFIED_CSV_FILE,
        )

        while True:
            choice = input("Do you want to delete the file now?[yn]")
            if choice == "y":
                return True
            if choice == "n":
                return False
            self.__logger.info("Invalid choice. Please enter y or n.")

    def _remove_unified_csv_file(self) -> None:
        try:
            remove(self.__UNIFIED_CSV_FILE)
            self.__logger.info("%s has been safely deleted.", self.__UNIFIED_CSV_FILE)
        except FileNotFoundError:
            self.__logger.info("File %s not found.", self.__UNIFIED_CSV_FILE)

    def _get_next_monday(self, date: datetime) -> datetime:
        days_ahead = (DAYS_IN_WEEK - date.weekday()) % DAYS_IN_WEEK
        if days_ahead == 0:
            days_ahead = DAYS_IN_WEEK
        return date + timedelta(days=days_ahead)
