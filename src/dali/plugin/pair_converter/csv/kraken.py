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

# Kraken CSV format: (epoch) timestamp, open, high, low, close, volume, trades

import logging
from csv import reader, writer
from datetime import datetime, timedelta, timezone
from gzip import open as gopen
from io import BytesIO
from json import JSONDecodeError
from multiprocessing.pool import ThreadPool
from os import makedirs, path
from time import sleep
from typing import Any, Dict, Generator, List, NamedTuple, Optional, Set, Tuple, cast
from zipfile import BadZipFile, ZipFile

import requests
from requests.models import Response
from requests.sessions import Session
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError, RP2ValueError

from dali.cache import load_from_cache, save_to_cache
from dali.historical_bar import HistoricalBar

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

# File ID for the folder which contains zipped OHLCVT files grouped by asset
# In Google Drive lingo, this is the 'parent' of the files we need.
# Files in this folder will be updated every quarter and thus will have new file IDs.
# However, the file ID for the folder or parent should remain the same.
_KRAKEN_FOLDER_ID: str = "1aoA6SKgPbS_p3pYStXUXFvmjqShJ2jv9"
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


class _PairStartEnd(NamedTuple):
    end: int
    start: int


class Kraken:
    ISSUES_URL: str = "https://github.com/eprbell/dali-rp2/issues"
    __KRAKEN_OHLCVT: str = "Kraken.com_CSVOHLCVT"

    __CACHE_DIRECTORY: str = ".dali_cache/kraken/"
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

    def __init__(
        self,
        google_api_key: str,
    ) -> None:
        self.__google_api_key: str = google_api_key
        self.__logger: logging.Logger = create_logger(self.__KRAKEN_OHLCVT)
        self.__session: Session = requests.Session()
        self.__cached_pairs: Dict[str, _PairStartEnd] = {}
        self.__cache_loaded: bool = False

        if not path.exists(self.__CACHE_DIRECTORY):
            makedirs(self.__CACHE_DIRECTORY)

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

                for i in range(0, len(chunk), 7):
                    seven_chunks = chunk[i : i + 7]

                    # We want to make sure we have a full week for the averages and accurate pricing
                    if len(seven_chunks) < 7:
                        break

                    # The timestamp of the first row becomes the timestamp for the weekly row
                    column_sums: List[str] = [seven_chunks[0][self.__TIMESTAMP_INDEX]]

                    # We don't want/need to add up the timestamp column
                    for column in range(self.__OPEN, self.__TRADES + 1):
                        column_sum = str(sum((RP2Decimal(row[column]) for row in seven_chunks), ZERO))

                        # Average all prices
                        # BUG FIX: shouldn't be averages but reflect a true candle (e.g. high should be the highest)
                        if column in range(self.__OPEN, (self.__CLOSE + 1)):
                            column_average = str(RP2Decimal(column_sum) / RP2Decimal("7"))
                            column_sums.append(column_average)
                        else:
                            column_sums.append(column_sum)
                    week_chunk.append(column_sums)

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

        while retry_count < len(_KRAKEN_TIME_GRANULARITY):
            window_start: int = self.__cached_pairs[pair_name + _KRAKEN_TIME_GRANULARITY[retry_count]].start
            window_end: int = self.__cached_pairs[pair_name + _KRAKEN_TIME_GRANULARITY[retry_count]].end

            if timestamp < window_start or timestamp > window_end:
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

                file_timestamp += duration_chunk_size

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
            self.__logger.debug("Retrieving cached bar for %s, %s at %s", base_asset, quote_asset, epoch_timestamp)
            return self._retrieve_cached_bars(base_asset, quote_asset, epoch_timestamp, all_bars, timespan)

        if self._download_and_chunk(base_asset, quote_asset, all_bars):
            return self._retrieve_cached_bars(base_asset, quote_asset, epoch_timestamp, all_bars, timespan)

        return None

    def _download_and_chunk(self, base_asset: str, quote_asset: str, all_bars: bool = False) -> bool:
        base_file: str = f"{base_asset}_OHLCVT.zip"

        self.__logger.info("Attempting to load %s from Kraken Google Drive.", base_file)
        file_bytes = self._google_file_to_bytes(base_file)

        if file_bytes:
            with ZipFile(BytesIO(file_bytes)) as zipped_ohlcvt:
                self.__logger.debug("Files found in zipped file - %s", zipped_ohlcvt.namelist())
                all_timespans_for_pair: List[str]
                if all_bars:
                    all_timespans_for_pair = zipped_ohlcvt.namelist()
                else:
                    all_timespans_for_pair = [x for x in zipped_ohlcvt.namelist() if x.startswith(f"{base_asset}{quote_asset}_")]
                if len(all_timespans_for_pair) == 0:
                    self.__logger.debug("Market not found in Kraken files. Skipping file read.")
                    return False

                csv_files: Dict[str, str] = {}
                for file_name in all_timespans_for_pair:
                    self.__logger.debug("Reading in file %s for Kraken CSV pricing.", file_name)
                    csv_files[file_name] = zipped_ohlcvt.read(file_name).decode(encoding="utf-8")

                with ThreadPool(self.__THREAD_COUNT) as pool:
                    pool.starmap(self._split_chunks_size_n, zip(list(csv_files.keys()), list(csv_files.values())))

            save_to_cache(self.cache_key(), self.__cached_pairs)
            return True

        return False

    # isolated in order to be mocked
    def _google_file_to_bytes(self, file_name: str) -> Optional[bytes]:
        params: Dict[str, Any] = {
            _QUERY: f"'{_KRAKEN_FOLDER_ID}' in parents and name = '{file_name}'",
            _API_KEY: self.__google_api_key,
        }
        try:
            # Searching the Kraken folder for the specific file for the asset we are interested in
            # This query returns a JSON with the file ID we need to download the specific file we need.
            response: Response = self.__session.get(_GOOGLE_APIS_URL, params=params, timeout=self.__TIMEOUT)
            # {
            #  "kind": "drive#fileList",
            #  "incompleteSearch": false,
            #  "files": [
            #   {
            #    "kind": "drive#file",
            #    "id": "1AAWkwfxJjOvZQKv3c5XOH1ZjoIQMblQt",
            #    "name": "USDT_OHLCVT.zip",
            #    "mimeType": "application/zip"
            #   }
            #  ]
            # }

            ## Error Response - Key is invalid
            # {
            #  'error': {
            #    'code': 400,
            #    'message': 'API key not valid. Please pass a valid API key.',
            #    'errors': [
            #      {
            #        'message': 'API key not valid. Please pass a valid API key.',
            #        'domain': 'global',
            #        'reason': 'badRequest'
            #      }
            #    ],
            #    'status': 'INVALID_ARGUMENT',
            #    'details': [
            #      {
            #        '@type': 'type.googleapis.com/google.rpc.ErrorInfo',
            #        'reason': 'API_KEY_INVALID',
            #        'domain': 'googleapis.com',
            #        'metadata': {
            #          'service': 'drive.googleapis.com'
            #        }
            #      }
            #    ]
            #  }
            # } from https://www.googleapis.com/drive/v3/files?q=...

            ## Error Response - Key is valid but Google Drive API is specifically not configured
            # {
            #  'error': {
            #   'errors': [
            #    {
            #     'domain': 'usageLimits',
            #     'reason': 'accessNotConfigured',
            #     'message': 'Access Not Configured...',
            #     'extendedHelp': 'https://console.developers.google.com/apis/api/drive.googleapis.com/overview?project=728279122903'
            #    }
            #   ],
            #   'code': 403,
            #   'message': 'Access Not Configured...'
            #  }
            # }
            data: Any = response.json()

            error_json: Any = data.get(_ERROR)

            if error_json is not None:
                errors: Any = error_json[_ERRORS]
                for error in errors:
                    if error[_REASON] == _ACCESS_NOT_CONFIGURED:
                        self.__logger.error(
                            "Access not granted to Google Drive API. You must grant authorization to the Google"
                            "Drive API for your API key. Follow the link in the message for more details. Message:\n%s",
                            error[_MESSAGE],
                        )
                        raise RP2RuntimeError("Google Drive not authorized")
                    if error[_REASON] == _BAD_REQUEST:
                        self.__logger.error(
                            "Google API key is invalid. Please check that you have entered the key correctly and try again."
                            "If the problem persists, you can leave the field blank to use the REST API.\n%s",
                            error[_MESSAGE],
                        )
                        raise RP2RuntimeError("Google Drive key invalid")
                    if error[_REASON] == _INVALID_VALUE:
                        self.__logger.error(
                            """Invalid parameters to google API call.\nparams=%s\nMessage=%s\n
                            """,
                            params,
                            error[_MESSAGE],
                        )
                        raise RP2RuntimeError("Google Drive not authorized")

            if not data.get(_FILES):
                self.__logger.error("No matching files for '%s' on the Kraken Google Drive. data=%s", file_name, data)
                return None

            self.__logger.debug("Retrieved %s from %s", data, response.url)
            retry_count: int = 0

            while True:
                try:
                    # Downloading the zipfile that contains the 6 files one for each of the standard durations of candles:
                    # 1m, 5m, 15m, 1h, 12h, 24h.
                    params = {_ALT: _MEDIA, _API_KEY: self.__google_api_key, _CONFIRM: 1}  # _CONFIRM: 1 bypasses large file warning
                    file_response: Response = self.__session.get(f"{_GOOGLE_APIS_URL}/{data[_FILES][0][_ID]}", params=params, timeout=self.__TIMEOUT)
                    with ZipFile(BytesIO(file_response.content)) as file_check:
                        if file_check.testzip() is None:
                            break
                        retry_count += 1
                    if retry_count > 2:
                        raise RP2RuntimeError(f"Invalid zipfile - {file_name}. Giving up. Try again later.")

                # This is probably caused by too many requests to Google Drive in a short period of time.
                except BadZipFile:
                    self.__logger.info("Bad zip file - %s, trying again after a minute.", file_name)
                    sleep(60)
                    retry_count += 1
                    if retry_count > 5:
                        raise RP2RuntimeError(f"Too many retries for {file_name}. Giving up. Try again later.")



        except JSONDecodeError as exc:
            self.__logger.debug("Fetching of kraken csv files failed. Try again later.")
            raise RP2RuntimeError("JSON decode error") from exc

        return file_response.content
