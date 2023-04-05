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
from typing import Any, Dict, Generator, List, NamedTuple, Optional, Tuple, cast, Union
from zipfile import ZipFile

import requests
from requests.models import Response
from requests.sessions import Session

from ccxt import kraken
from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2RuntimeError

from dali.plugin.input.rest.kraken import (
    _BASE,
    _KRAKEN_FIAT_LIST,
)
from dali.cache import load_from_cache, save_to_cache
from dali.historical_bar import HistoricalBar

# Kraken-Dali base id keys
_ALTNAME: str = 'altname'
_PAIRS: str = 'pairs'
_GOOGLE_ABBR: str = 'google_abbr'

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

# Time in minutes
_MINUTE: str = "1"
_FIVE_MINUTE: str = "5"
_FIFTEEN_MINUTE: str = "15"
_ONE_HOUR: str = "60"
_TWELVE_HOUR: str = "720"
_ONE_DAY: str = "1440"
_TIME_GRANULARITY: List[str] = [_MINUTE, _FIVE_MINUTE, _FIFTEEN_MINUTE, _ONE_HOUR, _TWELVE_HOUR, _ONE_DAY]

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
        self._kraken: kraken = kraken({"enableRateLimit": True})
        self.dali_base_to_kraken_google_base: Dict[str, Dict[str, Union[List[str], str]]] = {}
        self.kraken_google_base_to_dali_base: Dict[str, Dict[str, Union[List[str], str]]] = {}
        self.initialize_markets()

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

            chunk_filename: str = f'{pair}_{file_timestamp}_{duration_in_minutes}.{"csv.gz"}'
            chunk_filepath: str = path.join(self.__CACHE_DIRECTORY, chunk_filename)

            with gopen(chunk_filepath, "wt", encoding="utf-8", newline="") as chunk_file:
                csv_writer = writer(chunk_file)
                for row in chunk:
                    csv_writer.writerow(row)

        if pair_start:
            self.__cached_pairs[pair_duration] = _PairStartEnd(start=pair_start, end=pair_end)

    def _retrieve_cached_bar(self, base_asset: str, quote_asset: str, timestamp: int) -> Optional[HistoricalBar]:
        pair_name: str = base_asset + quote_asset

        retry_count: int = 0

        while retry_count < len(_TIME_GRANULARITY):
            if (
                timestamp < self.__cached_pairs[pair_name + _TIME_GRANULARITY[retry_count]].start
                or timestamp > self.__cached_pairs[pair_name + _TIME_GRANULARITY[retry_count]].end
            ):
                self.__logger.debug(
                    "Out of range - %s < %s or %s > %s",
                    timestamp,
                    self.__cached_pairs[pair_name + _TIME_GRANULARITY[retry_count]].start,
                    timestamp,
                    self.__cached_pairs[pair_name + _TIME_GRANULARITY[retry_count]].end,
                )
                retry_count += 1
                continue

            duration_chunk_size = _CHUNK_SIZE * min(int(_TIME_GRANULARITY[retry_count]), _MAX_MULTIPLIER)
            file_timestamp: int = (timestamp // duration_chunk_size) * duration_chunk_size

            # Floor the timestamp to find the price
            duration_timestamp: int = (timestamp // (int(_TIME_GRANULARITY[retry_count]) * _SECONDS_IN_MINUTE)) * (
                int(_TIME_GRANULARITY[retry_count]) * _SECONDS_IN_MINUTE
            )

            file_name: str = f"{base_asset + quote_asset}_{file_timestamp}_{_TIME_GRANULARITY[retry_count]}.csv.gz"
            file_path: str = path.join(self.__CACHE_DIRECTORY, file_name)
            self.__logger.debug("Retrieving %s -> %s at %s from %s stamped file.", base_asset, quote_asset, duration_timestamp, file_timestamp)
            with gopen(file_path, "rt") as file:
                rows = reader(file)
                for row in rows:
                    if int(row[self.__TIMESTAMP_INDEX]) == duration_timestamp:
                        return HistoricalBar(
                            duration=timedelta(minutes=int(_TIME_GRANULARITY[retry_count])),
                            timestamp=datetime.fromtimestamp(int(row[self.__TIMESTAMP_INDEX]), timezone.utc),
                            open=RP2Decimal(row[self.__OPEN]),
                            high=RP2Decimal(row[self.__HIGH]),
                            low=RP2Decimal(row[self.__LOW]),
                            close=RP2Decimal(row[self.__CLOSE]),
                            volume=RP2Decimal(row[self.__VOLUME]),
                        )

            retry_count += 1

        return None

    def find_historical_bar(self, base_asset: str, quote_asset: str, timestamp: datetime) -> Optional[HistoricalBar]:
        base_asset = str(self.dali_base_to_kraken_google_base[base_asset][_GOOGLE_ABBR])
        epoch_timestamp = int(timestamp.timestamp())
        self.__logger.debug("Retrieving bar for %s%s at %s", base_asset, quote_asset, epoch_timestamp)

        if not self.__cache_loaded:
            self.__logger.debug("Loading cache for Kraken CSV pair converter.")
            self.__load_cache()

        # Attempt to load smallest duration
        if self.__cached_pairs.get(base_asset + quote_asset + "1"):
            self.__logger.debug("Retrieving cached bar for %s, %s at %s", base_asset, quote_asset, epoch_timestamp)
            return self._retrieve_cached_bar(base_asset, quote_asset, epoch_timestamp)

        base_file: str = f"{base_asset}_OHLCVT.zip"

        self.__logger.info("Attempting to load %s from Kraken Google Drive.", base_file)
        file_bytes = self._google_file_to_bytes(base_file)

        if file_bytes:
            with ZipFile(BytesIO(file_bytes)) as zipped_ohlcvt:
                self.__logger.debug("Files found in zipped file - %s", zipped_ohlcvt.namelist())
                all_timespans_for_pair: List[str] = [x for x in zipped_ohlcvt.namelist() if x.startswith(f"{base_asset}{quote_asset}_")]
                if len(all_timespans_for_pair) == 0:
                    self.__logger.debug("Market not found in Kraken files. Skipping file read.")
                    return None

                csv_files: Dict[str, str] = {}
                for file_name in all_timespans_for_pair:
                    self.__logger.debug("Reading in file %s for Kraken CSV pricing.", file_name)
                    csv_files[file_name] = zipped_ohlcvt.read(file_name).decode(encoding="utf-8")

                with ThreadPool(self.__THREAD_COUNT) as pool:
                    pool.starmap(self._split_chunks_size_n, zip(list(csv_files.keys()), list(csv_files.values())))

            save_to_cache(self.cache_key(), self.__cached_pairs)
            return self._retrieve_cached_bar(base_asset, quote_asset, epoch_timestamp)

        return None

    # isolated in order to be mocked
    def _google_file_to_bytes(self, file_name: str) -> Optional[bytes]:
        params: Dict[str, Any] = {
            _QUERY: f"'{_KRAKEN_FOLDER_ID}' in parents and name = '{file_name}'",
            _API_KEY: self.__google_api_key,
        }
        query_google_result, data = self._query_google_drive(params)

        if not query_google_result:
            self.__logger.error(f"File name doesn't exist: {file_name} (skipping): data={data}. "
                                f"Please open an issue at %s", self.ISSUES_URL)
            return None

        # Downloading the zipfile that contains the 6 files one for each of the standard durations of candles:
        # 1m, 5m, 15m, 1h, 12h, 24h.
        params = {_ALT: _MEDIA, _API_KEY: self.__google_api_key, _CONFIRM: 1}  # _CONFIRM: 1 bypasses large file warning
        file_response: Response = self.__session.get(f"{_GOOGLE_APIS_URL}/{data[_FILES][0][_ID]}", params=params, timeout=self.__TIMEOUT)

        return file_response.content

    def _query_google_drive(self, params: Dict[str, Any]) -> Tuple[bool, Any]:
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
                self.__logger.error("No matching files were found on the Kraken Google Drive. data=%s", data)
                return False, data

            self.__logger.debug("Retrieved %s from %s", data, response.url)
        except JSONDecodeError as exc:
            self.__logger.debug("Fetching of kraken csv files failed. Try again later.")
            raise RP2RuntimeError("JSON decode error") from exc

        return True, data

    def _available_assets_from_google_drive(self) -> List[str]:
        params: Dict[str, Any] = {
            _QUERY: f"'{_KRAKEN_FOLDER_ID}' in parents",
            _API_KEY: self.__google_api_key,
            'pageSize': 1000
        }
        query_google_result, data = self._query_google_drive(params)
        if not query_google_result:
            self.__logger.error(f"Files were not found for folder_id={_KRAKEN_FOLDER_ID} (skipping): data={data}. "
                                f"Please open an issue at %s", self.ISSUES_URL)
            return []

        files = data[_FILES]
        available_assets: List[str] = []
        for file in files:
            if file['name'] in ['Kraken_OHLC_Sep1-Nov25.zip', 'Incremental Updates']:
                continue
            available_assets.append(file['name'].replace('_OHLCVT.zip', ''))

        return available_assets

    def initialize_markets(self) -> None:
        # setup internal asset to kraken asset conversion
        self._kraken.load_markets()

        def truncate_to_base(altname: str) -> str:
            length = len(altname)

            # Known Kraken assets with no zip files:
            # ARB
            # BLUR
            # GMX
            # HDX

            # Corner cases are handled by the following logic to handle conversion from well-formed base pairs used
            # by the dali importer to those stored by Kraken in their Google Drive when there is a mismatch between
            # Kraken's convention and those used by Dali.
            if altname in ['TUSD', 'TEUR']:
                return ''
            if altname in ['BLZEUR', 'BLZUSD']:
                return 'BLZ'
            if altname in ['CHZEUR', 'CHZUSD']:
                return 'CHZ'
            if altname in ['ETH2.SETH']:
                return 'ETH2'
            if altname in ['XTZAUD', 'XTZETH', 'XTZEUR', 'XTZGBP', 'XTZUSD', 'XTZUSDT', 'XTZXBT']:
                return 'XTZ'

            for fiat in _KRAKEN_FIAT_LIST:
                altname = altname.removesuffix(fiat)
                if length != len(altname):
                    break
            else:
                altname = altname[:-3]
            return altname

        for key, value in self._kraken.markets_by_id.items():
            pairs: List[str] = [key]

            # BUGFIX: the following line fixes issue where the expected dictionary is the element
            # of a list (of size 1). It should come be just a dictionary but in pytest (unit test flow)
            # it is a list and not a dictionary.
            value: Dict[str, str] = value if isinstance(value, dict) else value[0]  # type: ignore
            altnames: List[str] = [value[_ALTNAME]]
            google_abbr: str = truncate_to_base(value[_ALTNAME])
            if not google_abbr:
                continue
            if self.dali_base_to_kraken_google_base.get(value[_BASE]):
                pairs = self.dali_base_to_kraken_google_base[value[_BASE]][_PAIRS]  # type: ignore
                pairs.append(key)
                altnames = self.dali_base_to_kraken_google_base[value[_BASE]][_ALTNAME]  # type: ignore
                altnames.append(value[_ALTNAME])
                self.dali_base_to_kraken_google_base.update({value[_BASE]: {_PAIRS: pairs, _ALTNAME: altnames, _GOOGLE_ABBR: google_abbr}})
                self.kraken_google_base_to_dali_base.update({google_abbr: {_PAIRS: pairs, _ALTNAME: altnames, _BASE: value[_BASE]}})
            self.dali_base_to_kraken_google_base.update({value[_BASE]: {_PAIRS: pairs, _ALTNAME: altnames, _GOOGLE_ABBR: google_abbr}})
            self.kraken_google_base_to_dali_base.update({google_abbr: {_PAIRS: pairs, _ALTNAME: altnames, _BASE: value[_BASE]}})

    def expose_file_list_information(self) -> None:
        # This function performs two different checks:
        # 1) Expose the existence of OHLCVT zip files on the Kraken Google Drive matched against markets accessible
        #    via the Kraken API
        # 2) Expose OHLCVT zip files that do not have an obvious market accessible via the Kraken API
        #
        # The intended usage of this function is to expose information and support maintenance against upstream
        # changes by Kraken exchange.

        # 1) Iterates through well-formed assets (defined by Kraken API) and the Google Drive is checked to see if
        #    a file exists for that asset.
        for dummy_base_asset_dali, base_info_kraken in self.dali_base_to_kraken_google_base.items():
            base_asset_kraken: str = str(base_info_kraken[_GOOGLE_ABBR])
            base_file: str = f"{base_asset_kraken}_OHLCVT.zip"
            params: Dict[str, Any] = {
                _QUERY: f"'{_KRAKEN_FOLDER_ID}' in parents and name = '{base_file}'",
                _API_KEY: self.__google_api_key,
            }
            query_google_result, data = self._query_google_drive(params)
            if not query_google_result:
                self.__logger.error(f"File name doesn't exist: {base_file} (skipping): "
                                    f"base_asset_kraken={base_asset_kraken} from "
                                    f"altname={base_info_kraken[_PAIRS]}, "
                                    f"data={data}. Please open an issue at %s", self.ISSUES_URL)

        # 2) Check available assets from the Google Drive against well-formed assets (defined by Kraken API)
        kraken_asset_keys = set(self.kraken_google_base_to_dali_base.keys())
        google_avaialble_assets = set(self._available_assets_from_google_drive())
        orphaned_google_assets = google_avaialble_assets - kraken_asset_keys
        if orphaned_google_assets:
            self.__logger.error("Unmatched Google Drive asset against Kraken markets: orphaned_google_assets=%s", orphaned_google_assets)
