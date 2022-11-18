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

# Kraken CSV format: 

import logging
from csv import reader
from collections import ChainMap
from datetime import datetime, timedelta
from itertools import repeat
from io import BytesIO, TextIOWrapper
from json import JSONDecodeError, loads
from multiprocessing.pool import ThreadPool
from typing import Dict, List, Optional
from zipfile import ZipFile

import requests
from requests.models import Response

from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal

from dali.historical_bar import HistoricalBar

# Google Drive parameters
_ACCESS_NOT_CONFIGURED: str = "accessNotConfigured"
_ERROR: str = "error"
_ERRORS: str = "errors"
_GOOGLE_API_KEY: str = "AIzaSyBPZbQdzwVAYQox79GJ8yBkKQQD9ligOf8"
_GOOGLE_APIS_URL: str = "https://www.googleapis.com/drive/v3/files"
_KRAKEN_FOLDER_ID: str = "1aoA6SKgPbS_p3pYStXUXFvmjqShJ2jv9"
_MESSAGE: str = "message"
_REASON: str = "reason"

# Google Drive URL Params
_ALT: str = "alt"
_API_KEY: str = "key"
_CONFIRM: str = "confirm"
_MEDIA: str = "media"
_QUERY: str = "q"

# JSON params
_FILES: str = "files"
_ID: str = "id"
_ERROR: str = "error"

class kraken_csv_pricing():

    __KRAKEN_OHLCVT: str = "Kraken.com_CSVOHLCVT"

    __TIMEOUT: int = 30
    __THREAD_COUNT: int = 6

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

    def get_historical_bars_for_pair(self, base_asset: str, quote_asset: str) -> List[HistoricalBar]:
        bars: List[HistoricalBar] = []
        base_file: str = f"{base_asset}_OHLCVT.zip"

        self.__logger.debug(base_file)

        with ZipFile(BytesIO(self._google_file_to_bytes(base_file))) as zipped_OHLCVT:
            self.__logger.debug(zipped_OHLCVT.namelist())
            all_timespans_for_pair: List[str] = [x for x in zipped_OHLCVT.namelist() if (
                x.startswith(f"{base_asset}{quote_asset}_")
            )]

            if len(all_timespans_for_pair) == 0:
                self.__logger.debug("Market not found in Kraken files. Skipping file read.")
                return bars

            with ThreadPool(self.__THREAD_COUNT) as pool:
                processing_result_list: List[HistoricalBar] = pool.starmap(self._process_file, zip(all_timespans_for_pair, repeat(zipped_OHLCVT)))

        # Sort the bars by duration, largest duration first
        sorted_bars = reversed(sorted(processing_result_list, key= lambda x: int(list(x.keys())[0])))

        timed_bars: Dict[int, HistoricalBar] = {}

        # Start with longest duration and replace it with smaller durations
        for duration_bars in sorted_bars:
            bars_for_duration = list(duration_bars.values())[0]
            for bar in bars_for_duration:
                timed_bars[bar.timestamp] = bar

        return timed_bars.values()

    # isolated in order to be mocked
    def _google_file_to_bytes(self, file_name: str) -> bytes:
        params: Dict[str, Any] = {
            _QUERY: f"'{_KRAKEN_FOLDER_ID}' in parents and name = '{file_name}'", 
            _API_KEY: self.__google_api_key,
        }  
        try:
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

            ## Error Response
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
                        self.__logger.error("""
                            Access not granted to Google Drive API. You must grant authorization to the Google 
                            Drive API for your API key. Follow the link in the message for more details. Message:\n%s
                        """, error[_MESSAGE])
                        raise Exception("Google Drive not authorized")

            self.__logger.debug("Retrieved %s from %s", data, response.url)

            params = {
                _ALT : _MEDIA, _API_KEY: self.__google_api_key, _CONFIRM: 1 # Bypasses large file warning
            }
            file_response: Response = self.__session.get(f"{_GOOGLE_APIS_URL}/{data[_FILES][0][_ID]}", params=params, timeout=self.__TIMEOUT)

        except JSONDecodeError as exc:
            self.__logger.debug("Fetching of kraken csv files failed. Try again later.")
            raise Exception("JSON decode error") from exc        

        return file_response.content

    def _process_file(self, file_name: str, zip_file: ZipFile) -> Dict[int, List[HistoricalBar]]:
        bars: List[HistoricalBar] = []
        self.__logger.debug("Reading in file %s for Kraken CSV pricing.", file_name)
        csv_file: str = zip_file.read(file_name).decode(encoding='utf-8')
        duration_in_minutes: str = file_name.split("_", 1)[1].strip(".csv")

        lines = reader(csv_file.splitlines())
         
        for line in lines:
            bars.append(HistoricalBar(
                duration=timedelta(minutes=int(duration_in_minutes)),
                timestamp=datetime.fromtimestamp(int(line[self.__TIMESTAMP_INDEX])),
                open=RP2Decimal(line[self.__OPEN]),
                high=RP2Decimal(line[self.__HIGH]),
                low=RP2Decimal(line[self.__LOW]),
                close=RP2Decimal(line[self.__CLOSE]),
                volume=RP2Decimal(line[self.__VOLUME]),
            ))

        return {duration_in_minutes: bars}
