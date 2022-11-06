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
from datetime import datetime, timedelta
from io import TextIOWrapper
from typing import List, Optional
from zipfile import ZipFile

import requests
from requests.models import Response

from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal

from dali.historical_bar import HistoricalBar
from dali.plugin.pair_converter.ccxt import BaseQuotePair

# Google Drive parameters
_GOOGLE_API_KEY: str = "AIzaSyBPZbQdzwVAYQox79GJ8yBkKQQD9ligOf8"
_GOOGLE_APIS_URL: str = "https://www.googleapis.com/drive/v3/files"
_KRAKEN_FOLDER_ID: str = "1aoA6SKgPbS_p3pYStXUXFvmjqShJ2jv9"

# Google Drive URL Params
_ALT: str = "alt"
_API_KEY: str = "key"
_CONFIRM: str = "confirm"
_MEDIA: str = "media"
_QUERY: str = "q"

# JSON params
_FILES: str = "files"
_ID: str = "id"

class kraken_csv_pricing():

	__KRAKEN_OHLCVT: str = "Kraken.com_CSVOHLCVT"

    __TIMEOUT: int = 30

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
    	params: Dict[str, Any] = {
    		_QUERY: f"'{_KRAKEN_FOLDER_ID}' in parents and name = {base_file}", 
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
    	
    		data: Any = response.json()

    		params = {
    			_ALT : _MEDIA, _API_KEY: self.__google_api_key, _CONFIRM: 1 # Bypasses large file warning
    		}
    		file_response: Response = self.__session.get(f"{_GOOGLE_APIS_URL}/{data[_FILES][0][_ID]}", params=params, timeout=__TIMEOUT)

    	with ZipFile(file_response) as zipped_OHLCVT:
    		market_pairs: List[]
    		all_timespans_for_pair: List[str] = [x for x in zipped_OHLCVT.namelist() if (
    			x.startswith(f"{base_asset}{quote_asset}_")
    		)]

    		if len(all_timespans_for_pair) == 0:
                self.__logger.debug("Market not found in Kraken files. Skipping file read.")
                return bars

            for file_name in all_timespans_for_pair:
    			csv_file: str = zipped_OHLCVT.read(file_name).decode(encoding='utf-8')
    			duration_in_minutes: str = file_name.split("_", 1)[1].strip(".csv")

    			lines = reader(csv_file)
    			for line in lines:
    				datetime.fromtimestamp()
    				bars.append(HistoricalBar(
    					duration=timedelta(minutes=int(duration_in_minutes)),
    					timestamp=datetime.fromtimestamp(line[_TIMESTAMP_INDEX]),
    					open=RP2Decimal(line[__OPEN]),
    					high=RP2Decimal(line[__HIGH]),
    					low=RP2Decimal(line[__LOW]),
    					close=RP2Decimal(line[__CLOSE]),
    					volume=RP2Decimal(line[__VOLUME]),
    				))

            return bars
