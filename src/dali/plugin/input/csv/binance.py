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

# Auto-invest CSV Format: timestamp in UTC, base asset symbol, quote asset amount + symbol, trading fee (in quote asset) + symbol, 
#	amount of base asset + symbol

import logging
from csv import reader
from typing import Dict, List, Optional

from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

class InputPlugin(AbstractInputPlugin):

	__BINANCE_COM: str = "Binance.com"


	def __init__(
		self,
		account_holder: str,
		autoinvest_csv_file: Optional[str] = None,
		betheth_csv_file: Optional[str] = None,
		native_fiat: Optional[str] = None,
	) -> None:

		super().__init__(account_holder=account_holder, native_fiat=native_fiat)
		self.__autoinvest_csv_file: Optional[str] = autoinvest_csv_file
		self.__betheth_csv_file: Optional[str] = betheth_csv_file
		self.__logger: logging.Logger = create_logger(f"{self.__BINANCE_COM}/{self.account_holder}")