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

# Buys CSV Format: Internal ID, amount (amount received), (Total) Price, Trading Currency (Crypto Asset),
#    Original Currency (fiat used), Progress, UTC Timestamp

import logging
from csv import reader
from typing import List, Optional

from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction


class InputPlugin(AbstractInputPlugin):

    __COINCHECK: str = "Coincheck"
    __COINCHECK_SUPPLEMENTAL_PLUGIN: str = "Coincheck_Supplemental_CSV"

    __ID: int = 0
    __AMOUNT_PURCHASED: int = 1
    __TOTAL_PRICE: int = 2
    __CRYPTO_ASSET: int = 3
    __FIAT_USED: int = 4
    __PROGRESS: int = 5
    __TIMESTAMP_INDEX: int = 6

    __DELIMITER: str = ","

    def __init__(
        self,
        account_holder: str,
        buys_csv_file: str,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__buys_csv_file: str = buys_csv_file
        self.__logger: logging.Logger = create_logger(f"{self.__COINCHECK_SUPPLEMENTAL_PLUGIN}/{self.account_holder}")

    def load(self) -> List[AbstractTransaction]:
        return self.parse_buys_file(self.__buys_csv_file)

    def parse_buys_file(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)

            header = next(lines)
            self.__logger.debug("Header: %s", header)
            for line in lines:
                raw_data: str = self.__DELIMITER.join(line)
                self.__logger.debug("Transaction: %s", raw_data)

                unformatted_timestamp: str = line[self.__TIMESTAMP_INDEX]
                timestamp: str = unformatted_timestamp[: -len(" UTC")]

                result.append(
                    InTransaction(
                        plugin=self.__COINCHECK_SUPPLEMENTAL_PLUGIN,
                        unique_id=line[self.__ID],
                        raw_data=raw_data,
                        timestamp=f"{timestamp} -00:00",
                        asset=line[self.__CRYPTO_ASSET],
                        exchange=self.__COINCHECK,
                        holder=self.account_holder,
                        transaction_type=Keyword.BUY.value,
                        spot_price=str(RP2Decimal(line[self.__TOTAL_PRICE]) / RP2Decimal(line[self.__AMOUNT_PURCHASED])),
                        crypto_in=line[self.__AMOUNT_PURCHASED],
                        crypto_fee=None,
                        fiat_in_no_fee=line[self.__TOTAL_PRICE],
                        fiat_in_with_fee=line[self.__TOTAL_PRICE],
                        fiat_ticker="JPY",
                        notes=None,
                    )
                )

        return result
