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

# Withdrawal CSV Format: 日時 (timestamp), 数量 (amount), 手数料 (transaction fee), 合計 (total), ラベル (user-provided label), アドレス (address), Txid, ステータス (status)

import logging
from csv import reader
from datetime import datetime
from datetime import timezone as DatetimeTimezone
from typing import List, Optional

from pytz import timezone as PytzTimezone
from rp2.logger import create_logger
from rp2.rp2_error import RP2ValueError

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction


class InputPlugin(AbstractInputPlugin):

    __BITBANK: str = "Bitbank.cc"
    __BITBANK_PLUGIN: str = "Bitbank_Supplemental_CSV"

    __TIMESTAMP_INDEX: int = 0
    __SENT_AMOUNT: int = 1
    __DEPOSIT_TOTAL: int = 1
    __TRANSACTION_FEE: int = 2
    __DEPOSIT_STATUS: int = 2
    __TOTAL: int = 3
    __LABEL: int = 4
    __ADDRESS: int = 5
    __TX_ID: int = 6
    __STATUS: int = 7

    __DELIMITER: str = ","

    def __init__(
        self,
        account_holder: str,
        deposits_csv_file: Optional[str] = None,
        deposits_code: Optional[str] = None,
        withdrawals_csv_file: Optional[str] = None,
        withdrawals_code: Optional[str] = None,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__deposits_csv_file: Optional[str] = deposits_csv_file
        self.__deposits_code: Optional[str] = deposits_code
        self.__withdrawals_csv_file: Optional[str] = withdrawals_csv_file
        # Code of the asset being withdrawn since it is NOT included in the CSV file.
        self.__withdrawals_code: Optional[str] = withdrawals_code
        self.__logger: logging.Logger = create_logger(f"{self.__BITBANK_PLUGIN}/{self.account_holder}")

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        if self.__withdrawals_csv_file:
            result.extend(self.parse_withdrawals_file(self.__withdrawals_csv_file))

        if self.__deposits_csv_file:
            result.extend(self.parse_deposits_file(self.__deposits_csv_file))

        return result

    def parse_deposits_file(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []
        if not self.__deposits_code:
            raise RP2ValueError("Bitbank.cc supplemental plugin deposits file declared without deposits code.")

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)

            header = next(lines)
            self.__logger.debug("Header: %s", header)
            for line in lines:
                if line[self.__DEPOSIT_STATUS] == "DONE":
                    raw_data: str = self.__DELIMITER.join(line)
                    self.__logger.debug("Transaction: %s", raw_data)

                    jst_timezone = PytzTimezone("Asia/Tokyo")
                    jst_datetime: datetime = jst_timezone.localize(datetime.strptime(line[self.__TIMESTAMP_INDEX], "%Y/%m/%d %H:%M:%S"))
                    utc_timestamp: str = jst_datetime.astimezone(DatetimeTimezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

                    result.append(
                        InTransaction(
                            plugin=self.__BITBANK_PLUGIN,
                            unique_id=Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=utc_timestamp,
                            asset=self.__deposits_code,
                            exchange=self.__BITBANK,
                            holder=self.account_holder,
                            transaction_type=Keyword.BUY.value,
                            spot_price="1.0",
                            crypto_in=str(line[self.__DEPOSIT_TOTAL]),
                            crypto_fee=None,
                            fiat_in_no_fee=str(line[self.__DEPOSIT_TOTAL]),
                            fiat_in_with_fee=str(line[self.__DEPOSIT_TOTAL]),
                            fiat_ticker="JPY",
                            notes="Fiat deposit",
                        )
                    )

        return result

    def parse_withdrawals_file(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []
        if not self.__withdrawals_code:
            raise RP2ValueError("Bitbank.cc supplemental plugin withdrawals file declared without withdrawals code.")

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)

            header = next(lines)
            self.__logger.debug("Header: %s", header)
            for line in lines:
                raw_data: str = self.__DELIMITER.join(line)
                self.__logger.debug("Transaction: %s", raw_data)

                jst_timezone = PytzTimezone("Asia/Tokyo")
                jst_datetime: datetime = jst_timezone.localize(datetime.strptime(line[self.__TIMESTAMP_INDEX], "%Y/%m/%d %H:%M:%S"))
                utc_timestamp: str = jst_datetime.astimezone(DatetimeTimezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

                result.append(
                    IntraTransaction(
                        plugin=self.__BITBANK_PLUGIN,
                        unique_id=line[self.__TX_ID],
                        raw_data=raw_data,
                        timestamp=utc_timestamp,
                        asset=self.__withdrawals_code,
                        from_exchange=self.__BITBANK,
                        from_holder=self.account_holder,
                        to_exchange=Keyword.UNKNOWN.value,
                        to_holder=Keyword.UNKNOWN.value,
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_sent=str(line[self.__TOTAL]),
                        crypto_received=Keyword.UNKNOWN.value,
                    )
                )

        return result
