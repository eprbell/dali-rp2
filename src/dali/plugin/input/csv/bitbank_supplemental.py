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
from rp2.abstract_country import AbstractCountry
from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction


class InputPlugin(AbstractInputPlugin):
    __BITBANK: str = "Bitbank.cc"
    __BITBANK_PLUGIN: str = "Bitbank_Supplemental_CSV"

    __ASSET_CODE: int = 0
    __TIMESTAMP_INDEX: int = 1

    __WITHDRAWAL_AMOUNT: int = 2
    __WITHDRAWAL_TRANSACTION_FEE: int = 3
    __WITHDRAWAL_LABEL: int = 4
    __WITHDRAWAL_NETWORK: int = 5
    __WITHDRAWAL_ADDRESS: int = 6
    __WITHDRAWAL_TX_ID: int = 7
    __WITHDRAWAL_STATUS: int = 8

    __DEPOSIT_TOTAL: int = 2
    __DEPOSIT_NETWORK: int = 3
    __DEPOSIT_ADDRESS: int = 4
    __DEPOSIT_TX_ID: int = 5
    __DEPOSIT_STATUS: int = 6

    __FIAT_DEPOSIT_TIMESTAMP: int = 0
    __FIAT_DEPOSIT_TOTAL: int = 1
    __FIAT_DEPOSIT_STATUS: int = 2

    __DELIMITER: str = ","

    def __init__(
        self,
        account_holder: str,
        withdrawals_csv_file: Optional[str] = None,
        deposits_csv_file: Optional[str] = None,
        fiat_deposits_csv_file: Optional[str] = None,
        native_fiat: Optional[str] = None,
    ) -> None:
        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__deposits_csv_file: Optional[str] = deposits_csv_file
        self.__withdrawals_csv_file: Optional[str] = withdrawals_csv_file
        self.__fiat_deposits_csv_file: Optional[str] = fiat_deposits_csv_file
        self.__logger: logging.Logger = create_logger(f"{self.__BITBANK_PLUGIN}/{self.account_holder}")

    def load(self, country: AbstractCountry) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        if self.__withdrawals_csv_file:
            result.extend(self.parse_withdrawals_file(self.__withdrawals_csv_file))

        if self.__deposits_csv_file:
            result.extend(self.parse_deposits_file(self.__deposits_csv_file))

        if self.__fiat_deposits_csv_file:
            result.extend(self.parse_fiat_deposits_file(self.__fiat_deposits_csv_file))

        return result

    def parse_withdrawals_file(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)

            header = str(next(lines)).encode("utf-8")
            self.__logger.debug("Header: %s", header)
            for line in lines:
                if line[self.__WITHDRAWAL_STATUS] == "DONE":
                    raw_data: str = self.__DELIMITER.join(line)
                    self.__logger.debug("Transaction: %s", raw_data)
                    self.__logger.debug("Withdrawal: %s", line[self.__WITHDRAWAL_AMOUNT])

                    jst_timezone = PytzTimezone("Asia/Tokyo")
                    jst_datetime: datetime = jst_timezone.localize(datetime.strptime(line[self.__TIMESTAMP_INDEX], "%Y/%m/%d %H:%M:%S"))
                    utc_timestamp: str = jst_datetime.astimezone(DatetimeTimezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

                    result.append(
                        IntraTransaction(
                            plugin=self.__BITBANK_PLUGIN,
                            unique_id=line[self.__WITHDRAWAL_TX_ID],
                            raw_data=raw_data,
                            timestamp=utc_timestamp,
                            asset=str(line[self.__ASSET_CODE]).upper(),
                            from_exchange=self.__BITBANK,
                            from_holder=self.account_holder,
                            to_exchange=Keyword.UNKNOWN.value,
                            to_holder=Keyword.UNKNOWN.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_sent=str(RP2Decimal(line[self.__WITHDRAWAL_AMOUNT]) + RP2Decimal(line[self.__WITHDRAWAL_TRANSACTION_FEE])),
                            crypto_received=Keyword.UNKNOWN.value,
                        )
                    )

        return result

    def parse_deposits_file(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)

            header = str(next(lines)).encode("utf-8")
            self.__logger.debug("Header: %s", header)
            for line in lines:
                if line[self.__DEPOSIT_STATUS] == "DONE":
                    raw_data: str = self.__DELIMITER.join(line)
                    self.__logger.debug("Transaction: %s", raw_data)

                    jst_timezone = PytzTimezone("Asia/Tokyo")
                    jst_datetime: datetime = jst_timezone.localize(datetime.strptime(line[self.__TIMESTAMP_INDEX], "%Y/%m/%d %H:%M:%S"))
                    utc_timestamp: str = jst_datetime.astimezone(DatetimeTimezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

                    result.append(
                        IntraTransaction(
                            plugin=self.__BITBANK_PLUGIN,
                            unique_id=line[self.__DEPOSIT_TX_ID],
                            raw_data=raw_data,
                            timestamp=utc_timestamp,
                            asset=str(line[self.__ASSET_CODE]).upper(),
                            from_exchange=Keyword.UNKNOWN.value,
                            from_holder=Keyword.UNKNOWN.value,
                            to_exchange=self.__BITBANK,
                            to_holder=self.account_holder,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_sent=Keyword.UNKNOWN.value,
                            crypto_received=line[self.__DEPOSIT_TOTAL],
                        )
                    )

        return result

    # Currently, and most likely for the foreseeable future, Bitbank only accepts JPY deposits
    # These should be recorded if you need to file taxes in a different currency.
    def parse_fiat_deposits_file(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)

            header = str(next(lines)).encode("utf-8")
            self.__logger.debug("Header: %s", header)
            for line in lines:
                if line[self.__FIAT_DEPOSIT_STATUS] == "DONE":
                    raw_data: str = self.__DELIMITER.join(line)
                    self.__logger.debug("Transaction: %s", raw_data)

                    jst_timezone = PytzTimezone("Asia/Tokyo")
                    jst_datetime: datetime = jst_timezone.localize(datetime.strptime(line[self.__FIAT_DEPOSIT_TIMESTAMP], "%Y/%m/%d %H:%M:%S"))
                    utc_timestamp: str = jst_datetime.astimezone(DatetimeTimezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")

                    result.append(
                        InTransaction(
                            plugin=self.__BITBANK_PLUGIN,
                            unique_id=Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=utc_timestamp,
                            asset="JPY",
                            exchange=self.__BITBANK,
                            holder=self.account_holder,
                            transaction_type=Keyword.BUY.value,
                            spot_price="1.0",
                            crypto_in=str(line[self.__FIAT_DEPOSIT_TOTAL]),
                            crypto_fee=None,
                            fiat_in_no_fee=str(line[self.__FIAT_DEPOSIT_TOTAL]),
                            fiat_in_with_fee=str(line[self.__FIAT_DEPOSIT_TOTAL]),
                            fiat_ticker="JPY",
                            notes="Fiat deposit",
                        )
                    )

        return result
