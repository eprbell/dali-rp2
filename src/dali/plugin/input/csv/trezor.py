# Copyright 2022 eprbell
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

# CSV Format: timestamp; type; transaction_id; address; fee; total

import logging
from csv import reader
from datetime import datetime
from typing import List, Optional

import pytz
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.intra_transaction import IntraTransaction

_SENT: str = "SENT"
_RECV: str = "RECV"


class InputPlugin(AbstractInputPlugin):

    __TREZOR: str = "Trezor"

    __TIMESTAMP_INDEX: int = 0
    __TYPE_INDEX: int = 1
    __TRANSACTION_ID_INDEX: int = 2
    __FEE_INDEX: int = 4
    __TOTAL_INDEX: int = 5

    __DELIMITER = ";"

    def __init__(
        self,
        account_holder: str,
        account_nickname: str,
        currency: str,
        timezone: str,
        csv_file: str,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__account_nickname: str = account_nickname
        self.__currency: str = currency
        self.__timezone = pytz.timezone(timezone)
        self.__csv_file: str = csv_file

        self.__logger: logging.Logger = create_logger(f"{self.__TREZOR}/{currency}/{self.__account_nickname}/{self.account_holder}")

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(self.__csv_file, encoding="utf-8") as csv_file:
            lines = reader(csv_file, delimiter=self.__DELIMITER)
            header_found: bool = False
            for line in lines:
                raw_data: str = self.__DELIMITER.join(line)
                if not header_found:
                    # Skip header line
                    header_found = True
                    self.__logger.debug("Header: %s", raw_data)
                    continue
                timestamp: str = line[self.__TIMESTAMP_INDEX]
                timestamp_value: datetime = datetime.strptime(timestamp, "%m/%d/%Y, %I:%M:%S %p")
                timestamp_value = self.__timezone.normalize(self.__timezone.localize(timestamp_value))
                self.__logger.debug("Transaction: %s", raw_data)
                transaction_type: str = line[self.__TYPE_INDEX]
                spot_price: str = Keyword.UNKNOWN.value
                crypto_hash: str = line[self.__TRANSACTION_ID_INDEX]
                fee_number: RP2Decimal = RP2Decimal(line[self.__FEE_INDEX])
                total_number: RP2Decimal = RP2Decimal(line[self.__TOTAL_INDEX])

                if total_number == ZERO and fee_number > ZERO:
                    self.__logger.warning("Possible dusting attack (fee > 0, total = 0): %s", raw_data)
                    continue
                if transaction_type in {_SENT, _RECV}:
                    result.append(
                        IntraTransaction(
                            plugin=self.__TREZOR,
                            unique_id=crypto_hash,
                            raw_data=raw_data,
                            timestamp=f"{timestamp_value}",
                            asset=self.__currency,
                            from_exchange=self.__account_nickname if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            from_holder=self.account_holder if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            to_exchange=self.__account_nickname if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            to_holder=self.account_holder if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            spot_price=spot_price,
                            crypto_sent=str(total_number + fee_number) if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            crypto_received=str(total_number) if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            notes=None,
                        )
                    )
                else:
                    self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        return result
