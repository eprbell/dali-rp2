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
from typing import List

from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal, ZERO

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.dali_configuration import Keyword
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

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
    ) -> None:

        super().__init__(account_holder)
        self.__account_nickname: str = account_nickname
        self.__currency: str = currency
        self.__timezone: str = timezone
        self.__csv_file: str = csv_file

        self.__logger: logging.Logger = create_logger(f"{self.__TREZOR}/{currency}/{self.__account_nickname}/{self.account_holder}")

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(self.__csv_file, mode="r", encoding="utf-8") as csv_file:
            lines = reader(csv_file, delimiter=self.__DELIMITER)
            header_found: bool = False
            for line in lines:
                raw_data: str = self.__DELIMITER.join(line)
                if not header_found:
                    # Skip header line
                    header_found = True
                    self.__logger.debug("Header: %s", raw_data)
                    continue
                self.__logger.debug("Transaction: %s", raw_data)
                transaction_type: str = line[self.__TYPE_INDEX]
                spot_price: str = Keyword.UNKNOWN.value
                crypto_hash: str = line[self.__TRANSACTION_ID_INDEX]
                fee_number: RP2Decimal = RP2Decimal(line[self.__FEE_INDEX])
                total_number: RP2Decimal = RP2Decimal(line[self.__TOTAL_INDEX])

                if total_number == ZERO and fee_number > ZERO:
                    # Cost-only transaction
                    result.append(
                        OutTransaction(
                            self.__TREZOR,
                            crypto_hash,
                            raw_data,
                            f"{line[self.__TIMESTAMP_INDEX]} {self.__timezone}",
                            self.__currency,
                            self.__account_nickname,
                            self.account_holder,
                            "Sell",
                            spot_price,
                            line[self.__FEE_INDEX],
                            "0",
                            notes="Cost-only transaction",
                        )
                    )
                else:
                    result.append(
                        IntraTransaction(
                            self.__TREZOR,
                            crypto_hash,
                            raw_data,
                            f"{line[self.__TIMESTAMP_INDEX]} {self.__timezone}",
                            self.__currency,
                            self.__account_nickname if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            self.account_holder if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            self.__account_nickname if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            self.account_holder if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            spot_price,
                            str(total_number + fee_number) if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            str(total_number) if transaction_type == _RECV else Keyword.UNKNOWN.value,
                        )
                    )

        return result
