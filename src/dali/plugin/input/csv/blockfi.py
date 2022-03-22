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

# CSV Format: currency, amount, type, timestamp
# Note: BlockFi doesn't provide hash information, so BlockFi transactions cannot be resolved

import logging
from csv import reader
from typing import List, Optional

from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.dali_configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction

_CRYPTO_TRANSFER: str = "Crypto Transfer"
_INTEREST_PAYMENT: str = "Interest Payment"
_WITHDRAWAL: str = "Withdrawal"
_WITHDRAWAL_FEE: str = "Withdrawal Fee"


class InputPlugin(AbstractInputPlugin):

    __BLOCKFI: str = "BlockFi"

    __CURRENCY_INDEX: int = 0
    __AMOUNT_INDEX: int = 1
    __TYPE_INDEX: int = 2
    __TIMESTAMP_INDEX: int = 3
    #    __SPOT_PRICE_INDEX: int = 4 # It used to be present in the CSV, but sometime in 2021 BlockFi removed this field.

    __DELIMITER = ","

    def __init__(
        self,
        account_holder: str,
        csv_file: str,
    ) -> None:

        super().__init__(account_holder)
        self.__csv_file: str = csv_file

        self.__logger: logging.Logger = create_logger(f"{self.__BLOCKFI}/{self.account_holder}")

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        last_withdrawal_fee: Optional[RP2Decimal] = None
        with open(self.__csv_file, mode="r", encoding="utf-8") as csv_file:
            lines = reader(csv_file)
            header_found: bool = False
            for line in lines:
                raw_data: str = self.__DELIMITER.join(line)
                if not header_found:
                    # Skip header line
                    header_found = True
                    self.__logger.debug("Header: %s", raw_data)
                    continue
                self.__logger.debug("Transaction: %s", raw_data)

                if last_withdrawal_fee is not None and line[self.__TYPE_INDEX] != _WITHDRAWAL:
                    raise Exception(f"Internal error: withdrawal fee {last_withdrawal_fee} is not followed by withdrawal")

                if line[self.__TYPE_INDEX] == _INTEREST_PAYMENT:
                    last_withdrawal_fee = None
                    result.append(
                        InTransaction(
                            plugin=self.__BLOCKFI,
                            unique_id=Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                            asset=line[self.__CURRENCY_INDEX],
                            exchange=self.__BLOCKFI,
                            holder=self.account_holder,
                            transaction_type="Interest",
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_in=line[self.__AMOUNT_INDEX],
                            fiat_fee="0",
                        )
                    )
                elif line[self.__TYPE_INDEX] == _CRYPTO_TRANSFER:
                    last_withdrawal_fee = None
                    result.append(
                        IntraTransaction(
                            plugin=self.__BLOCKFI,
                            unique_id=Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                            asset=line[self.__CURRENCY_INDEX],
                            from_exchange=Keyword.UNKNOWN.value,
                            from_holder=Keyword.UNKNOWN.value,
                            to_exchange=self.__BLOCKFI,
                            to_holder=self.account_holder,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_sent=Keyword.UNKNOWN.value,
                            crypto_received=line[self.__AMOUNT_INDEX],
                        )
                    )
                elif line[self.__TYPE_INDEX] == _WITHDRAWAL:
                    amount: RP2Decimal = RP2Decimal(line[self.__AMOUNT_INDEX])
                    amount = -amount  # type: ignore
                    if last_withdrawal_fee is not None:
                        amount += last_withdrawal_fee
                    last_withdrawal_fee = None
                    result.append(
                        IntraTransaction(
                            plugin=self.__BLOCKFI,
                            unique_id=Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                            asset=line[self.__CURRENCY_INDEX],
                            from_exchange=self.__BLOCKFI,
                            from_holder=self.account_holder,
                            to_exchange=Keyword.UNKNOWN.value,
                            to_holder=Keyword.UNKNOWN.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_sent=str(amount),
                            crypto_received=Keyword.UNKNOWN.value,
                        )
                    )
                elif line[self.__TYPE_INDEX] == _WITHDRAWAL_FEE:
                    last_withdrawal_fee = RP2Decimal(line[self.__AMOUNT_INDEX])
                    last_withdrawal_fee = -last_withdrawal_fee  # type: ignore
                else:
                    self.__logger.debug("Unsupported transaction type (skipping): %s", raw_data)
        return result
