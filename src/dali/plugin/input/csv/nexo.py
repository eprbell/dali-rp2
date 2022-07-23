# Copyright 2022 mbianco
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
from typing import Dict, List, Optional

from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# _ACH_DEPOSIT: str = "Ach Deposit"
# _ACH_WITHDRAWAL: str = "Ach Withdrawal"
# _BUY_CURRENCY: str = "Buy Currency"
# _BUY_QUANTITY: str = "Buy Quantity"
# _CRYPTO_TRANSFER: str = "Crypto Transfer"
# _DATE: str = "Date"
# _INTEREST_PAYMENT: str = "Interest Payment"
# _REFERRAL_BONUS: str = "Referral Bonus"
# _SOLD_CURRENCY: str = "Sold Currency"
# _SOLD_QUANTITY: str = "Sold Quantity"
# _TRADE: str = "Trade"
# _TRADE_ID: str = "Trade ID"
# _TYPE: str = "Type"
# _WITHDRAWAL: str = "Withdrawal"
# _WITHDRAWAL_FEE: str = "Withdrawal Fee"


# types of transactions
_INTEREST = "Interest"
_LOCKING_TERM_DEPOSIT = "LockingTermDeposit"
_UNLOCKING_TERM_DEPOSIT = "UnlockingTermDeposit"
_FIXED_TERM_INTEREST = "FixedTermInterest"
_DEPOSIT = "Deposit"

class InputPlugin(AbstractInputPlugin):

    __NEXO: str = "Nexo"

    __TRANSACTION_ID_INDEX = 0
    __TRANSACTION_TYPE_INDEX = 1
    __CURRENCY_INDEX: int = 2
    __AMOUNT_INDEX: int = 3

    __TIMESTAMP_INDEX: int = 7

    __DELIMITER = ","

    def __init__(
        self,
        account_holder: str,
        transaction_csv_file: str,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__transaction_csv_file: str = transaction_csv_file
        self.__logger: logging.Logger = create_logger(f"{self.__NEXO}/{self.account_holder}")

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        last_withdrawal_fee: Optional[RP2Decimal] = None
        with open(self.__transaction_csv_file, encoding="utf-8") as transaction_csv_file:
            # read CSV with header and skip first row
            lines = reader(transaction_csv_file)

            # Skip header line
            header = next(lines)
            self.__logger.debug("Header: %s", header)

            for line in lines:
                raw_data: str = self.__DELIMITER.join(line)
                self.__logger.debug("Transaction: %s", raw_data)

                transaction_id: str = line[self.__TRANSACTION_ID_INDEX].strip()
                transaction_type: str = line[self.__TRANSACTION_TYPE_INDEX].strip()
                currency: str = line[self.__CURRENCY_INDEX].strip()
                amount = line[self.__AMOUNT_INDEX].strip()
                timestamp_with_timezone = f"{line[self.__TIMESTAMP_INDEX].strip()} -00:00"

                if transaction_type in [_INTEREST, _FIXED_TERM_INTEREST]:
                    result.append(
                        InTransaction(
                            plugin=self.__NEXO,
                            unique_id=transaction_id,
                            raw_data=raw_data,
                            exchange=self.__NEXO,
                            holder=self.account_holder,
                            timestamp=timestamp_with_timezone,
                            asset=currency,

                            transaction_type=Keyword.INTEREST.name,
                            # nexo does give us the spot price, but it's often 0 if subcent
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_in=amount,
                            fiat_fee="0",
                        )
                    )
                elif transaction_type in [_LOCKING_TERM_DEPOSIT, _UNLOCKING_TERM_DEPOSIT]:
                    # I don't think we need to record locking/unlocking deposits for term interest
                    self.__logger.debug("Skipping lock or unlock deposit: %s", line)
                elif transaction_type == _DEPOSIT:
                    result.append(
                        IntraTransaction(
                            plugin=self.__NEXO,
                            unique_id=transaction_id,
                            raw_data=raw_data,
                            timestamp=timestamp_with_timezone,
                            asset=currency,

                            crypto_received=amount,

                            # most likely, it's you, but we can't say for sure
                            from_exchange=Keyword.UNKNOWN.value,
                            from_holder=Keyword.UNKNOWN.value,

                            to_exchange=self.__NEXO,
                            to_holder=self.account_holder,

                            # we do know the spot price, but nexo seems to do some aggressive rounding
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_sent=Keyword.UNKNOWN.value,
                        )
                    )
                else:
                    self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        return result

