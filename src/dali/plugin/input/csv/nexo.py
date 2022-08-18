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

# CSV Format: transaction id, type, currency, amount, usd equivalent, details, outstanding loan, datetime

import logging
import re
from csv import reader
from decimal import Decimal
from typing import List, Optional

from rp2.logger import create_logger

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction

# transaction types
# locking & unlocking deposits are transfers within Nexo. They cannot lock funds from other wallets/accounts.
_INTEREST = "Interest"
_LOCKING_TERM_DEPOSIT = "LockingTermDeposit"
_UNLOCKING_TERM_DEPOSIT = "UnlockingTermDeposit"
_FIXED_TERM_INTEREST = "FixedTermInterest"
_DEPOSIT = "Deposit"


class InputPlugin(AbstractInputPlugin):
    __NEXO: str = "Nexo"

    __TRANSACTION_TYPE_INDEX = 1
    __CURRENCY_INDEX: int = 2
    __AMOUNT_INDEX: int = 3
    __SPOT_PRICE_INDEX: int = 4
    __DETAILS_INDEX = 5

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

        with open(self.__transaction_csv_file, encoding="utf-8") as transaction_csv_file:
            # read CSV with header and skip first row
            lines = reader(transaction_csv_file)

            # Skip header line
            header = next(lines)
            self.__logger.debug("Header: %s", header)

            for line in lines:
                raw_data: str = self.__DELIMITER.join(line)
                self.__logger.debug("Transaction: %s", raw_data)

                transaction_type: str = line[self.__TRANSACTION_TYPE_INDEX].strip()
                currency: str = line[self.__CURRENCY_INDEX].strip()
                amount = line[self.__AMOUNT_INDEX].strip()
                # there is no timezone information in the CSV, so we assume UTC
                timestamp_with_timezone = f"{line[self.__TIMESTAMP_INDEX].strip()} -00:00"

                common_params = {
                    # although there is a transaction id in the CSV, it is not a transaction hash shared across exchanges, so it is uselsss
                    # https://github.com/eprbell/dali-rp2/pull/60#issuecomment-1201481064
                    "unique_id": Keyword.UNKNOWN.value,
                    "plugin": self.__NEXO,
                    "raw_data": raw_data,
                    "timestamp": timestamp_with_timezone,
                    "asset": currency,
                }

                # nexo gives us the realized usd, but it's often 0 if a subcent value
                # if it is non-zero, we use it to calculate the spot price, otherwise we use unknown as the value
                # the spot price contains $ char, so we remove it. Sometimes the price is specified with scientific notation,
                # so we are not using a more general regex like `[^\d.]`
                realized_usd = Decimal(re.sub(r"[$]", "", line[self.__SPOT_PRICE_INDEX]))
                spot_price = str(realized_usd / Decimal(amount)) if not realized_usd.is_zero() else Keyword.UNKNOWN.value

                if transaction_type in [_INTEREST, _FIXED_TERM_INTEREST]:
                    result.append(
                        InTransaction(
                            **(
                                common_params  # type: ignore
                                | {
                                    "exchange": self.__NEXO,
                                    "holder": self.account_holder,
                                    "transaction_type": Keyword.INTEREST.value,
                                    "spot_price": spot_price,
                                    "crypto_in": amount,
                                    "fiat_fee": "0",
                                }
                            )
                        )
                    )
                elif transaction_type in [_LOCKING_TERM_DEPOSIT, _UNLOCKING_TERM_DEPOSIT]:
                    # These are unique to Nexo: they "lock" your crypto in a "fixed term" deposit which earns higher interest.
                    # i.e. these transactions just indicate that you cannot withdraw these funds while these are locked. So they effect your available balance.
                    # I don't think we need to record locking/unlocking deposits for term interest
                    self.__logger.debug("Skipping lock or unlock deposit: %s", line)
                elif transaction_type == _DEPOSIT:
                    unique_id = common_params["unique_id"]

                    # nexo includes a network transaction hash in the case of deposits: it's embedded in the details field
                    # let's extract it using a regex
                    if transaction_hash_match := re.search(r"([A-Fa-f0-9]{64})$", line[self.__DETAILS_INDEX]):
                        unique_id = transaction_hash_match[0]

                    result.append(
                        IntraTransaction(
                            **(
                                common_params  # type: ignore
                                | {
                                    "unique_id": unique_id,
                                    "crypto_received": amount,
                                    # most likely, funds are coming from the user/tax payer, but we can't say for sure so we use unknown
                                    # and let the DaLI transaction resolver fill in the missing details.
                                    "from_exchange": Keyword.UNKNOWN.value,
                                    "from_holder": Keyword.UNKNOWN.value,
                                    "to_exchange": self.__NEXO,
                                    "to_holder": self.account_holder,
                                    # we do know the spot price, but nexo seems to do some aggressive rounding
                                    "spot_price": spot_price,
                                    "crypto_sent": Keyword.UNKNOWN.value,
                                }
                            )
                        )
                    )
                else:
                    # TODO in my data, I had no withdrawals, they will need to be implemented in the future # pylint: disable=fixme
                    self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        return result
