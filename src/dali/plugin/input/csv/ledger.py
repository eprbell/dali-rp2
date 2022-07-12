# Copyright 2022  Steve Davis
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

# CSV Format:
#   0: "Operation Date"  # For example, "2022-06-05T00:39:14.007Z"
#   1: "Currency Ticker"
#   2: "Operation Type"
#   3: "Operation Amount"
#   4: "Operation Fees"  # This value is often missing or incorrect for receive transactions
#   5: "Operation Hash"
#   6: "Account Name"
#   7: "Account xpub"  # Public key
#   8: "Countervalue Ticker"  # Fiat ticker
#   9: "Countervalue at Operation Date"  # Total fiat value
#  10: "Countervalue at CSV Export"
#
# Note: the Ledger Live software displays this warning message when exporting operation history:
#       The countervalues in the export is provided for information
#       purposes only. Do not rely on such data for accounting, tax,
#       regulation or legal purposes, as they only represent an
#       estimation of the price of the assets at the time of transactions
#       and export, under the valuation methods provided by our
#       service provider, Kaiko


import logging
from csv import reader
from datetime import datetime
from typing import List, Optional

import dateutil
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.intra_transaction import IntraTransaction

_SENT: str = "OUT"
_RECV: str = "IN"


class InputPlugin(AbstractInputPlugin):

    __LEDGER: str = "Ledger"

    __TIMESTAMP_INDEX: int = 0
    __CURRENCY_INDEX: int = 1
    __OPERATION_TYPE_INDEX: int = 2
    __QUANTITY_INDEX: int = 3
    __FEE_INDEX: int = 4
    __TRANSACTION_ID_INDEX: int = 5

    __DELIMITER = ","

    __CURRENCY_ALIAS_DICT = {
        "FANTOM": "FTM",
    }

    def __init__(
        self,
        account_holder: str,
        account_nickname: str,
        csv_file: str,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__account_nickname: str = account_nickname
        self.__csv_file: str = csv_file
        self.__logger: logging.Logger = create_logger(f"{self.__LEDGER}/{self.__account_nickname}/{self.account_holder}")

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
                timestamp_value: datetime = dateutil.parser.isoparse(timestamp)  # For example, "2022-06-05T00:39:14.007Z"
                self.__logger.debug("Transaction: %s", raw_data)
                currency: str = line[self.__CURRENCY_INDEX]
                currency = self.__CURRENCY_ALIAS_DICT.get(currency, currency)
                transaction_type: str = line[self.__OPERATION_TYPE_INDEX]
                spot_price: str = Keyword.UNKNOWN.value
                crypto_hash: str = line[self.__TRANSACTION_ID_INDEX]
                fee_str: str = line[self.__FEE_INDEX]
                fee_number: RP2Decimal = RP2Decimal(fee_str) if fee_str else ZERO  # Fee is sometimes missing
                quantity_number: RP2Decimal = RP2Decimal(line[self.__QUANTITY_INDEX])

                if quantity_number == ZERO and fee_number > ZERO:
                    self.__logger.warning("Possible dusting attack (fee > 0, total amount = 0): %s", raw_data)
                    continue
                if transaction_type in {_SENT, _RECV}:  # Need example data for sent transactions, untested as of 7/9/2022
                    result.append(
                        IntraTransaction(
                            plugin=self.__LEDGER,
                            unique_id=crypto_hash,
                            raw_data=raw_data,
                            timestamp=f"{timestamp_value}",
                            asset=currency,
                            from_exchange=self.__account_nickname if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            from_holder=self.account_holder if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            to_exchange=self.__account_nickname if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            to_holder=self.account_holder if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            spot_price=spot_price,
                            crypto_sent=str(quantity_number + fee_number) if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            crypto_received=str(quantity_number) if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            notes=None,
                        )
                    )
                else:
                    self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        return result
