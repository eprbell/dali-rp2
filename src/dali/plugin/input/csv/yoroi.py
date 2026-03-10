# Copyright 2026 anlach
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

# This plugin parses the export from Yoroi wallet.
# CSV Format: 
# 0: "Type (Trade, IN or OUT)", 
# 1: "Buy Amount",
# 2: "Buy Cur.",
# 3: "Sell Amount",
# 4: "Sell Cur.",
# 5: "Fee Amount (optional)",
# 6: "Fee Cur. (optional)",
# 7: "Exchange (optional)",
# 8: "Trade Group (optional)",
# 9: "Comment (optional)",
# 10: "Date",
# 11: "ID"


import logging
from csv import reader
from datetime import datetime
from typing import List, Optional

import pytz
from dateutil.parser import parse
from rp2.abstract_country import AbstractCountry
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction

_SENT: str = "Withdrawal"
_RECV: str = "Deposit"


class InputPlugin(AbstractInputPlugin):
    __YOROI: str = "Yoroi"

    __TYPE_INDEX: int = 0
    __DATETIME_INDEX: int = 10
    __TRANSACTION_ID_INDEX: int = 11
    __FEE_INDEX: int = 5
    __FEE_CURRENCY_INDEX: int = 6
    __BUY_AMOUNT_INDEX: int = 1
    __BUY_CURRENCY_INDEX: int = 2
    __SELL_AMOUNT_INDEX: int = 3
    __SELL_CURRENCY_INDEX: int = 4
    __COMMENT_INDEX: int = 9

    __DELIMITER = ","

    def __init__(
        self,
        account_holder: str,
        account_nickname: str,
        csv_file: str,
        timezone: str,
        native_fiat: Optional[str] = None,
    ) -> None:
        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__account_nickname: str = account_nickname
        self.__csv_file: str = csv_file
        self.__timezone = pytz.timezone(timezone)

        self.__logger: logging.Logger = create_logger(f"{self.__YOROI}/{self.__account_nickname}/{self.account_holder}")

    def load(self, country: AbstractCountry) -> List[AbstractTransaction]:
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
                self.__logger.debug("Transaction: %s", raw_data)

                timestamp_value: datetime = parse(line[self.__DATETIME_INDEX])
                timestamp_value = self.__timezone.normalize(self.__timezone.localize(timestamp_value))

                transaction_type: str = line[self.__TYPE_INDEX]
                spot_price: str = Keyword.UNKNOWN.value
                crypto_hash: str = line[self.__TRANSACTION_ID_INDEX] if line[self.__TRANSACTION_ID_INDEX] else Keyword.UNKNOWN.value
                currency: str = None
                if transaction_type == _RECV:
                    currency = line[self.__BUY_CURRENCY_INDEX]
                    amount_number = RP2Decimal(line[self.__BUY_AMOUNT_INDEX])
                elif transaction_type == _SENT:
                    currency = line[self.__SELL_CURRENCY_INDEX]
                    amount_number = RP2Decimal(line[self.__SELL_AMOUNT_INDEX])
                else:
                    self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)
                    continue
                fee_currency: str = line[self.__FEE_CURRENCY_INDEX] if line[self.__FEE_CURRENCY_INDEX] else None
                if fee_currency and fee_currency == currency and line[self.__FEE_INDEX]:
                    fee_number: RP2Decimal = RP2Decimal(line[self.__FEE_INDEX])
                else:
                    fee_number = ZERO

                if amount_number == ZERO and fee_number > ZERO:
                    self.__logger.warning("Possible dusting attack (fee > 0, total = 0), skipping transaction: %s", raw_data)
                    continue

                # Check for staking rewards (Deposit with "Staking Reward" in comment)
                comment: str = line[self.__COMMENT_INDEX] if len(line) > self.__COMMENT_INDEX and line[self.__COMMENT_INDEX] else ""
                is_staking_reward: bool = transaction_type == _RECV and "Staking Reward" in comment

                if is_staking_reward:
                    # Create InTransaction for staking rewards
                    result.append(
                        InTransaction(
                            plugin=self.__YOROI,
                            unique_id=crypto_hash,
                            raw_data=raw_data,
                            timestamp=f"{timestamp_value}",
                            asset=currency,
                            exchange=self.__account_nickname,
                            holder=self.account_holder,
                            transaction_type=Keyword.STAKING.value,
                            spot_price=spot_price,
                            crypto_in=str(amount_number),
                            notes=comment,
                        )
                    )
                elif transaction_type in {_RECV, _SENT}:
                    result.append(
                        IntraTransaction(
                            plugin=self.__YOROI,
                            unique_id=crypto_hash,
                            raw_data=raw_data,
                            timestamp=f"{timestamp_value}",
                            asset=currency,
                            from_exchange=self.__account_nickname if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            from_holder=self.account_holder if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            to_exchange=self.__account_nickname if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            to_holder=self.account_holder if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            spot_price=spot_price,
                            crypto_sent=str(amount_number + fee_number) if transaction_type == _SENT else Keyword.UNKNOWN.value,
                            crypto_received=str(amount_number) if transaction_type == _RECV else Keyword.UNKNOWN.value,
                            notes=None,
                        )
                    )
                else:
                    self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        return result
