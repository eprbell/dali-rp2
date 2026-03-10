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

# This plugin parses the export from Strike Bitcoin wallet.
# CSV Format:
# 0: Transaction ID (UUID)
# 1: Time (UTC) - e.g., "Sep 12 2024 05:00:14"
# 2: Status - e.g., "Completed"
# 3: Transaction Type - Deposit, Purchase, Send, Receive
# 4: Amount USD - positive for deposits, negative for purchases
# 5: Fee USD
# 6: Amount BTC - positive for receive/purchase, negative for send
# 7: Fee BTC
# 8: Description
# 9: Exchange Rate
# 10: Transaction Hash (crypto transaction hash)


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
from dali.intra_transaction import IntraTransaction


class InputPlugin(AbstractInputPlugin):
    __STRIKE: str = "Strike"

    # Column indices
    __TRANSACTION_ID_INDEX: int = 0
    __TIME_INDEX: int = 1
    __STATUS_INDEX: int = 2
    __TRANSACTION_TYPE_INDEX: int = 3
    __AMOUNT_USD_INDEX: int = 4
    __FEE_USD_INDEX: int = 5
    __AMOUNT_BTC_INDEX: int = 6
    __FEE_BTC_INDEX: int = 7
    __DESCRIPTION_INDEX: int = 8
    __EXCHANGE_RATE_INDEX: int = 9
    __TRANSACTION_HASH_INDEX: int = 10

    __DELIMITER = ","
    __CURRENCY = "BTC"

    # Transaction types
    __DEPOSIT: str = "Deposit"
    __PURCHASE: str = "Purchase"
    __SEND: str = "Send"
    __RECEIVE: str = "Receive"

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

        self.__logger: logging.Logger = create_logger(f"{self.__STRIKE}/{self.__account_nickname}/{self.account_holder}")

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

                transaction_type: str = line[self.__TRANSACTION_TYPE_INDEX].strip()
                amount_btc: str = line[self.__AMOUNT_BTC_INDEX].strip() if self.__AMOUNT_BTC_INDEX < len(line) else ""
                amount_usd: str = line[self.__AMOUNT_USD_INDEX].strip() if self.__AMOUNT_USD_INDEX < len(line) else ""
                fee_usd: str = line[self.__FEE_USD_INDEX].strip() if self.__FEE_USD_INDEX < len(line) else ""
                fee_btc: str = line[self.__FEE_BTC_INDEX].strip() if self.__FEE_BTC_INDEX < len(line) else ""
                description: str = line[self.__DESCRIPTION_INDEX].strip() if self.__DESCRIPTION_INDEX < len(line) else ""
                exchange_rate: str = line[self.__EXCHANGE_RATE_INDEX].strip() if self.__EXCHANGE_RATE_INDEX < len(line) else ""
                transaction_hash: str = line[self.__TRANSACTION_HASH_INDEX].strip() if self.__TRANSACTION_HASH_INDEX < len(line) else ""
                unique_id: str = line[self.__TRANSACTION_ID_INDEX].strip() if self.__TRANSACTION_ID_INDEX < len(line) else ""

                # Parse timestamp
                timestamp_str: str = line[self.__TIME_INDEX].strip() if self.__TIME_INDEX < len(line) else ""
                try:
                    # Handle format like "Sep 12 2024 05:00:14"
                    timestamp_value: datetime = datetime.strptime(timestamp_str, "%b %d %Y %H:%M:%S")
                    timestamp_value = self.__timezone.localize(timestamp_value)
                except ValueError as e:
                    self.__logger.warning("Failed to parse timestamp '%s': %s", timestamp_str, e)
                    continue

                # Handle different transaction types
                if transaction_type == self.__RECEIVE:
                    # Receive: crypto comes into wallet (no USD involved)
                    if not amount_btc:
                        self.__logger.warning("Receive transaction missing BTC amount, skipping: %s", raw_data)
                        continue

                    btc_amount = RP2Decimal(amount_btc)
                    if btc_amount <= ZERO:
                        self.__logger.warning("Receive transaction has non-positive BTC amount: %s", amount_btc)
                        continue

                    result.append(
                        IntraTransaction(
                            plugin=self.__STRIKE,
                            unique_id=unique_id or transaction_hash or Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=f"{timestamp_value}",
                            asset=self.__CURRENCY,
                            from_exchange=Keyword.UNKNOWN.value,
                            from_holder=Keyword.UNKNOWN.value,
                            to_exchange=self.__account_nickname,
                            to_holder=self.account_holder,
                            spot_price=exchange_rate if exchange_rate else Keyword.UNKNOWN.value,
                            crypto_sent=Keyword.UNKNOWN.value,
                            crypto_received=str(btc_amount),
                            notes=description if description else None,
                        )
                    )

                elif transaction_type == self.__SEND:
                    # Send: crypto leaves wallet (no USD involved)
                    if not amount_btc:
                        self.__logger.warning("Send transaction missing BTC amount, skipping: %s", raw_data)
                        continue

                    btc_amount = RP2Decimal(amount_btc)
                    if btc_amount >= ZERO:
                        self.__logger.warning("Send transaction has non-negative BTC amount: %s", amount_btc)
                        continue

                    # Include fee in sent amount
                    total_sent = abs(btc_amount)
                    if fee_btc:
                        fee = RP2Decimal(fee_btc)
                        total_sent = total_sent + fee

                    result.append(
                        IntraTransaction(
                            plugin=self.__STRIKE,
                            unique_id=unique_id or transaction_hash or Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=f"{timestamp_value}",
                            asset=self.__CURRENCY,
                            from_exchange=self.__account_nickname,
                            from_holder=self.account_holder,
                            to_exchange=Keyword.UNKNOWN.value,
                            to_holder=Keyword.UNKNOWN.value,
                            spot_price=exchange_rate if exchange_rate else Keyword.UNKNOWN.value,
                            crypto_sent=str(total_sent),
                            crypto_received=Keyword.UNKNOWN.value,
                            notes=description if description else None,
                        )
                    )

                elif transaction_type == self.__PURCHASE:
                    # Purchase: fiat out (negative USD), crypto in (positive BTC)
                    if not amount_usd or not amount_btc:
                        self.__logger.warning("Purchase transaction missing USD or BTC amount, skipping: %s", raw_data)
                        continue

                    btc_amount = RP2Decimal(amount_btc)
                    if btc_amount <= ZERO:
                        self.__logger.warning("Purchase transaction has non-positive BTC amount: %s", amount_btc)
                        continue

                    # For purchase, USD is negative (fiat spent), BTC is positive (crypto received)
                    result.append(
                        IntraTransaction(
                            plugin=self.__STRIKE,
                            unique_id=unique_id or transaction_hash or Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=f"{timestamp_value}",
                            asset=self.__CURRENCY,
                            from_exchange=Keyword.UNKNOWN.value,
                            from_holder=Keyword.UNKNOWN.value,
                            to_exchange=self.__account_nickname,
                            to_holder=self.account_holder,
                            spot_price=exchange_rate if exchange_rate else Keyword.UNKNOWN.value,
                            crypto_sent=Keyword.UNKNOWN.value,
                            crypto_received=str(btc_amount),
                            notes=description if description else None,
                        )
                    )

                elif transaction_type == self.__DEPOSIT:
                    # Deposit: fiat deposit (positive USD), no crypto involved
                    # This is a fiat-only transaction - we'll log it but skip for now
                    # as it doesn't involve crypto movement
                    self.__logger.debug("Deposit transaction (fiat only): %s USD - skipping crypto transaction", amount_usd)
                    # TODO: Handle fiat deposits if needed - would need different transaction type

                else:
                    self.__logger.warning("Unknown transaction type: %s", transaction_type)

        return result