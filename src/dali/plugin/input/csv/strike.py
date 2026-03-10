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
# 0: Transaction ID,
# 1: Time (UTC),
# 2: Status,
# 3: Transaction Type,
# 4: Amount USD,
# 5: Fee USD,
# 6: Amount BTC,
# 7: Fee BTC,
# 8: Description,
# 9: Exchange Rate,
# 10: Transaction Hash



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
    __ELECTRUM: str = "Electrum"

    __OC_TRANSACTION_HASH_INDEX: int = 0
    __LN_PAYMENT_HASH_INDEX: int = 1
    __LABEL_INDEX: int = 2
    __CONFIRMATIONS_INDEX: int = 3
    __AMOUNT_CHAIN_BC_INDEX: int = 4
    __AMOUNT_LIGHTNING_BC_INDEX: int = 5
    __FIAT_VALUE_INDEX: int = 6
    __NETWORK_FEE_SATOSHI_INDEX: int = 7
    __FIAT_FEE_INDEX: int = 8
    __TIMESTAMP_INDEX: int = 9
    __CURRENCY = "BTC"
    __DELIMITER = ","
    __SATS_PER_BTC = RP2Decimal("100000000")

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

        self.__logger: logging.Logger = create_logger(f"{self.__ELECTRUM}/{self.__account_nickname}/{self.account_holder}")

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

                timestamp_value: datetime = parse(line[self.__TIMESTAMP_INDEX])
                timestamp_value = self.__timezone.normalize(self.__timezone.localize(timestamp_value))

                amount: RP2Decimal = RP2Decimal(line[self.__AMOUNT_CHAIN_BC_INDEX]) if line[self.__AMOUNT_CHAIN_BC_INDEX] else RP2Decimal(line[self.__AMOUNT_LIGHTNING_BC_INDEX])
                is_deposit = amount > ZERO
                print("amount and fee", amount, line[self.__NETWORK_FEE_SATOSHI_INDEX])
                if not is_deposit:
                    amount = amount * RP2Decimal("-1") + RP2Decimal(line[self.__NETWORK_FEE_SATOSHI_INDEX])/self.__SATS_PER_BTC

                result.append(
                    IntraTransaction(
                        plugin=self.__ELECTRUM,
                        unique_id=line[self.__OC_TRANSACTION_HASH_INDEX],
                        raw_data=raw_data,
                        timestamp=f"{timestamp_value}",
                        asset=self.__CURRENCY,
                        from_exchange=self.__ELECTRUM if not is_deposit else Keyword.UNKNOWN.value,
                        from_holder=self.account_holder,
                        to_exchange=self.__ELECTRUM if is_deposit else Keyword.UNKNOWN.value,
                        to_holder=self.account_holder,
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_sent=str(amount) if not is_deposit else Keyword.UNKNOWN.value,
                        crypto_received=str(amount) if is_deposit else Keyword.UNKNOWN.value,
                        notes=line[self.__LABEL_INDEX],
                    )
                )

        return result
