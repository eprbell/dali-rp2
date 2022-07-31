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

# CSV Format: Transaction ID, Transaction Type, Date & time, Asset Acquired, "Quantity Acquired (Bought, Received, etc)" ,Cost Basis (incl. fees paid) (USD) ,Data Source   ,"Asset Disposed (Sold, Sent, etc)" ,Quantity Disposed ,Proceeds (excl. fees paid) (USD)

import logging
from csv import reader
from typing import Dict, List, Optional
from decimal import Decimal

from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal
from dali import transaction_resolver

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# transaction types
_BUY = "Buy"
_SEND = "Send"
_REWARD = "Reward"
_INCOMING = "Incoming"
_FORK = "Fork"
_AIRDROP = "Airdrop"
_CONVERT = "Convert"


class InputPlugin(AbstractInputPlugin):

    __COINBASE: str = "Coinbase"

    __TRANSACTION_ID_INDEX = 0
    __TRANSACTION_TYPE_INDEX = 1
    __TIMESTAMP_INDEX = 2
    __CURRENCY_INDEX = 3
    __ASSET_AMOUNT_INDEX = 4
    __COST_BASIS_INDEX = 5
    __SOLD_CURRENCY_INDEX = 7
    __SOLD_AMOUNT_INDEX = 8
    __SOLD_PROCEEDS_INDEX = 9

    __DELIMITER = ","

    def __init__(
        self,
        account_holder: str,
        transaction_csv_file: str,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__transaction_csv_file: str = transaction_csv_file
        self.__logger: logging.Logger = create_logger(f"{self.__COINBASE}/{self.account_holder}")

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

                transaction_id: str = line[self.__TRANSACTION_ID_INDEX].strip()
                transaction_type: str = line[self.__TRANSACTION_TYPE_INDEX].strip()

                if transaction_type == _CONVERT:
                    continue

                # there is no timezone information in the CSV, so we assume UTC
                timestamp_with_timezone = f"{line[self.__TIMESTAMP_INDEX].strip()} -00:00"

                currency_amount = None
                currency = None
                spot_price = None

                if transaction_type in [_BUY, _AIRDROP, _REWARD, _FORK]:
                    # spot price is not defined, but cost basis is. We can derive the spot price used for the transaction.
                    currency = line[self.__CURRENCY_INDEX].strip()
                    currency_amount = line[self.__ASSET_AMOUNT_INDEX].strip()
                    cost_basis = line[self.__COST_BASIS_INDEX].strip()
                    spot_price = str(Decimal(currency_amount) / Decimal(cost_basis))
                elif transaction_type == _SEND:
                    currency = line[self.__SOLD_CURRENCY_INDEX].strip()
                    currency_amount = line[self.__SOLD_AMOUNT_INDEX].strip()
                    cost_basis = line[self.__SOLD_PROCEEDS_INDEX].strip()
                    # in some cases the sold proceeds are not defined
                    spot_price = str(Decimal(currency_amount) / Decimal(cost_basis)) if cost_basis != "0" else Keyword.UNKNOWN.value
                elif transaction_type == _INCOMING:
                    currency = line[self.__CURRENCY_INDEX].strip()
                    currency_amount = line[self.__ASSET_AMOUNT_INDEX].strip()
                    spot_price = Keyword.UNKNOWN.value
                else:
                    raise Exception(f"Unhandled transaction type: {transaction_type}")

                assert currency is not None and currency_amount is not None and spot_price is not None

                common_params = {
                    "plugin": self.__COINBASE,
                    "unique_id": transaction_id,
                    "raw_data": raw_data,
                    "timestamp": timestamp_with_timezone,
                    "asset": currency,
                }

                # staking rewards & commission do not have any fees and only have a primary asset
                if transaction_type in [_REWARD, _AIRDROP, _FORK]:
                    result.append(
                        InTransaction(
                            **(
                                common_params
                                | {
                                    "exchange": self.__COINBASE,
                                    "holder": self.account_holder,
                                    "fiat_fee": "0",
                                    "transaction_type": Keyword.INCOME.value,
                                    "spot_price": spot_price,
                                    "crypto_in": currency_amount,
                                }
                            )
                        )
                    )
                elif transaction_type == _INCOMING:
                    result.append(
                        IntraTransaction(
                            **(
                                common_params
                                | {
                                    "crypto_sent": "0",
                                    "crypto_received": currency_amount,
                                    "spot_price": spot_price,
                                    "to_exchange": self.__COINBASE,
                                    "to_holder": self.account_holder,
                                    # most likely, funds are coming from the user/tax payer, but we can't say for sure so we use unknown
                                    # and let the user manually input the owner of these funds.
                                    "from_exchange": Keyword.UNKNOWN.value,
                                    "from_holder": Keyword.UNKNOWN.value,
                                }
                            )
                        )
                    )
                elif transaction_type == _SEND:
                    result.append(
                        IntraTransaction(
                            **(
                                common_params
                                | {
                                    "crypto_received": "0",
                                    "crypto_sent": currency_amount,
                                    "spot_price": spot_price,
                                    "from_exchange": self.__COINBASE,
                                    "from_holder": self.account_holder,
                                    # most likely, funds are coming from the user/tax payer, but we can't say for sure so we use unknown
                                    # and let the user manually input the owner of these funds.
                                    "to_exchange": Keyword.UNKNOWN.value,
                                    "to_holder": Keyword.UNKNOWN.value,
                                }
                            )
                        )
                    )
                elif transaction_type == _BUY:
                    result.append(
                        InTransaction(
                            **(
                                common_params
                                | {
                                    "exchange": self.__COINBASE,
                                    "holder": self.account_holder,
                                    "transaction_type": Keyword.BUY.value,
                                    "crypto_in": currency_amount,
                                    "spot_price": spot_price,
                                    # coinbase definitely charges a fee, and it's included in the cost basis
                                    # there is no breakout of the specific fee in the CSV, so we assume 0
                                    # "crypto_fee": "0",
                                }
                            )
                        )
                    )
                else:
                    # TODO I've seen some "Distrubition > Others" but there is no indication about what they respresent
                    # TODO in my data, I had no sells, they will need to be implemented in the future
                    self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        return result
