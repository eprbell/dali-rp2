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

# CSV Format:
# User_Id,Time,Category,Operation,Order_Id,Transaction_Id,Primary_Asset,Realized_Amount_For_Primary_Asset,Realized_Amount_For_Primary_Asset_In_USD_Value,Base_Asset,Realized_Amount_For_Base_Asset,Realized_Amount_For_Base_Asset_In_USD_Value,Quote_Asset,Realized_Amount_For_Quote_Asset,Realized_Amount_For_Quote_Asset_In_USD_Value,Fee_Asset,Realized_Amount_For_Fee_Asset,Realized_Amount_For_Fee_Asset_In_USD_Value,Payment_Method,Withdrawal_Method,Additional_Note # pylint: disable=line-too-long

import logging
from csv import reader
from decimal import Decimal
from typing import List, Optional, Dict

from rp2.logger import create_logger

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# transaction types
_CRYPTO_DEPOSIT = "Crypto Deposit"
_SELL = "Sell"
_BUY = "Buy"
_CRYPTO_WITHDRAWAL = "Crypto Withdrawal"
_USD_DEPOSIT = "USD Deposit"
_REFERRAL_COMMISSION = "Referral Commission"
_STAKING_REWARDS = "Staking Rewards"
_OTHERS = "Others"


class InputPlugin(AbstractInputPlugin):

    __BINANCE: str = "Binance.us"

    # amounts in binance tax CSVs are not comma separated and are nicely formatting floating point values

    __TIMESTAMP_INDEX = 1
    __TRANSACTION_CATEGORY_INDEX = 2
    __TRANSACTION_TYPE_INDEX = 3

    __PRIMARY_ASSET_INDEX = 6
    __PRIMARY_ASSET_AMOUNT_INDEX = 7
    __PRIMARY_ASSET_SPOT_PRICE_INDEX = 8

    __BASE_ASSET_INDEX = 9
    __BASE_ASSET_AMOUNT_INDEX = 10
    __BASE_ASSET_AMOUNT_SPOT_PRICE_INDEX = 11

    __QUOTE_ASSET_INDEX = 12
    __QUOTE_ASSET_AMOUNT_INDEX = 13
    __QUOTE_ASSET_AMOUNT_SPOT_PRICE_INDEX = 14

    __FEE_ASSET_INDEX = 15
    __FEE_ASSET_AMOUNT_INDEX = 16
    __FEE_ASSET_AMOUNT_SPOT_PRICE_INDEX = 17

    __DELIMITER = ","

    def __init__(
        self,
        account_holder: str,
        transaction_csv_file: str,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__transaction_csv_file: str = transaction_csv_file
        self.__logger: logging.Logger = create_logger(f"{self.__BINANCE}/{self.account_holder}")

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
                category = line[self.__TRANSACTION_CATEGORY_INDEX].strip()

                # there is no timezone information in the CSV, so we assume UTC
                timestamp_with_timezone = f"{line[self.__TIMESTAMP_INDEX].strip()} -00:00"

                common_params = {
                    "plugin": self.__BINANCE,
                    "unique_id": Keyword.UNKNOWN.value,
                    "raw_data": raw_data,
                    "timestamp": timestamp_with_timezone,
                    "notes": f"{category} - {transaction_type}",
                }

                # in binance, you can pay fees with lots of different currencies
                # because of this, we represent fees on transactions as a separate fee transaction since it could be a different currency
                # that what is being purchased or sold.
                # More info: https://github.com/eprbell/rp2/blob/main/docs/user_faq.md#how-to-represent-fiat-vs-crypto-transaction-fees

                # staking rewards & commission do not have any fees and only have a primary asset
                # it is unclear what 'Distribution > Other' represents, but it looks like some type of income
                if transaction_type in [_REFERRAL_COMMISSION, _STAKING_REWARDS] or (category == "Distribution" and transaction_type == _OTHERS):
                    granular_transaction_type = Keyword.INCOME if transaction_type == _REFERRAL_COMMISSION else Keyword.INTEREST
                    currency = line[self.__PRIMARY_ASSET_INDEX].strip()

                    crypto_amount = line[self.__PRIMARY_ASSET_AMOUNT_INDEX].strip()
                    calculated_spot_price = Decimal(line[self.__PRIMARY_ASSET_SPOT_PRICE_INDEX].strip()) / Decimal(crypto_amount)

                    result.append(
                        InTransaction(
                            **(
                                common_params  # type: ignore
                                | {
                                    "asset": currency,
                                    "exchange": self.__BINANCE,
                                    "holder": self.account_holder,
                                    "transaction_type": granular_transaction_type.value,
                                    "crypto_in": crypto_amount,
                                    "spot_price": str(calculated_spot_price),
                                    "fiat_fee": "0",
                                }
                            )
                        )
                    )
                elif transaction_type == _CRYPTO_DEPOSIT:
                    currency = line[self.__PRIMARY_ASSET_INDEX].strip()
                    crypto_amount = line[self.__PRIMARY_ASSET_AMOUNT_INDEX].strip()
                    calculated_spot_price = Decimal(line[self.__PRIMARY_ASSET_SPOT_PRICE_INDEX].strip()) / Decimal(crypto_amount)

                    result.append(
                        IntraTransaction(
                            **(
                                common_params  # type: ignore
                                | {
                                    "asset": currency,
                                    "crypto_sent": "0",
                                    "crypto_received": crypto_amount,
                                    "spot_price": str(calculated_spot_price),
                                    # most likely, funds are coming from the user/tax payer, but we can't say for sure so we use unknown
                                    # and let the user manually input the owner of these funds.
                                    "from_exchange": Keyword.UNKNOWN.value,
                                    "from_holder": Keyword.UNKNOWN.value,
                                    "to_exchange": self.__BINANCE,
                                    "to_holder": self.account_holder,
                                }
                            )
                        )
                    )
                elif transaction_type == _CRYPTO_WITHDRAWAL:
                    currency = line[self.__PRIMARY_ASSET_INDEX].strip()
                    crypto_amount = line[self.__PRIMARY_ASSET_AMOUNT_INDEX].strip()
                    calculated_spot_price = Decimal(line[self.__PRIMARY_ASSET_SPOT_PRICE_INDEX].strip()) / Decimal(crypto_amount)

                    result.append(
                        IntraTransaction(
                            **(
                                common_params  # type: ignore
                                | {
                                    "crypto_received": "0",
                                    # withdrawals happen in the primary asset field
                                    "asset": currency,
                                    "crypto_sent": crypto_amount,
                                    "spot_price": str(calculated_spot_price),
                                    "from_exchange": self.__BINANCE,
                                    "from_holder": self.account_holder,
                                    # most likely, funds are coming from the user/tax payer, but we can't say for sure so we use unknown
                                    # and let the user manually input the owner of these funds.
                                    "to_exchange": Keyword.UNKNOWN.value,
                                    "to_holder": Keyword.UNKNOWN.value,
                                }
                            )
                        )
                    )

                    result.append(
                        OutTransaction(
                            **(
                                common_params  # type: ignore
                                | {
                                    "notes": f"Fee for {category} - {transaction_type}",
                                }
                                | self.generate_fee_parameters(line)
                            )
                        )
                    )
                elif transaction_type == _BUY:
                    # in the case of a "Quick Buy" the fields seem to be swapped: the base asset is the quote asset (the currency being used to purchase)
                    # and the quote asset is the asset being purchased. For instance, buying BUSD with USD is represented as:
                    # 52358478,2021-08-04 16:15:55.614,Quick Buy,Buy,{32 char txn id},{9 char id},,,,USD,30.00000000,30.00000000,BUSD,29.84000000,29.84542100,USD,0.15000000,0.15000000,ACH,, # pylint: disable=line-too-long

                    if category == "Quick Buy":
                        purchased_asset = line[self.__QUOTE_ASSET_INDEX].strip()
                        crypto_amount = line[self.__QUOTE_ASSET_AMOUNT_INDEX].strip()
                        calculated_spot_price = Decimal(line[self.__QUOTE_ASSET_AMOUNT_SPOT_PRICE_INDEX].strip()) / Decimal(crypto_amount)
                    else:
                        purchased_asset = line[self.__BASE_ASSET_INDEX].strip()
                        crypto_amount = line[self.__BASE_ASSET_AMOUNT_INDEX].strip()
                        calculated_spot_price = Decimal(line[self.__BASE_ASSET_AMOUNT_SPOT_PRICE_INDEX].strip()) / Decimal(crypto_amount)

                    result.append(
                        InTransaction(
                            **(
                                common_params  # type: ignore
                                | {
                                    "exchange": self.__BINANCE,
                                    "holder": self.account_holder,
                                    "transaction_type": Keyword.BUY.value,
                                    "asset": purchased_asset,
                                    "crypto_in": crypto_amount,
                                    "spot_price": str(calculated_spot_price),
                                }
                            )
                        )
                    )

                    if category == "Quick Buy":
                        quote_asset: str = line[self.__BASE_ASSET_INDEX].strip()
                        quote_crypto_amount = line[self.__BASE_ASSET_AMOUNT_INDEX].strip()
                        calculated_quote_spot_price = Decimal(line[self.__BASE_ASSET_AMOUNT_SPOT_PRICE_INDEX].strip()) / Decimal(crypto_amount)
                    else:
                        quote_asset = line[self.__QUOTE_ASSET_INDEX].strip()
                        quote_crypto_amount = line[self.__QUOTE_ASSET_AMOUNT_INDEX].strip()
                        calculated_quote_spot_price = Decimal(line[self.__QUOTE_ASSET_AMOUNT_SPOT_PRICE_INDEX].strip()) / Decimal(quote_crypto_amount)

                    result.append(
                        OutTransaction(
                            **(
                                common_params  # type: ignore
                                | {
                                    "exchange": self.__BINANCE,
                                    "holder": self.account_holder,
                                    "transaction_type": Keyword.SELL.value,
                                    "asset": quote_asset,
                                    "crypto_out_no_fee": quote_crypto_amount,
                                    "crypto_fee": "0",
                                    "spot_price": str(calculated_quote_spot_price),
                                }
                            )
                        )
                    )

                    fee_params = self.generate_fee_parameters(line)
                    if fee_params:
                        result.append(
                            OutTransaction(
                                **(
                                    common_params  # type: ignore
                                    | {
                                        "notes": f"Fee for {category} - {transaction_type}",
                                    }
                                    | fee_params
                                )
                            )
                        )
                elif transaction_type == _SELL:
                    crypto_amount = line[self.__BASE_ASSET_AMOUNT_INDEX].strip()
                    calculated_spot_price = Decimal(line[self.__BASE_ASSET_AMOUNT_SPOT_PRICE_INDEX].strip()) / Decimal(crypto_amount)

                    result.append(
                        OutTransaction(
                            **(
                                common_params  # type: ignore
                                | {
                                    "exchange": self.__BINANCE,
                                    "holder": self.account_holder,
                                    "transaction_type": Keyword.SELL.value,
                                    "asset": line[self.__BASE_ASSET_INDEX].strip(),
                                    "crypto_out_no_fee": crypto_amount,
                                    "crypto_fee": "0",
                                    "spot_price": str(calculated_spot_price),
                                }
                            )
                        )
                    )

                    quote_crypto_amount = line[self.__QUOTE_ASSET_AMOUNT_INDEX].strip()
                    calculated_quote_spot_price = Decimal(line[self.__QUOTE_ASSET_AMOUNT_SPOT_PRICE_INDEX].strip()) / Decimal(quote_crypto_amount)

                    result.append(
                        InTransaction(
                            **(
                                common_params  # type: ignore
                                | {
                                    "exchange": self.__BINANCE,
                                    "holder": self.account_holder,
                                    "transaction_type": Keyword.BUY.value,
                                    "asset": line[self.__QUOTE_ASSET_INDEX].strip(),
                                    "crypto_in": quote_crypto_amount,
                                    "spot_price": str(calculated_quote_spot_price),
                                }
                            )
                        )
                    )

                    fee_params = self.generate_fee_parameters(line)
                    if fee_params:
                        result.append(
                            OutTransaction(
                                **(
                                    common_params  # type: ignore
                                    | {
                                        "notes": f"Fee for {category} - {transaction_type}",
                                    }
                                    | fee_params
                                )
                            )
                        )
                elif transaction_type == _USD_DEPOSIT:
                    # we only care when USD is used to buy something, so we can skip the deposit entries
                    self.__logger.debug("Skipping USD deposit %s", raw_data)
                else:
                    # TODO in my data, I had no withdrawals, they will need to be implemented in the future # pylint: disable=fixme
                    self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        return result

    def generate_fee_parameters(self, line: list[str]) -> Dict[str, str]:
        fee_amount = Decimal(line[self.__FEE_ASSET_AMOUNT_INDEX].strip())

        if fee_amount.is_zero():
            return {}

        fee_realized_usd = Decimal(line[self.__FEE_ASSET_AMOUNT_SPOT_PRICE_INDEX].strip())
        calculated_spot_price = fee_realized_usd / fee_amount

        return {
            "asset": line[self.__FEE_ASSET_INDEX].strip(),
            # `fee` transaction_types must have a zero specified for crypto_out_no_fee
            "crypto_out_no_fee": "0",
            "crypto_fee": str(fee_amount),
            "spot_price": str(calculated_spot_price),
            "exchange": self.__BINANCE,
            "holder": self.account_holder,
            "transaction_type": Keyword.FEE.value,
        }
