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
from typing import Dict, List, Optional

from rp2.logger import create_logger
from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2RuntimeError

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

_ACH_DEPOSIT: str = "Ach Deposit"
_ACH_WITHDRAWAL: str = "Ach Withdrawal"
_BUY_CURRENCY: str = "Buy Currency"
_BUY_QUANTITY: str = "Buy Quantity"
_CRYPTO_TRANSFER: str = "Crypto Transfer"
_DATE: str = "Date"
_INTEREST_PAYMENT: str = "Interest Payment"
_REFERRAL_BONUS: str = "Referral Bonus"
_BONUS_PAYMENT = "Bonus Payment"
_SOLD_CURRENCY: str = "Sold Currency"
_SOLD_QUANTITY: str = "Sold Quantity"
_TRADE: str = "Trade"
_TRADE_ID: str = "Trade ID"
_TYPE: str = "Type"
_WITHDRAWAL: str = "Withdrawal"
_BIA_WITHDRAWAL: str = "BIA Withdraw"
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
        transaction_csv_file: str,
        trade_csv_file: Optional[str] = None,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__transaction_csv_file: str = transaction_csv_file
        self.__trade_csv_file: Optional[str] = trade_csv_file
        self.__logger: logging.Logger = create_logger(f"{self.__BLOCKFI}/{self.account_holder}")

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        last_withdrawal_fee: Optional[RP2Decimal] = None
        with open(self.__transaction_csv_file, encoding="utf-8") as transaction_csv_file:
            lines = reader(transaction_csv_file)
            # Skip header line
            header = next(lines)
            self.__logger.debug("Header: %s", header)
            for line in lines:
                raw_data: str = self.__DELIMITER.join(line)
                self.__logger.debug("Transaction: %s", raw_data)

                if last_withdrawal_fee is not None and line[self.__TYPE_INDEX] != _WITHDRAWAL:
                    raise RP2RuntimeError(f"Internal error: withdrawal fee {last_withdrawal_fee} is not followed by withdrawal")

                transaction_type: str = line[self.__TYPE_INDEX]
                if transaction_type == _INTEREST_PAYMENT:
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
                            transaction_type=Keyword.INTEREST.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_in=line[self.__AMOUNT_INDEX],
                            fiat_fee="0",
                        )
                    )
                elif transaction_type in [_REFERRAL_BONUS, _BONUS_PAYMENT]:
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
                            transaction_type=Keyword.INCOME.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_in=line[self.__AMOUNT_INDEX],
                            fiat_fee="0",
                            notes="Referral Bonus",
                        )
                    )
                elif transaction_type == _CRYPTO_TRANSFER:
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
                elif transaction_type == _ACH_WITHDRAWAL:
                    last_withdrawal_fee = None
                    result.append(
                        OutTransaction(
                            plugin=self.__BLOCKFI,
                            unique_id=Keyword.UNKNOWN.value,
                            raw_data=raw_data,
                            timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                            asset=line[self.__CURRENCY_INDEX],
                            exchange=self.__BLOCKFI,
                            holder=self.account_holder,
                            transaction_type=Keyword.SELL.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_out_no_fee=str(-RP2Decimal(line[self.__AMOUNT_INDEX])),
                            crypto_fee="0",
                            notes="ACH withdrawal",
                        )
                    )
                elif transaction_type == _WITHDRAWAL:
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
                elif transaction_type == _ACH_DEPOSIT:
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
                            transaction_type=Keyword.BUY.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_in=line[self.__AMOUNT_INDEX],
                            fiat_fee="0",
                            notes="ACH deposit",
                        )
                    )
                elif transaction_type == _TRADE:
                    # Trades will be handled by parsing trade_report_all.csv
                    # export
                    continue
                elif transaction_type == _BIA_WITHDRAWAL:
                    # these withdrawals are internal transfers within blockfi which is why they are skipped
                    # https://github.com/eprbell/dali-rp2/pull/64
                    self.__logger.debug("BIA Withdraw: %s", raw_data)
                    continue
                elif transaction_type == _WITHDRAWAL_FEE:
                    last_withdrawal_fee = RP2Decimal(line[self.__AMOUNT_INDEX])
                    last_withdrawal_fee = -last_withdrawal_fee  # type: ignore
                else:
                    self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        if self.__trade_csv_file:
            result += self.parse_trade_report(self.__trade_csv_file)
        return result

    def parse_trade_report(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)
            # skip csv header
            header = next(lines)
            self.__logger.debug("Header: %s", header)

            column_index: Dict[str, int] = {}
            for (index, name) in enumerate(header):
                column_index[name] = index

            for line in lines:
                raw_data: str = self.__DELIMITER.join(line)
                self.__logger.debug("Transaction: %s", raw_data)

                transaction_type: str = line[column_index[_TYPE]]
                if transaction_type != "Trade":
                    raise RP2RuntimeError(f"Internal error: unsupported transaction type: {transaction_type}")

                trade_id: str = line[column_index[_TRADE_ID]]
                date: str = line[column_index[_DATE]]
                timestamp: str = f"{date} -00:00"
                from_currency: str = line[column_index[_SOLD_CURRENCY]].upper()
                from_size: str = line[column_index[_SOLD_QUANTITY]]
                to_currency: str = line[column_index[_BUY_CURRENCY]].upper()
                to_size: str = line[column_index[_BUY_QUANTITY]]

                result.append(
                    OutTransaction(
                        plugin=self.__BLOCKFI,
                        unique_id=trade_id,
                        raw_data=raw_data,
                        timestamp=timestamp,
                        asset=from_currency,
                        exchange=self.__BLOCKFI,
                        holder=self.account_holder,
                        transaction_type="Sell",
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_out_no_fee=from_size,
                        crypto_fee="0",
                        notes=f"Sell side of trade: {from_size} {from_currency} -> {to_size} {to_currency}",
                    ),
                )
                result.append(
                    InTransaction(
                        plugin=self.__BLOCKFI,
                        unique_id=f"{trade_id}/buy",
                        raw_data=raw_data,
                        timestamp=timestamp,
                        asset=to_currency,
                        exchange=self.__BLOCKFI,
                        holder=self.account_holder,
                        transaction_type="Buy",
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_in=to_size,
                        fiat_fee="0",
                        notes=f"Buy side of trade: {from_size} {from_currency} -> {to_size} {to_currency}",
                    ),
                )

        return result
