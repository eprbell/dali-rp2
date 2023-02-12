# Copyright 2022 macanudo527
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

# This plugin uses the sheet found under the "for-cointracker" because of its simpler format.
# Trades CSV Format: timestamp UTC, received quantity, received currency, sent quantity,
#    sent currency, fee amount, fee currency, tag

# The transfers CSV can be found under the "depositwithdrawal" tab
# transfer CSV format: timestamp UTC, transaction type, amount, coin (prefixed with the network used), network, txid, fee

import logging
from csv import reader
from typing import List, Optional

from rp2.logger import create_logger

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction


class InputPlugin(AbstractInputPlugin):

    __PIONEX: str = "Pionex"
    __PIONEX_PLUGIN: str = "Pionex_CSV"

    __TIMESTAMP_INDEX: int = 0
    __RECEIVED_AMOUNT: int = 1
    __TRANSACTION_TYPE: int = 1
    __ASSET_RECEIVED: int = 2
    __AMOUNT_TRANSFERED: int = 2
    __SENT_AMOUNT: int = 3
    __ASSET_TRANSFERED: int = 3
    __ASSET_SENT: int = 4
    __CHAIN_USED: int = 4
    __FEE_AMOUNT: int = 5
    __TXN_ID: int = 5
    __FEE_ASSET: int = 6

    __DELIMITER: str = ","

    # Keywords
    __DEPOSIT: str = "DEPOSIT"
    __WITHDRAWAL: str = "WITHDRAWAL"

    def __init__(
        self,
        account_holder: str,
        trades_csv_file: Optional[str] = None,
        transfers_csv_file: Optional[str] = None,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__trades_csv_file: Optional[str] = trades_csv_file
        self.__transfers_csv_file: Optional[str] = transfers_csv_file
        self.__logger: logging.Logger = create_logger(f"{self.__PIONEX_PLUGIN}/{self.account_holder}")

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        if self.__trades_csv_file:
            result += self.parse_trades_file(self.__trades_csv_file)

        if self.__transfers_csv_file:
            result += self.parse_transfers_file(self.__transfers_csv_file)

        return result

    def parse_trades_file(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)

            header = next(lines)
            self.__logger.debug("Header: %s", header)
            for line in lines:
                # If there is a blank sent/receive asset, this is a transfer, which we will process under transfers
                # Pionex sometimes creates 0 entries for some reason
                if line[self.__ASSET_SENT] == "" or line[self.__ASSET_RECEIVED] == "" or float(line[self.__RECEIVED_AMOUNT]) == 0:
                    continue

                raw_data: str = self.__DELIMITER.join(line)
                self.__logger.debug("Transaction: %s", raw_data)

                in_crypto_fee: str = "0"
                out_crypto_fee: str = "0"

                if line[self.__ASSET_RECEIVED] == line[self.__FEE_ASSET]:
                    in_crypto_fee = line[self.__FEE_AMOUNT]
                else:
                    out_crypto_fee = line[self.__FEE_AMOUNT]

                result.append(
                    InTransaction(
                        plugin=self.__PIONEX_PLUGIN,
                        unique_id=Keyword.UNKNOWN.value,
                        raw_data=raw_data,
                        timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                        asset=line[self.__ASSET_RECEIVED],
                        exchange=self.__PIONEX,
                        holder=self.account_holder,
                        transaction_type=Keyword.BUY.value,
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_in=line[self.__RECEIVED_AMOUNT],
                        crypto_fee=in_crypto_fee,
                        notes=None,
                    )
                )

                result.append(
                    OutTransaction(
                        plugin=self.__PIONEX_PLUGIN,
                        unique_id=Keyword.UNKNOWN.value,
                        raw_data=raw_data,
                        timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                        asset=line[self.__ASSET_SENT],
                        exchange=self.__PIONEX,
                        holder=self.account_holder,
                        transaction_type=Keyword.SELL.value,
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_out_no_fee=line[self.__SENT_AMOUNT],
                        crypto_out_with_fee=line[self.__SENT_AMOUNT],
                        crypto_fee=out_crypto_fee,
                        notes=None,
                    )
                )

            return result

    def parse_transfers_file(self, file_path: str) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        with open(file_path, encoding="utf-8") as csv_file:
            lines = reader(csv_file)

            header = next(lines)
            self.__logger.debug("Header: %s", header)
            for line in lines:

                raw_data: str = self.__DELIMITER.join(line)
                self.__logger.debug("Transaction: %s", raw_data)

                asset: str = (
                    line[self.__ASSET_TRANSFERED][: -len(line[self.__CHAIN_USED])]
                    if (line[self.__ASSET_TRANSFERED].endswith(line[self.__CHAIN_USED]))
                    else (line[self.__ASSET_TRANSFERED])
                )

                if line[self.__TRANSACTION_TYPE] == self.__DEPOSIT:
                    result.append(
                        IntraTransaction(
                            plugin=self.__PIONEX_PLUGIN,
                            unique_id=line[self.__TXN_ID],
                            raw_data=raw_data,
                            timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                            asset=asset,
                            from_exchange=Keyword.UNKNOWN.value,
                            from_holder=Keyword.UNKNOWN.value,
                            to_exchange=self.__PIONEX,
                            to_holder=self.account_holder,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_sent=Keyword.UNKNOWN.value,
                            crypto_received=str(line[self.__AMOUNT_TRANSFERED]),
                        )
                    )
                elif line[self.__TRANSACTION_TYPE] == self.__WITHDRAWAL:
                    result.append(
                        IntraTransaction(
                            plugin=self.__PIONEX_PLUGIN,
                            unique_id=line[self.__TXN_ID],
                            raw_data=raw_data,
                            timestamp=f"{line[self.__TIMESTAMP_INDEX]} -00:00",
                            asset=asset,
                            from_exchange=self.__PIONEX,
                            from_holder=self.account_holder,
                            to_exchange=Keyword.UNKNOWN.value,
                            to_holder=Keyword.UNKNOWN.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_sent=str(line[self.__AMOUNT_TRANSFERED]),
                            crypto_received=Keyword.UNKNOWN.value,
                        )
                    )
                else:
                    self.__logger.error("Unrecognized Crypto transfer: %s", raw_data)

            return result
