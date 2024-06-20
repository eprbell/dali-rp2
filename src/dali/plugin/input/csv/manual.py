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

# pylint: disable=line-too-long

# IN CSV Format: unique_id, timestamp, asset, exchange, holder, transaction_type, spot_price, crypto_in, fiat_fee, fiat_in_no_fee, fiat_in_with_fee, notes
# OUT CSV Format: unique_id, timestamp, asset, exchange, holder, transaction_type, spot_price, crypto_out_no_fee, crypto_fee, crypto_out_with_fee, fiat_in_no_fee, fiat_fee, notes
# INTRA CSV Format:unique_id, timestamp, asset, from_exchange, from_holder, to_exchange, to_holder, spot_price, crypto_sent, crypto_received, notes

# pylint: enable=line-too-long

import logging
from csv import reader
from typing import List, Optional, Dict

from rp2.abstract_country import AbstractCountry
from rp2.logger import create_logger

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction
from dali.transaction_item import TransactionItem, InTransactionItems, OutTransactionItems, IntraTransactionItems


class InputPlugin(AbstractInputPlugin):
    __MANUAL: str = "Manual"

    __IN_UNIQUE_ID_INDEX: int = 0
    __IN_TIMESTAMP_INDEX: int = 1
    __IN_ASSET_INDEX: int = 2
    __IN_EXCHANGE_INDEX: int = 3
    __IN_HOLDER_INDEX: int = 4
    __IN_TYPE_INDEX: int = 5
    __IN_SPOT_PRICE_INDEX: int = 6
    __IN_CRYPTO_IN_INDEX: int = 7
    __IN_CRYPTO_FEE_INDEX: int = 8
    __IN_FIAT_IN_NO_FEE_INDEX: int = 9
    __IN_FIAT_IN_WITH_FEE_INDEX: int = 10
    __IN_FIAT_FEE_INDEX: int = 11
    __IN_NOTES_INDEX: int = 12
    __IN_FIAT_TICKER: int = 13
    __IN_LEN: int = __IN_FIAT_TICKER

    __OUT_UNIQUE_ID_INDEX: int = 0
    __OUT_TIMESTAMP_INDEX: int = 1
    __OUT_ASSET_INDEX: int = 2
    __OUT_EXCHANGE_INDEX: int = 3
    __OUT_HOLDER_INDEX: int = 4
    __OUT_TYPE_INDEX: int = 5
    __OUT_SPOT_PRICE_INDEX: int = 6
    __OUT_CRYPTO_OUT_NO_FEE_INDEX: int = 7
    __OUT_CRYPTO_FEE_INDEX: int = 8
    __OUT_CRYPTO_OUT_WITH_FEE_INDEX: int = 9
    __OUT_FIAT_OUT_NO_FEE_INDEX: int = 10
    __OUT_FIAT_FEE_INDEX: int = 11
    __OUT_NOTES_INDEX: int = 12
    __OUT_FIAT_TICKER: int = 13
    __OUT_LEN: int = __OUT_FIAT_TICKER

    __INTRA_UNIQUE_ID_INDEX: int = 0
    __INTRA_TIMESTAMP_INDEX: int = 1
    __INTRA_ASSET_INDEX: int = 2
    __INTRA_FROM_EXCHANGE_INDEX: int = 3
    __INTRA_FROM_HOLDER_INDEX: int = 4
    __INTRA_TO_EXCHANGE_INDEX: int = 5
    __INTRA_TO_HOLDER_INDEX: int = 6
    __INTRA_SPOT_PRICE_INDEX: int = 7
    __INTRA_CRYPTO_SENT_INDEX: int = 8
    __INTRA_CRYPTO_RECEIVED_INDEX: int = 9
    __INTRA_NOTES_INDEX: int = 10
    __INTRA_FIAT_TICKER: int = 11
    __INTRA_LEN: int = __INTRA_FIAT_TICKER

    def __init__(
        self,
        in_csv_file: Optional[str] = None,
        out_csv_file: Optional[str] = None,
        intra_csv_file: Optional[str] = None,
        native_fiat: Optional[str] = None,
        in_unique_id: Optional[int] = __IN_UNIQUE_ID_INDEX,
        in_timestamp: Optional[int] = __IN_TIMESTAMP_INDEX,
        in_asset: Optional[int] = __IN_ASSET_INDEX,
        in_exchange: Optional[int] = __IN_EXCHANGE_INDEX,
        in_holder: Optional[int] = __IN_HOLDER_INDEX,
        in_transaction_type: Optional[int] = __IN_TYPE_INDEX,
        in_spot_price: Optional[int] = __IN_SPOT_PRICE_INDEX,
        in_crypto_in: Optional[int] = __IN_CRYPTO_IN_INDEX,
        in_crypto_fee: Optional[int] = __IN_CRYPTO_FEE_INDEX,
        in_fiat_in_no_fee: Optional[int] = __IN_FIAT_IN_NO_FEE_INDEX,
        in_fiat_in_with_fee: Optional[int] = __IN_FIAT_IN_WITH_FEE_INDEX,
        in_fiat_fee: Optional[int] = __IN_FIAT_FEE_INDEX,
        in_notes: Optional[int] = __IN_NOTES_INDEX,
        in_fiat_ticker: Optional[int] = __IN_FIAT_TICKER,
        out_timestamp: Optional[int] = __OUT_TIMESTAMP_INDEX,
        out_asset: Optional[int] = __OUT_ASSET_INDEX,
        out_exchange: Optional[int] = __OUT_EXCHANGE_INDEX,
        out_holder: Optional[int] = __OUT_HOLDER_INDEX,
        out_transaction_type: Optional[int] = __OUT_TYPE_INDEX,
        out_spot_price: Optional[int] = __OUT_SPOT_PRICE_INDEX,
        out_crypto_out_no_fee: Optional[int] = __OUT_CRYPTO_OUT_NO_FEE_INDEX,
        out_crypto_fee: Optional[int] = __OUT_CRYPTO_FEE_INDEX,
        out_crypto_out_with_fee: Optional[int] = __OUT_CRYPTO_OUT_WITH_FEE_INDEX,
        out_fiat_out_no_fee: Optional[int] = __OUT_FIAT_OUT_NO_FEE_INDEX,
        out_fiat_fee: Optional[int] = __OUT_FIAT_FEE_INDEX,
        out_fiat_ticker: Optional[int] = __OUT_FIAT_TICKER,
        out_unique_id: Optional[int] = __OUT_UNIQUE_ID_INDEX,
        out_notes: Optional[int] = __OUT_NOTES_INDEX,
        intra_unique_id: Optional[int] = __INTRA_UNIQUE_ID_INDEX,
        intra_timestamp: Optional[int] = __INTRA_TIMESTAMP_INDEX,
        intra_asset: Optional[int] = __INTRA_ASSET_INDEX,
        intra_from_exchange: Optional[int] = __INTRA_FROM_EXCHANGE_INDEX,
        intra_from_holder: Optional[int] = __INTRA_FROM_HOLDER_INDEX,
        intra_to_exchange: Optional[int] = __INTRA_TO_EXCHANGE_INDEX,
        intra_to_holder: Optional[int] = __INTRA_TO_HOLDER_INDEX,
        intra_spot_price: Optional[int] = __INTRA_SPOT_PRICE_INDEX,
        intra_crypto_sent: Optional[int] = __INTRA_CRYPTO_SENT_INDEX,
        intra_crypto_received: Optional[int] = __INTRA_CRYPTO_RECEIVED_INDEX,
        intra_fiat_ticker: Optional[int] = __INTRA_FIAT_TICKER,
        intra_notes: Optional[int] = __INTRA_NOTES_INDEX,
    ) -> None:
        super().__init__(account_holder="", native_fiat=native_fiat)

        self.__in_csv_file: Optional[str] = in_csv_file
        self.__out_csv_file: Optional[str] = out_csv_file
        self.__intra_csv_file: Optional[str] = intra_csv_file

        self.__in_required_columns: Dict[str, TransactionItem] = {key: value for key, value in InTransactionItems.items() if value.required}
        self.__in_optional_columns: Dict[str, TransactionItem] = {key: value for key, value in InTransactionItems.items() if not value.required}
        self.__out_required_columns: Dict[str, TransactionItem] = {key: value for key, value in OutTransactionItems.items() if value.required}
        self.__out_optional_columns: Dict[str, TransactionItem] = {key: value for key, value in OutTransactionItems.items() if not value.required}
        self.__intra_required_columns: Dict[str, TransactionItem] = {key: value for key, value in IntraTransactionItems.items() if value.required}
        self.__intra_optional_columns: Dict[str, TransactionItem] = {key: value for key, value in IntraTransactionItems.items() if not value.required}

        self.__logger: logging.Logger = create_logger(self.__MANUAL)

    def load(self, country: AbstractCountry) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        self._load_in_file(result)
        self._load_out_file(result)
        self._load_intra_file(result)

        return result

    def _load_in_file(self, transactions: List[AbstractTransaction]) -> None:
        if not self.__in_csv_file:
            return

        with open(self.__in_csv_file, encoding="utf-8") as csv_file:
            lines = reader(csv_file)
            header_found: bool = False
            for line_idx, line in enumerate(lines):
                raw_data: str = ",".join(line).strip()
                if not header_found:
                    # let user know there is not enough columns
                    if len(line) < len(self.__in_required_columns):
                        raise ValueError(f"Not enough columns: the {self.__in_csv_file} CSV must contain {len(self.__in_required_columns)} columns.")

                    # Skip header line
                    header_found = True
                    self.__logger.debug("Header: %s", ";".join(line))
                    continue

                if raw_data.startswith("," * self.__IN_LEN):
                    # Skip empty lines
                    continue

                input_parameters: Dict[str, str] = {}
                input_parameters = self._check_required_columns(self.__in_csv_file, line_idx, line, self.__in_required_columns) 
                input_parameters = self._check_optional_columns(line, self.__in_optional_columns, input_parameters)       

                self.__logger.debug("Transaction: %s", ",".join(line))
                transactions.append(
                    InTransaction(
                        plugin=self.__MANUAL,
                        unique_id=input_parameters[Keyword.UNIQUE_ID.value],
                        raw_data=raw_data,
                        timestamp=input_parameters[Keyword.TIMESTAMP.value],
                        asset=input_parameters[Keyword.ASSET.value],
                        exchange=input_parameters[Keyword.EXCHANGE.value],
                        holder=input_parameters[Keyword.HOLDER.value],
                        transaction_type=input_parameters[Keyword.TRANSACTION_TYPE.value],
                        spot_price=input_parameters[Keyword.SPOT_PRICE.value],
                        crypto_in=input_parameters[Keyword.CRYPTO_IN.value],
                        crypto_fee=input_parameters[Keyword.CRYPTO_FEE.value],
                        fiat_in_no_fee=input_parameters[Keyword.FIAT_IN_NO_FEE.value],
                        fiat_in_with_fee=input_parameters[Keyword.FIAT_IN_WITH_FEE.value],
                        fiat_fee=input_parameters[Keyword.FIAT_FEE.value],
                        fiat_ticker=input_parameters[Keyword.FIAT_TICKER.value],
                        notes=input_parameters[Keyword.NOTES.value],
                    )
                )

    def _load_out_file(self, transactions: List[AbstractTransaction]) -> None:
        if not self.__out_csv_file:
            return

        with open(self.__out_csv_file, encoding="utf-8") as csv_file:
            lines = reader(csv_file)
            header_found: bool = False
            for line_idx, line in enumerate(lines):
                raw_data: str = ",".join(line).strip()

                if not header_found:
                    # let user know there is not enough columns
                    if len(line) < len(self.__out_required_columns):
                        raise ValueError(f"Not enough columns: the {self.__out_csv_file} CSV must contain {len(self.__out_required_columns)} columns.")

                    # Skip header line
                    header_found = True
                    self.__logger.debug("Header: %s", ";".join(line))
                    continue

                if raw_data.startswith("," * self.__OUT_LEN):
                    # Skip empty lines
                    continue

                input_parameters: Dict[str, str] = {}
                input_parameters = self._check_required_columns(self.__out_csv_file, line_idx, line, self.__out_required_columns) 
                input_parameters = self._check_optional_columns(line, self.__out_optional_columns, input_parameters)       
                
                self.__logger.debug("Transaction: %s", ",".join(line))
                transactions.append(
                    OutTransaction(
                        plugin=self.__MANUAL,
                        unique_id=input_parameters[Keyword.UNIQUE_ID.value],
                        raw_data=raw_data,
                        timestamp=input_parameters[Keyword.TIMESTAMP.value],
                        asset=input_parameters[Keyword.ASSET.value],
                        exchange=input_parameters[Keyword.EXCHANGE.value],
                        holder=input_parameters[Keyword.HOLDER.value],
                        transaction_type=input_parameters[Keyword.TRANSACTION_TYPE.value],
                        spot_price=input_parameters[Keyword.SPOT_PRICE.value],
                        crypto_out=input_parameters[Keyword.CRYPTO_OUT.value],
                        crypto_fee=input_parameters[Keyword.CRYPTO_FEE.value],
                        fiat_out_no_fee=input_parameters[Keyword.FIAT_OUT_NO_FEE.value],
                        fiat_out_with_fee=input_parameters[Keyword.FIAT_OUT_WITH_FEE.value],
                        fiat_fee=input_parameters[Keyword.FIAT_FEE.value],
                        fiat_ticker=input_parameters[Keyword.FIAT_TICKER.value],
                        notes=input_parameters[Keyword.NOTES.value],
                    )
                )

    def _load_intra_file(self, transactions: List[AbstractTransaction]) -> None:
        if not self.__intra_csv_file:
            return

        with open(self.__intra_csv_file, encoding="utf-8") as csv_file:
            lines = reader(csv_file)
            header_found: bool = False
            for line_idx, line in enumerate(lines):
                raw_data: str = ",".join(line).strip()
                if not header_found:
                    # let user know there is not enough columns
                    if len(line) < len(self.__intra_required_columns):
                        raise ValueError(f"Not enough columns: the {self.__intra_csv_file} CSV must contain {len(self.__intra_required_columns)} columns.")

                    # Skip header line
                    header_found = True
                    self.__logger.debug("Header: %s", ";".join(line))
                    continue

                if raw_data.startswith("," * self.__INTRA_LEN):
                    # Skip empty lines
                    continue

                input_parameters: Dict[str, str] = {}
                input_parameters = self._check_required_columns(self.__intra_csv_file, line_idx, line, self.__intra_required_columns) 
                input_parameters = self._check_optional_columns(line, self.__intra_optional_columns, input_parameters)     

                self.__logger.debug("Transaction: %s", ",".join(line))
                transactions.append(
                    IntraTransaction(
                        plugin=self.__MANUAL,
                        unique_id=input_parameters[Keyword.UNIQUE_ID.value],
                        raw_data=raw_data,
                        timestamp=input_parameters[Keyword.TIMESTAMP.value],
                        asset=input_parameters[Keyword.ASSET.value],
                        from_exchange=input_parameters[Keyword.FROM_EXCHANGE.value],
                        from_holder=input_parameters[Keyword.FROM_HOLDER.value],
                        to_exchange=input_parameters[Keyword.TO_EXCHANGE.value],
                        to_holder=input_parameters[Keyword.TO_HOLDER.value],
                        spot_price=input_parameters[Keyword.SPOT_PRICE.value],
                        crypto_sent=input_parameters[Keyword.CRYPTO_SENT.value],
                        crypto_received=input_parameters[Keyword.CRYPTO_RECEIVED.value],
                        notes=input_parameters[Keyword.NOTES.value],
                        fiat_ticker=input_parameters[Keyword.FIAT_TICKER.value],
                    )
                )
    
    def _check_required_columns(self, csv_file: str, line_idx: int, line: List[str], required_columns: Dict[int, str]) -> Dict[str, str]:
        input_parameters: Dict[str, str] = {}
        for index, keyword in required_columns.items():
            try:
                input_parameters[keyword.value] = line[index].strip()
            except IndexError:
                raise ValueError(f"Missing required column: {keyword} in {csv_file}, line {line_idx + 1}")
        return input_parameters
                
    def _check_optional_columns(self, line: List[str], optional_columns: Dict[int, str], input_parameters: Dict[str, str]) -> Dict[str, str]:
        for index, keyword in optional_columns.items():
            try:
                if line[index].strip() == '':
                    input_parameters[keyword.value] = Keyword.UNKNOWN.value
                else:
                    input_parameters[keyword.value] = line[index].strip()
            except IndexError:
                input_parameters[keyword.value] = Keyword.UNKNOWN.value
        return input_parameters
