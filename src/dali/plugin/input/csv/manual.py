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
from typing import List, Optional

from rp2.logger import create_logger

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction


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

    def __init__(
        self,
        in_csv_file: str,
        out_csv_file: str,
        intra_csv_file: str,
        native_fiat: Optional[str] = None,
    ) -> None:

        super().__init__(account_holder="", native_fiat=native_fiat)

        self.__in_csv_file: str = in_csv_file
        self.__out_csv_file: str = out_csv_file
        self.__intra_csv_file: str = intra_csv_file

        self.__logger: logging.Logger = create_logger(self.__MANUAL)

    def load(self) -> List[AbstractTransaction]:
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
            for line in lines:
                raw_data: str = ",".join(line).strip()
                if not header_found:
                    # let user know there is not enough columns
                    if len(line) - 1 < self.__IN_NOTES_INDEX:
                        raise ValueError(f"Not enough columns: the {self.__in_csv_file} CSV must contain {self.__IN_NOTES_INDEX} columns.")

                    # Skip header line
                    header_found = True
                    self.__logger.debug("Header: %s", ";".join(line))
                    continue

                if raw_data.startswith("," * self.__IN_NOTES_INDEX):
                    # Skip empty lines
                    continue

                unique_id: str = line[self.__IN_UNIQUE_ID_INDEX]
                if not unique_id:
                    unique_id = Keyword.UNKNOWN.value
                self.__logger.debug("Transaction: %s", ",".join(line))
                transactions.append(
                    InTransaction(
                        plugin=self.__MANUAL,
                        unique_id=unique_id,
                        raw_data=raw_data,
                        timestamp=line[self.__IN_TIMESTAMP_INDEX],
                        asset=line[self.__IN_ASSET_INDEX],
                        exchange=line[self.__IN_EXCHANGE_INDEX],
                        holder=line[self.__IN_HOLDER_INDEX],
                        transaction_type=line[self.__IN_TYPE_INDEX],
                        spot_price=line[self.__IN_SPOT_PRICE_INDEX],
                        crypto_in=line[self.__IN_CRYPTO_IN_INDEX],
                        crypto_fee=line[self.__IN_CRYPTO_FEE_INDEX],
                        fiat_in_no_fee=line[self.__IN_FIAT_IN_NO_FEE_INDEX],
                        fiat_in_with_fee=line[self.__IN_FIAT_IN_WITH_FEE_INDEX],
                        fiat_fee=line[self.__IN_FIAT_FEE_INDEX],
                        notes=line[self.__IN_NOTES_INDEX],
                    )
                )

    def _load_out_file(self, transactions: List[AbstractTransaction]) -> None:

        if not self.__out_csv_file:
            return

        with open(self.__out_csv_file, encoding="utf-8") as csv_file:
            lines = reader(csv_file)
            header_found: bool = False
            for line in lines:
                raw_data: str = ",".join(line).strip()

                if not header_found:
                    # let user know there is not enough columns
                    if len(line) - 1 < self.__OUT_NOTES_INDEX:
                        raise ValueError(f"Not enough columns: the {self.__out_csv_file} CSV must contain {self.__OUT_NOTES_INDEX} columns.")

                    # Skip header line
                    header_found = True
                    self.__logger.debug("Header: %s", ";".join(line))
                    continue

                if raw_data.startswith("," * self.__OUT_NOTES_INDEX):
                    # Skip empty lines
                    continue

                unique_id: str = line[self.__OUT_UNIQUE_ID_INDEX]
                if not unique_id:
                    unique_id = Keyword.UNKNOWN.value
                self.__logger.debug("Transaction: %s", ",".join(line))
                transactions.append(
                    OutTransaction(
                        plugin=self.__MANUAL,
                        unique_id=unique_id,
                        raw_data=raw_data,
                        timestamp=line[self.__OUT_TIMESTAMP_INDEX],
                        asset=line[self.__OUT_ASSET_INDEX],
                        exchange=line[self.__OUT_EXCHANGE_INDEX],
                        holder=line[self.__OUT_HOLDER_INDEX],
                        transaction_type=line[self.__OUT_TYPE_INDEX],
                        spot_price=line[self.__OUT_SPOT_PRICE_INDEX],
                        crypto_out_no_fee=line[self.__OUT_CRYPTO_OUT_NO_FEE_INDEX],
                        crypto_fee=line[self.__OUT_CRYPTO_FEE_INDEX],
                        crypto_out_with_fee=line[self.__OUT_CRYPTO_OUT_WITH_FEE_INDEX],
                        fiat_out_no_fee=line[self.__OUT_FIAT_OUT_NO_FEE_INDEX],
                        fiat_fee=line[self.__OUT_FIAT_FEE_INDEX],
                        notes=line[self.__OUT_NOTES_INDEX],
                    )
                )

    def _load_intra_file(self, transactions: List[AbstractTransaction]) -> None:

        if not self.__intra_csv_file:
            return

        with open(self.__intra_csv_file, encoding="utf-8") as csv_file:
            lines = reader(csv_file)
            header_found: bool = False
            for line in lines:
                raw_data: str = ",".join(line).strip()
                if not header_found:
                    # let user know there is not enough columns
                    if len(line) - 1 < self.__INTRA_NOTES_INDEX:
                        raise ValueError(f"Not enough columns: the {self.__intra_csv_file} CSV must contain {self.__INTRA_NOTES_INDEX} columns.")

                    # Skip header line
                    header_found = True
                    self.__logger.debug("Header: %s", ";".join(line))
                    continue

                if raw_data.startswith("," * self.__INTRA_NOTES_INDEX):
                    # Skip empty lines
                    continue

                self.__logger.debug("Transaction: %s", ",".join(line))
                from_exchange: str = line[self.__INTRA_FROM_EXCHANGE_INDEX]
                from_holder: str = line[self.__INTRA_FROM_HOLDER_INDEX]
                to_exchange: str = line[self.__INTRA_TO_EXCHANGE_INDEX]
                to_holder: str = line[self.__INTRA_TO_HOLDER_INDEX]
                crypto_sent: str = line[self.__INTRA_CRYPTO_SENT_INDEX]
                crypto_received: str = line[self.__INTRA_CRYPTO_RECEIVED_INDEX]
                transactions.append(
                    IntraTransaction(
                        plugin=self.__MANUAL,
                        unique_id=line[self.__INTRA_UNIQUE_ID_INDEX],
                        raw_data=raw_data,
                        timestamp=line[self.__INTRA_TIMESTAMP_INDEX],
                        asset=line[self.__INTRA_ASSET_INDEX],
                        from_exchange=from_exchange if from_exchange else Keyword.UNKNOWN.value,
                        from_holder=from_holder if from_holder else Keyword.UNKNOWN.value,
                        to_exchange=to_exchange if to_exchange else Keyword.UNKNOWN.value,
                        to_holder=to_holder if to_holder else Keyword.UNKNOWN.value,
                        spot_price=line[self.__INTRA_SPOT_PRICE_INDEX],
                        crypto_sent=crypto_sent if crypto_sent else Keyword.UNKNOWN.value,
                        crypto_received=crypto_received if crypto_received else Keyword.UNKNOWN.value,
                        notes=line[self.__INTRA_NOTES_INDEX],
                    )
                )
