# Copyright 2023 jamesbaber1
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

import logging
from typing import List, Optional, cast

from rp2.abstract_country import AbstractCountry
from rp2.configuration import MAX_DATE, MIN_DATE, Configuration
from rp2.in_transaction import InTransaction as RP2InTransaction
from rp2.input_data import InputData
from rp2.intra_transaction import IntraTransaction as RP2IntraTransaction
from rp2.logger import create_logger
from rp2.ods_parser import open_ods, parse_ods
from rp2.out_transaction import OutTransaction as RP2OutTransaction

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction


class InputPlugin(AbstractInputPlugin):
    __RP2_INPUT: str = "RP2 Input"

    def __init__(
        self,
        configuration_path: str,
        input_file: str,
        native_fiat: Optional[str] = None,
        force_repricing: Optional[bool] = False,
    ) -> None:
        super().__init__(account_holder="", native_fiat=native_fiat)
        self.__configuration_path: str = configuration_path
        self.__input_file: str = input_file
        self.__logger: logging.Logger = create_logger(self.__RP2_INPUT)
        self.__force_repricing = force_repricing

    def load(self, country: AbstractCountry) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        rp2_configuration: Configuration = Configuration(
            configuration_path=self.__configuration_path,
            country=country,
            from_date=MIN_DATE,
            to_date=MAX_DATE,
        )

        input_file_handle: object = open_ods(configuration=rp2_configuration, input_file_path=self.__input_file)
        assets = sorted(list(rp2_configuration.assets))

        for asset in assets:
            self.__logger.info("Processing %s", asset)

            input_data: InputData = parse_ods(configuration=rp2_configuration, asset=asset, input_file_handle=input_file_handle)
            self.__logger.debug("InputData object: %s", input_data)
            for asset_entry in input_data.unfiltered_in_transaction_set:
                in_transaction: RP2InTransaction = cast(RP2InTransaction, asset_entry)
                self.__logger.debug("Transaction: %s", str(in_transaction))
                result.append(
                    InTransaction(
                        plugin=self.__RP2_INPUT,
                        unique_id=in_transaction.unique_id,
                        raw_data=str(in_transaction),
                        timestamp=str(in_transaction.timestamp),
                        asset=in_transaction.asset,
                        exchange=in_transaction.exchange,
                        holder=in_transaction.holder,
                        transaction_type=in_transaction.transaction_type.value,
                        spot_price=Keyword.UNKNOWN.value if self.__force_repricing else str(in_transaction.spot_price),
                        crypto_in=str(in_transaction.crypto_in),
                        crypto_fee=str(in_transaction.crypto_fee) if in_transaction.crypto_fee else None,
                        fiat_in_no_fee=None if self.__force_repricing else str(in_transaction.fiat_in_no_fee),
                        fiat_in_with_fee=None if self.__force_repricing else str(in_transaction.fiat_in_with_fee),
                        fiat_fee=None if self.__force_repricing else str(in_transaction.fiat_fee),
                        notes=str(in_transaction.notes) if in_transaction.notes else None,
                    )
                )

            for asset_transfer in input_data.unfiltered_intra_transaction_set:
                intra_transaction: RP2IntraTransaction = cast(RP2IntraTransaction, asset_transfer)
                self.__logger.debug("Transaction: %s", str(intra_transaction))
                result.append(
                    IntraTransaction(
                        plugin=self.__RP2_INPUT,
                        unique_id=intra_transaction.unique_id,
                        raw_data=str(intra_transaction),
                        timestamp=str(intra_transaction.timestamp),
                        asset=intra_transaction.asset,
                        from_exchange=intra_transaction.from_exchange,
                        from_holder=intra_transaction.from_holder,
                        to_exchange=intra_transaction.to_exchange,
                        to_holder=intra_transaction.to_holder,
                        spot_price=Keyword.UNKNOWN.value if self.__force_repricing else str(intra_transaction.spot_price),
                        crypto_sent=str(intra_transaction.crypto_sent),
                        crypto_received=str(intra_transaction.crypto_received),
                        notes=str(intra_transaction.notes) if intra_transaction.notes else None,
                    )
                )

            for asset_exit in input_data.unfiltered_out_transaction_set:
                out_transaction: RP2OutTransaction = cast(RP2OutTransaction, asset_exit)
                self.__logger.debug("Transaction: %s", str(out_transaction))
                result.append(
                    OutTransaction(
                        plugin=self.__RP2_INPUT,
                        unique_id=out_transaction.unique_id,
                        raw_data=str(out_transaction),
                        timestamp=str(out_transaction.timestamp),
                        asset=out_transaction.asset,
                        exchange=out_transaction.exchange,
                        holder=out_transaction.holder,
                        transaction_type=out_transaction.transaction_type.value,
                        spot_price=Keyword.UNKNOWN.value if self.__force_repricing else str(out_transaction.spot_price),
                        crypto_out_no_fee=str(out_transaction.crypto_out_no_fee),
                        crypto_fee=str(out_transaction.crypto_fee),
                        crypto_out_with_fee=str(out_transaction.crypto_out_with_fee),
                        fiat_out_no_fee=None if self.__force_repricing else str(out_transaction.fiat_out_no_fee),
                        fiat_fee=None if self.__force_repricing else str(out_transaction.fiat_fee) if out_transaction.fiat_fee else None,
                        notes=str(out_transaction.notes) if out_transaction.notes else None,
                    )
                )
        return result
