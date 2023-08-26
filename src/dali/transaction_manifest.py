# Copyright 2023 Neal Chambers
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

from datetime import datetime, timezone
from multiprocessing.pool import ThreadPool
from typing import List, Set, Tuple

from rp2.rp2_error import RP2TypeError

from dali.abstract_transaction import AbstractTransaction
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.logger import LOGGER
from dali.out_transaction import OutTransaction


# The TransactionManifest helps pair_converter plugins optimize their searches for prices from the web
# by informing the plugin of what assets and exchanges will need pricing as well as the first transaction datetime
class TransactionManifest:
    def __init__(self, transactions: List[AbstractTransaction], threads: int, native_fiat: str):
        # Split the transactions into chunks
        chunk_size = len(transactions) // threads
        chunks = [transactions[i : i + chunk_size] for i in range(0, len(transactions), chunk_size)]

        with ThreadPool(threads) as pool:
            result_list = pool.map(self._process_chunk, chunks)

        # Combine the asset and exchange sets
        combined_assets = {native_fiat}
        combined_exchanges = set()
        first_transaction_datetime = datetime.now(timezone.utc)

        for result in result_list:
            combined_assets.update(result[1])
            combined_exchanges.update(result[2])
            if result[0] < first_transaction_datetime:
                first_transaction_datetime = result[0]

        self.__assets: Set[str] = combined_assets
        self.__exchanges: Set[str] = combined_exchanges
        self.__first_transaction_datetime: datetime = first_transaction_datetime
        LOGGER.debug(
            "Created manifest - Assets: %s, Exchanges: %s, and first transaction: %s", self.__assets, self.__exchanges, self.__first_transaction_datetime
        )

    @property
    def assets(self) -> Set[str]:
        return self.__assets

    @property
    def exchanges(self) -> Set[str]:
        return self.__exchanges

    @property
    def first_transaction_datetime(self) -> datetime:
        return self.__first_transaction_datetime

    def _process_chunk(self, transactions: List[AbstractTransaction]) -> Tuple[datetime, Set[str], Set[str]]:
        first_transaction_datetime: datetime = datetime.now(timezone.utc)
        assets: Set[str] = set()
        exchanges: Set[str] = set()

        for transaction in transactions:
            assets.add(transaction.asset)
            if transaction.timestamp_value < first_transaction_datetime:
                first_transaction_datetime = transaction.timestamp_value
            elif isinstance(transaction, (InTransaction, OutTransaction)):
                exchanges.add(transaction.exchange)
            elif isinstance(transaction, IntraTransaction):
                exchanges.add(transaction.from_exchange)
            else:
                raise RP2TypeError("Internal error: Invalid transaction type passed to _process_chunk.")

        return first_transaction_datetime, assets, exchanges
