# Copyright 2022 topherbuckley
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

from enum import Enum
from typing import Dict

from dali.configuration import Keyword


class TransactionItem:
    def __init__(self, disallow_empty: bool, disallow_unknown: bool, transaction_item_type: Keyword) -> None:
        self.__disallow_empty: bool = disallow_empty
        self.__disallow_unknown: bool = disallow_unknown
        self.__transaction_item_type: Keyword = transaction_item_type

    @property
    def disallow_empty(self) -> bool:
        return self.__disallow_empty

    @property
    def disallow_unknown(self) -> bool:
        return self.__disallow_unknown

    @property
    def transaction_item_type(self) -> Keyword:
        return self.__transaction_item_type


AbstractTransactionItems: Dict[str, TransactionItem] = dict(
    PLUGIN=TransactionItem(True, True, Keyword.PLUGIN),
    UNIQUE_ID=TransactionItem(True, False, Keyword.UNIQUE_ID),
    RAW_DATA=TransactionItem(True, True, Keyword.RAW_DATA),
    TIMESTAMP=TransactionItem(True, True, Keyword.TIMESTAMP),
    ASSET=TransactionItem(True, True, Keyword.ASSET),
    NOTES=TransactionItem(False, True, Keyword.NOTES),
    FIAT_TICKER=TransactionItem(True, True, Keyword.FIAT_TICKER),
)

OutTransactionItems: Dict[str, TransactionItem] = dict(
    EXCHANGE=TransactionItem(True, True, Keyword.EXCHANGE),
    HOLDER=TransactionItem(True, True, Keyword.HOLDER),
    TRANSACTION_TYPE=TransactionItem(True, True, Keyword.TRANSACTION_TYPE),
    SPOT_PRICE=TransactionItem(True, False, Keyword.SPOT_PRICE),
    CRYPTO_OUT_NO_FEE=TransactionItem(True, True, Keyword.CRYPTO_OUT_NO_FEE),
    CRYPTO_FEE=TransactionItem(True, True, Keyword.CRYPTO_FEE),
    CRYPTO_OUT_WITH_FEE=TransactionItem(False, True, Keyword.CRYPTO_OUT_WITH_FEE),
    FIAT_OUT_NO_FEE=TransactionItem(False, True, Keyword.FIAT_OUT_NO_FEE),
    FIAT_FEE=TransactionItem(False, True, Keyword.FIAT_FEE),
)

InTransactionItems: Dict[str, TransactionItem] = dict(
    EXCHANGE=TransactionItem(True, True, Keyword.EXCHANGE),
    HOLDER=TransactionItem(True, True, Keyword.HOLDER),
    TRANSACTION_TYPE=TransactionItem(True, True, Keyword.TRANSACTION_TYPE),
    SPOT_PRICE=TransactionItem(True, False, Keyword.SPOT_PRICE),
    CRYPTO_IN=TransactionItem(True, True, Keyword.CRYPTO_IN),
    CRYPTO_FEE=TransactionItem(False, True, Keyword.CRYPTO_FEE),
    FIAT_IN_NO_FEE=TransactionItem(False, True, Keyword.FIAT_IN_NO_FEE),
    FIAT_IN_WITH_FEE=TransactionItem(False, True, Keyword.FIAT_IN_WITH_FEE),
    FIAT_FEE=TransactionItem(False, True, Keyword.FIAT_FEE),
)

IntraTransactionItems: Dict[str, TransactionItem] = dict(
    FROM_EXCHANGE=TransactionItem(True, False, Keyword.FROM_EXCHANGE),
    FROM_HOLDER=TransactionItem(True, False, Keyword.FROM_HOLDER),
    TO_EXCHANGE=TransactionItem(True, False, Keyword.TO_EXCHANGE),
    TO_HOLDER=TransactionItem(True, False, Keyword.TO_HOLDER),
    SPOT_PRICE=TransactionItem(False, False, Keyword.SPOT_PRICE),
    CRYPTO_SENT=TransactionItem(True, False, Keyword.CRYPTO_SENT),
    CRYPTO_RECEIVED=TransactionItem(True, False, Keyword.CRYPTO_RECEIVED),
)


class TransactionDirection(Enum):
    __members__: Dict[str, TransactionItem]
    IN = InTransactionItems


class TransactionItems:
    def __init__(self, transaction_direction: TransactionDirection) -> None:
        self.__transaction_item_types: Dict[str, TransactionItem] = {**AbstractTransactionItems, **transaction_direction.value}
