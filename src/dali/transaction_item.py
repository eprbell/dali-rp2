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
    def __init__(self, disallow_empty: bool,
                 disallow_unknown: bool,
                 required: bool,
                 ) -> None:
        self.__disallow_empty: bool = disallow_empty
        self.__disallow_unknown: bool = disallow_unknown
        self.__required: bool = required

    @property
    def disallow_empty(self) -> bool:
        return self.__disallow_empty

    @property
    def disallow_unknown(self) -> bool:
        return self.__disallow_unknown

    @property
    def required(self) -> bool:
        return self.__required


_abstract_transaction_items: Dict[str, TransactionItem] = {
    Keyword.PLUGIN.value: TransactionItem(True, True, True),
    # TODO I'm still confused  about whether this is actually required as the docs state it is not for an IntraTransaction, but I don't see how it would get through the checks without failing. 
    Keyword.UNIQUE_ID.value: TransactionItem(True, False, True),
    Keyword.RAW_DATA.value: TransactionItem(True, True, True),
    Keyword.TIMESTAMP.value: TransactionItem(True, True, True),
    Keyword.ASSET.value: TransactionItem(True, True, True),
    Keyword.NOTES.value: TransactionItem(False, True, False),
    Keyword.FIAT_TICKER.value: TransactionItem(True, True, False),
}

_in_transaction_items: Dict[str, TransactionItem] = {
    Keyword.EXCHANGE.value: TransactionItem(True, True, True),
    Keyword.HOLDER.value: TransactionItem(True, True, True),
    Keyword.TRANSACTION_TYPE.value: TransactionItem(True, True, True),
    Keyword.SPOT_PRICE.value: TransactionItem(True, False, True),
    Keyword.CRYPTO_IN.value: TransactionItem(True, True, True),
    Keyword.CRYPTO_FEE.value: TransactionItem(False, True, False),
    Keyword.FIAT_IN_NO_FEE.value: TransactionItem(False, True, False),
    Keyword.FIAT_IN_WITH_FEE.value: TransactionItem(False, True, False),
    Keyword.FIAT_FEE.value: TransactionItem(False, True, False),
}

_out_transaction_items: Dict[str, TransactionItem] = {
    Keyword.EXCHANGE.value: TransactionItem(True, True, True),
    Keyword.HOLDER.value: TransactionItem(True, True, True),
    Keyword.TRANSACTION_TYPE.value: TransactionItem(True, True, True),
    Keyword.SPOT_PRICE.value: TransactionItem(True, False, True),
    Keyword.CRYPTO_OUT_NO_FEE.value: TransactionItem(True, True, True),
    Keyword.CRYPTO_FEE.value: TransactionItem(True, True, True),
    Keyword.CRYPTO_OUT_WITH_FEE.value: TransactionItem(False, True, False),
    Keyword.FIAT_OUT_NO_FEE.value: TransactionItem(False, True, False),
    Keyword.FIAT_FEE.value: TransactionItem(False, True, True),
}

_intra_transaction_items: Dict[str, TransactionItem] = {
    Keyword.FROM_EXCHANGE.value: TransactionItem(True, False, True),
    Keyword.FROM_HOLDER.value: TransactionItem(True, False, True),
    Keyword.TO_EXCHANGE.value: TransactionItem(True, False, True),
    Keyword.TO_HOLDER.value: TransactionItem(True, False, True),
    Keyword.SPOT_PRICE.value: TransactionItem(False, False, False),
    Keyword.CRYPTO_SENT.value: TransactionItem(True, False, True),
    Keyword.CRYPTO_RECEIVED.value: TransactionItem(True, False, True),
}


class TransactionDirection(Enum):
    __members__: Dict[str, TransactionItem]
    IN = _in_transaction_items
    OUT = _out_transaction_items
    INTRA = _intra_transaction_items


def _combine_dicts(transaction_direction: TransactionDirection) -> Dict[str, TransactionItem]:
    return {**_abstract_transaction_items, **transaction_direction.value}


InTransactionItems = _combine_dicts(TransactionDirection.IN)
OutTransactionItems = _combine_dicts(TransactionDirection.OUT)
IntraTransactionItems = _combine_dicts(TransactionDirection.INTRA)
