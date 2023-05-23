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

from dali.configuration import Keyword
from enum import Enum


class Transaction_Item:
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


class TRANSACTION_ITEM_TYPE(Enum):
    __members__: Transaction_Item
    PLUGIN = Transaction_Item(True, True, Keyword.PLUGIN)
    UNIQUE_ID = Transaction_Item(True, False, Keyword.UNIQUE_ID)
    RAW_DATA = Transaction_Item(True, True, Keyword.RAW_DATA)
    TIMESTAMP = Transaction_Item(True, True, Keyword.TIMESTAMP)
    ASSET = Transaction_Item(True, True, Keyword.ASSET)
    NOTES = Transaction_Item(False, True, Keyword.NOTES)
    FIAT_TICKER = Transaction_Item(True, True, Keyword.FIAT_TICKER)
