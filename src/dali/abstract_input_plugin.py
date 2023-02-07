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

from typing import List, Optional, cast

from rp2.rp2_error import RP2RuntimeError, RP2TypeError

from dali.abstract_transaction import AbstractTransaction
from dali.cache import load_from_cache, save_to_cache


class AbstractInputPlugin:
    ISSUES_URL: str = "https://github.com/eprbell/dali-rp2/issues"

    def __init__(
        self,
        account_holder: str,
        native_fiat: Optional[str],
    ) -> None:
        if not isinstance(account_holder, str):
            raise RP2TypeError(f"account_holder is not a string: {account_holder}")
        self.__account_holder: str = account_holder
        self.__native_fiat: Optional[str] = native_fiat

    def cache_key(self) -> Optional[str]:
        return None

    def load(self) -> List[AbstractTransaction]:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def load_from_cache(self) -> Optional[List[AbstractTransaction]]:
        cache_key = self.cache_key()  # pylint: disable=assignment-from-none
        if cache_key is None:
            raise RP2RuntimeError("Plugin doesn't support load cache")
        if not isinstance(cache_key, str):
            raise RP2RuntimeError("Plugin cache_key() doesn't return a string")
        return cast(Optional[List[AbstractTransaction]], load_from_cache(cache_key))

    def save_to_cache(self, transactions: List[AbstractTransaction]) -> None:
        cache_key = self.cache_key()  # pylint: disable=assignment-from-none
        if cache_key is None:
            raise RP2RuntimeError("Plugin doesn't support load cache")
        if not isinstance(cache_key, str):
            raise RP2RuntimeError("Plugin cache_key() doesn't return a string")
        save_to_cache(cache_key, transactions)

    @property
    def account_holder(self) -> str:
        return self.__account_holder

    @property
    def native_fiat(self) -> Optional[str]:
        return self.__native_fiat

    def is_native_fiat(self, currency: str) -> bool:
        return currency == self.__native_fiat
