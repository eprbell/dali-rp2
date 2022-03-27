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

from typing import List, Optional

import os
import pickle  # nosec
from rp2.rp2_error import RP2TypeError

from dali.abstract_transaction import AbstractTransaction


class AbstractInputPlugin:
    ISSUES_URL: str = "https://github.com/eprbell/dali-rp2/issues"
    __CACHE_DIR = ".cache"

    def __init__(
        self,
        account_holder: str,
    ) -> None:
        if not isinstance(account_holder, str):
            raise RP2TypeError(f"account_holder is not a string: {account_holder}")
        self.__account_holder: str = account_holder

    # pylint: disable=no-self-use
    def cache_key(self) -> Optional[str]:
        return None

    def load(self) -> List[AbstractTransaction]:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def load_cache(self) -> Optional[List[AbstractTransaction]]:
        cache_key = self.cache_key()  # pylint: disable=assignment-from-none
        if cache_key is None:
            raise Exception("Plugin doesn't support load cache")
        cache_path = os.path.join(self.__CACHE_DIR, cache_key)
        if not os.path.exists(cache_path):
            return None
        with open(cache_path, "rb") as cache_file:
            result: List[AbstractTransaction] = pickle.load(cache_file)  # nosec
            return result

    def save_cache(self, transactions: List[AbstractTransaction]) -> None:
        cache_key = self.cache_key()  # pylint: disable=assignment-from-none
        if cache_key is None:
            raise Exception("Plugin doesn't support load cache")
        if not os.path.exists(self.__CACHE_DIR):
            os.mkdir(self.__CACHE_DIR)
        with open(os.path.join(self.__CACHE_DIR, cache_key), "wb") as cache_file:
            cache_file.write(pickle.dumps(transactions))

    @property
    def account_holder(self) -> str:
        return self.__account_holder
