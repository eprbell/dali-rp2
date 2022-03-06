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

from typing import List

from rp2.rp2_error import RP2TypeError

from dali.abstract_transaction import AbstractTransaction


class AbstractInputPlugin:
    def __init__(
        self,
        account_holder: str,
    ) -> None:
        if not isinstance(account_holder, str):
            raise RP2TypeError(f"account_holder is not a string: {account_holder}")
        self.__account_holder: str = account_holder

    def load(self) -> List[AbstractTransaction]:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    @property
    def account_holder(self) -> str:
        return self.__account_holder
