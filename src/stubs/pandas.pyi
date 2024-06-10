# Copyright 2022 Steve Davis
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

from typing import Any, List, Optional, overload

class Index:
    def __init__(self, data: List[object], name: str) -> None: ...

class _iLocIndexer:
    @overload
    def __getitem__(self, index: int) -> Series: ...
    @overload
    def __getitem__(self, index: slice) -> DataFrame: ...

class DataFrame:
    def __init__(self, data: Optional[dict[str, List[float]]] = None, index: Optional[Index] = None) -> None:
        self.index: Series
        ...

    def reset_index(self) -> DataFrame: ...
    @property
    def iloc(self) -> _iLocIndexer: ...

class Series:
    def __init__(self, name: str) -> None: ...
    def __getitem__(self, key: object) -> Any: ...  # type: ignore
    def tz_localize(self, tz: str) -> Series: ...
