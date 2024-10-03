# Copyright 2021 eprbell
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

from datetime import datetime
from typing import Any, Union

class HistoricalData:
    def __init__(self, ticker: str, granularity: int, start_date: str, end_data: str, verbose: bool) -> None: ...
    def retrieve_data(self) -> Any: ...  # type: ignore
    end_date: str
    granularity: int
    start_date: str
    ticker: str
    verbose: bool
    def _date_cleaner(self, date_time: Union[datetime, str]) -> str: ...
    def _ticker_checker(self) -> None: ...
