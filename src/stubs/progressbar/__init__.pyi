# Copyright 2023 Christopher Whelan
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

from types import TracebackType
from typing import List, Optional, Type, Union

from .widgets import Widgets

class ProgressBar:
    max_value: int

    def __init__(self, max_value: int, widgets: Optional[List[Union[Widgets, str]]]) -> None: ...
    def __enter__(self) -> ProgressBar: ...
    def __exit__(self, exc_type: Optional[Type[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]) -> None: ...
    def update(self, value: int) -> None: ...

class StreamWrapper:
    def wrap_stderr(self) -> None: ...

streams = StreamWrapper()
