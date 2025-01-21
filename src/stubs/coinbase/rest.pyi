# Copyright 2024 Neal Chambers
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

from typing import IO, Any, Optional, Union

class RESTClient:
    def __init__(
        self,
        api_key: Optional[str] = ...,
        api_secret: Optional[str] = ...,
        key_file: Optional[Union[IO[bytes], str]] = ...,
        base_url: Optional[str] = ...,
        timeout: Optional[int] = ...,
        verbose: Optional[bool] = ...,
        rate_limit_headers: Optional[bool] = ...,
    ) -> None: ...
    def get_candles(self, product_id: str, start: str, end: str, granularity: str, limit: Optional[int] = None, **kwargs) -> Any: ...  # type: ignore
    def get_public_candles(self, product_id: str, start: str, end: str, granularity: str, limit: Optional[int] = None, **kwargs) -> Any: ...  # type: ignore
