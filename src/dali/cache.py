# Copyright 2022 QP Hou
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


import os
import pickle  # nosec
from typing import Any

from rp2.rp2_error import RP2TypeError

CACHE_DIR: str = ".dali_cache"


def load_from_cache(cache_name: str) -> Any:
    cache_path = os.path.join(CACHE_DIR, cache_name)
    if not os.path.exists(cache_path):
        return None
    with open(cache_path, "rb") as cache_file:
        try:
            result: Any = pickle.load(cache_file)  # nosec
        except TypeError as exc:
            raise RP2TypeError(f"Cache format changed for {cache_path}: delete the cache file and rerun DaLI") from exc
        return result


def save_to_cache(cache_name: str, data: Any) -> None:
    if not os.path.exists(CACHE_DIR):
        os.mkdir(CACHE_DIR)
    with open(os.path.join(CACHE_DIR, cache_name), "wb") as cache_file:
        cache_file.write(pickle.dumps(data))
