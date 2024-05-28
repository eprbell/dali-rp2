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

from datetime import datetime
from typing import Any, Dict, NamedTuple, Optional, cast

from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2TypeError

from dali.cache import load_from_cache, save_to_cache
from dali.configuration import HISTORICAL_PRICE_KEYWORD_SET
from dali.historical_bar import HistoricalBar
from dali.logger import LOGGER
from dali.transaction_manifest import TransactionManifest

_CONFIG_DOC_FILE_URL: str = "https://github.com/eprbell/dali-rp2/blob/main/docs/configuration_file.md"


class AssetPairAndTimestamp(NamedTuple):
    timestamp: datetime
    from_asset: str
    to_asset: str
    exchange: str


class AbstractPairConverterPlugin:
    __ISSUES_URL: str = "https://github.com/eprbell/dali-rp2/issues"

    def __init__(self, historical_price_type: str) -> None:
        if not isinstance(historical_price_type, str):
            raise RP2TypeError(f"historical_price_type is not a string: {historical_price_type}")
        if historical_price_type not in HISTORICAL_PRICE_KEYWORD_SET:
            raise RP2TypeError(
                f"historical_price_type must be one of {', '.join(sorted(HISTORICAL_PRICE_KEYWORD_SET))}, instead it was: {historical_price_type}"
            )
        try:
            result = cast(Dict[AssetPairAndTimestamp, HistoricalBar], load_from_cache(self.cache_key()))
        except EOFError:
            LOGGER.error("EOFError: Cached file corrupted, no cache found.")
            result = None
        self._cache: Dict[AssetPairAndTimestamp, Any] = result if result is not None else {}
        self.__historical_price_type: str = historical_price_type

    def name(self) -> str:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def cache_key(self) -> str:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def optimize(self, transaction_manifest: TransactionManifest) -> None:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    @property
    def historical_price_type(self) -> str:
        return self.__historical_price_type

    @property
    def issues_url(self) -> str:
        return self.__ISSUES_URL

    # The exchange parameter is a hint on which exchange to use for price lookups. The plugin is free to use it or ignore it.
    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def save_historical_price_cache(self) -> None:
        save_to_cache(self.cache_key(), self._cache)

    def get_conversion_rate(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[RP2Decimal]:
        result: Optional[RP2Decimal] = None
        historical_bar: Optional[HistoricalBar] = None
        key: AssetPairAndTimestamp = AssetPairAndTimestamp(timestamp, from_asset, to_asset, exchange)
        log_message_qualifier: str = ""
        if key in self._cache:
            historical_bar = self._cache[key]
            log_message_qualifier = "cache of "
        else:
            historical_bar = self.get_historic_bar_from_native_source(timestamp, from_asset, to_asset, exchange)
            if historical_bar:
                self._cache[key] = historical_bar

        if historical_bar:
            result = historical_bar.derive_transaction_price(timestamp, self.__historical_price_type)
            LOGGER.debug(
                "Fetched %s conversion rate %s for %s/%s->%s from %splugin %s: %s",
                self.__historical_price_type,
                result,
                timestamp,
                from_asset,
                to_asset,
                log_message_qualifier,
                self.name(),
                historical_bar,
            )

        return result
