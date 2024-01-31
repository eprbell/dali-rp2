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

from datetime import datetime, timedelta
from json import JSONDecodeError
from typing import Any, Dict, List, NamedTuple, Optional, cast

import requests
from requests.exceptions import ReadTimeout
from requests.models import Response
from requests.sessions import Session
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError, RP2TypeError, RP2ValueError

from dali.cache import load_from_cache, save_to_cache
from dali.configuration import HISTORICAL_PRICE_KEYWORD_SET
from dali.historical_bar import HistoricalBar
from dali.logger import LOGGER
from dali.mapped_graph import MappedGraph
from dali.transaction_manifest import TransactionManifest

# exchangerates.host keywords
_ACCESS_KEY: str = "access_key"
_CURRENCIES: str = "currencies"
_DATE: str = "date"
_QUOTES: str = "quotes"
_SUCCESS: str = "success"

# exchangerates.host urls
_EXCHANGE_BASE_URL: str = "http://api.exchangerate.host/historical"
_EXCHANGE_SYMBOLS_URL: str = "http://api.exchangerate.host/list"

_DAYS_IN_SECONDS: int = 86400
_FIAT_EXCHANGE: str = "exchangerate.host"

# First on the list has the most priority
# This is hard-coded for now based on volume of each of these markets for BTC on Coinmarketcap.com
# Any change to this priority should be documented in "docs/configuration_file.md"
_FIAT_PRIORITY: Dict[str, float] = {
    "USD": 1,
    "JPY": 2,
    "KRW": 3,
    "EUR": 4,
    "GBP": 5,
    "AUD": 6,
}

# Other Weights
_STANDARD_WEIGHT: float = 1
_STANDARD_INCREMENT: float = 1

_CONFIG_DOC_FILE_URL: str = "https://github.com/eprbell/dali-rp2/blob/main/docs/configuration_file.md"


class AssetPairAndTimestamp(NamedTuple):
    timestamp: datetime
    from_asset: str
    to_asset: str
    exchange: str


class AbstractPairConverterPlugin:
    __ISSUES_URL: str = "https://github.com/eprbell/dali-rp2/issues"
    __TIMEOUT: int = 30

    def __init__(self, historical_price_type: str, fiat_access_key: Optional[str] = None, fiat_priority: Optional[str] = None) -> None:
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
        self.__cache: Dict[AssetPairAndTimestamp, Any] = result if result is not None else {}
        self.__historical_price_type: str = historical_price_type
        self.__session: Session = requests.Session()
        self.__fiat_list: List[str] = []
        self.__fiat_priority: Dict[str, float]
        if fiat_priority:
            weight: float = _STANDARD_WEIGHT
            for fiat in fiat_priority:
                self.__fiat_priority[fiat] = weight
                weight += _STANDARD_INCREMENT
        else:
            self.__fiat_priority = _FIAT_PRIORITY
        self.__fiat_access_key: Optional[str] = None
        if fiat_access_key:
            self.__fiat_access_key = fiat_access_key
        else:
            LOGGER.warning("No Fiat Access Key. Fiat pricing will NOT be available. To enable fiat pricing, an access key from exchangerate.host is required.")

    def name(self) -> str:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def cache_key(self) -> str:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def optimize(self, transaction_manifest: TransactionManifest) -> None:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def _add_bar_to_cache(self, key: AssetPairAndTimestamp, historical_bar: HistoricalBar) -> None:
        self.__cache[self._floor_key(key)] = historical_bar

    def _get_bar_from_cache(self, key: AssetPairAndTimestamp) -> Optional[HistoricalBar]:
        return self.__cache.get(self._floor_key(key))

    # All bundle timestamps have 1 millisecond added to them, so will not conflict with the floored timestamps of single bars
    def _add_bundle_to_cache(self, key: AssetPairAndTimestamp, historical_bars: List[HistoricalBar]) -> None:
        self.__cache[key] = historical_bars

    def _get_bundle_from_cache(self, key: AssetPairAndTimestamp) -> Optional[List[HistoricalBar]]:
        return cast(List[HistoricalBar], self.__cache.get(key))

    # The most granular pricing available is 1 minute, to reduce the size of cache and increase the reuse of pricing data
    def _floor_key(self, key: AssetPairAndTimestamp) -> AssetPairAndTimestamp:
        raw_timestamp: datetime = key.timestamp
        floored_timestamp: datetime = raw_timestamp - timedelta(
            minutes=raw_timestamp.minute % 1, seconds=raw_timestamp.second, microseconds=raw_timestamp.microsecond
        )
        floored_key: AssetPairAndTimestamp = AssetPairAndTimestamp(
            timestamp=floored_timestamp,
            from_asset=key.from_asset,
            to_asset=key.to_asset,
            exchange=key.exchange,
        )

        return floored_key

    @property
    def historical_price_type(self) -> str:
        return self.__historical_price_type

    @property
    def fiat_list(self) -> List[str]:
        return self.__fiat_list

    @property
    def issues_url(self) -> str:
        return self.__ISSUES_URL

    # The exchange parameter is a hint on which exchange to use for price lookups. The plugin is free to use it or ignore it.
    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def save_historical_price_cache(self) -> None:
        save_to_cache(self.cache_key(), self.__cache)

    def get_conversion_rate(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[RP2Decimal]:
        result: Optional[RP2Decimal] = None
        historical_bar: Optional[HistoricalBar] = None
        key: AssetPairAndTimestamp = AssetPairAndTimestamp(timestamp, from_asset, to_asset, exchange)
        log_message_qualifier: str = ""
        if key in self.__cache:
            historical_bar = self.__cache[key]
            log_message_qualifier = "cache of "
        else:
            historical_bar = self.get_historic_bar_from_native_source(timestamp, from_asset, to_asset, exchange)
            if historical_bar:
                self.__cache[key] = historical_bar

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

    def _build_fiat_list(self) -> None:
        self._check_fiat_access_key()
        try:
            response: Response = self.__session.get(_EXCHANGE_SYMBOLS_URL, params={_ACCESS_KEY: self.__fiat_access_key}, timeout=self.__TIMEOUT)
            #  {
            #     "success": true,
            #     "terms": "https://exchangerate.host/terms",
            #     "privacy": "https://exchangerate.host/privacy",
            #     "currencies": {
            #         "AED": "United Arab Emirates Dirham",
            #         "AFN": "Afghan Afghani",
            #         "ALL": "Albanian Lek",
            #         "AMD": "Armenian Dram",
            #         "ANG": "Netherlands Antillean Guilder",
            #         [...]
            #     }
            # }
            data: Any = response.json()
            if data[_SUCCESS]:
                self.__fiat_list = [fiat_iso for fiat_iso in data[_CURRENCIES] if fiat_iso != "BTC"]
            else:
                if "message" in data:
                    LOGGER.error("Error %d: %s: %s", response.status_code, _EXCHANGE_SYMBOLS_URL, data["message"])
                response.raise_for_status()

        except JSONDecodeError as exc:
            LOGGER.info("Fetching of fiat symbols failed. The server might be down. Please try again later.")
            raise RP2RuntimeError("JSON decode error") from exc

    def _add_fiat_edges_to_graph(self, graph: MappedGraph[str], markets: Dict[str, List[str]]) -> None:
        if not self.__fiat_list:
            self._build_fiat_list()

        for fiat in self.__fiat_list:
            to_fiat_list: Dict[str, None] = dict.fromkeys(self.__fiat_list.copy())
            del to_fiat_list[fiat]
            # We don't want to add a fiat vertex here because that would allow a double hop on fiat (eg. USD -> KRW -> JPY)
            if graph.get_vertex(fiat):
                for to_be_added_fiat in to_fiat_list:
                    graph.add_neighbor(
                        fiat,
                        to_be_added_fiat,
                        self.__fiat_priority.get(fiat, _STANDARD_WEIGHT),
                        True,  # use set optimization
                    )

                LOGGER.debug("Added to assets for %s: %s", fiat, to_fiat_list)

            for to_fiat in to_fiat_list:
                fiat_market = f"{fiat}{to_fiat}"
                markets[fiat_market] = [_FIAT_EXCHANGE]

    def _is_fiat_pair(self, from_asset: str, to_asset: str) -> bool:
        return self._is_fiat(from_asset) and self._is_fiat(to_asset)

    def _is_fiat(self, asset: str) -> bool:
        if not self.__fiat_list:
            self._build_fiat_list()

        return asset in self.__fiat_list

    def _get_fiat_exchange_rate(self, timestamp: datetime, from_asset: str, to_asset: str) -> Optional[HistoricalBar]:
        key: AssetPairAndTimestamp = AssetPairAndTimestamp(timestamp, from_asset, to_asset, _FIAT_EXCHANGE)
        historical_bar: Optional[HistoricalBar] = self._get_bar_from_cache(key)

        if historical_bar is not None:
            LOGGER.debug("Retrieved cache for %s/%s->%s for %s", timestamp, from_asset, to_asset, _FIAT_EXCHANGE)
            return historical_bar

        self._check_fiat_access_key()
        # Currency has to be USD on free tier
        if from_asset != "USD" and to_asset != "USD":
            raise RP2ValueError("Fiat conversion is only available to/from USD at this time.")
        currency: str = from_asset if from_asset != "USD" else to_asset
        result: Optional[HistoricalBar] = None

        params: Dict[str, Any] = {_ACCESS_KEY: self.__fiat_access_key, _DATE: timestamp.strftime("%Y-%m-%d"), _CURRENCIES: currency}
        request_count: int = 0
        # exchangerate.host only gives us daily accuracy, which should be suitable for tax reporting
        while request_count < 5:
            try:
                response: Response = self.__session.get(f"{_EXCHANGE_BASE_URL}", params=params, timeout=self.__TIMEOUT)
                # {
                #     "success": true,
                #     "terms": "https://exchangerate.host/terms",
                #     "privacy": "https://exchangerate.host/privacy",
                #     "historical": true,
                #     "date": "2005-02-01",
                #     "timestamp": 1107302399,
                #     "source": "USD",
                #     "quotes": {
                #         "USDAED": 3.67266,
                #         "USDALL": 96.848753,
                #         "USDAMD": 475.798297,
                #         "USDANG": 1.790403,
                #         "USDARS": 2.918969,
                #         "USDAUD": 1.293878,
                #         [...]
                #     }
                # }
                data: Any = response.json()
                if data[_SUCCESS]:
                    market: str = f"USD{to_asset}" if to_asset != "USD" else f"USD{from_asset}"
                    usd_rate: RP2Decimal = RP2Decimal(str(data[_QUOTES][market]))
                    usd_result = HistoricalBar(
                        duration=timedelta(seconds=_DAYS_IN_SECONDS),
                        timestamp=timestamp,
                        open=usd_rate,
                        high=usd_rate,
                        low=usd_rate,
                        close=usd_rate,
                        volume=ZERO,
                    )
                    self._add_bar_to_cache(key, usd_result)

                    # Note: the from_asset and to_asset are purposely reversed
                    reverse_key: AssetPairAndTimestamp = AssetPairAndTimestamp(timestamp, to_asset, from_asset, _FIAT_EXCHANGE)
                    reverse_rate: RP2Decimal = RP2Decimal("1") / usd_rate
                    reverse_result = HistoricalBar(
                        duration=timedelta(seconds=_DAYS_IN_SECONDS),
                        timestamp=timestamp,
                        open=reverse_rate,
                        high=reverse_rate,
                        low=reverse_rate,
                        close=reverse_rate,
                        volume=ZERO,
                    )
                    self._add_bar_to_cache(reverse_key, reverse_result)

                    if from_asset == "USD":
                        result = usd_result
                    else:
                        result = reverse_result
                break

            except (JSONDecodeError, ReadTimeout) as exc:
                LOGGER.debug("Fetching of fiat exchange rates failed. The server might be down. Retrying the connection.")
                request_count += 1
                if request_count > 4:
                    LOGGER.info("Giving up after 4 tries. Saving to Cache.")
                    self.save_historical_price_cache()
                    raise RP2RuntimeError("JSON decode error") from exc

        return result

    def _check_fiat_access_key(self) -> None:
        if self.__fiat_access_key is None:
            raise RP2ValueError(
                f"No fiat access key. To convert fiat assets, please acquire an access key from exchangerate.host."
                f"The access key will then need to be added to the configuration file. For more details visit "
                f"{_CONFIG_DOC_FILE_URL}"
            )
