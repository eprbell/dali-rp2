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
from json import JSONDecodeError, loads
from typing import Any, Dict, List, NamedTuple, Optional, cast

import requests
from requests.exceptions import ReadTimeout
from requests.models import Response
from requests.sessions import Session
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError, RP2TypeError

from dali.cache import load_from_cache, save_to_cache
from dali.configuration import HISTORICAL_PRICE_KEYWORD_SET
from dali.historical_bar import HistoricalBar
from dali.logger import LOGGER

# exchangerates.host keywords
_SUCCESS: str = "success"
_SYMBOLS: str = "symbols"
_RATES: str = "rates"

# exchangerates.host urls
_EXCHANGE_BASE_URL: str = "https://api.exchangerate.host/"
_EXCHANGE_SYMBOLS_URL: str = "https://api.exchangerate.host/symbols"

_DAYS_IN_SECONDS: int = 86400
_FIAT_EXCHANGE: str = "exchangerate.host"

# First on the list has the most priority
# This is hard-coded for now based on volume of each of these markets for BTC on Coinmarketcap.com
# Any change to this priority should be documented in "docs/configuration_file.md"
_FIAT_PRIORITY: List[str] = ["USD", "JPY", "KRW", "EUR", "GBP", "AUD"]


class AssetPairAndTimestamp(NamedTuple):
    timestamp: datetime
    from_asset: str
    to_asset: str
    exchange: str


class AbstractPairConverterPlugin:
    __ISSUES_URL: str = "https://github.com/eprbell/dali-rp2/issues"
    __TIMEOUT: int = 30

    def __init__(self, historical_price_type: str, fiat_priority: Optional[str] = None) -> None:
        if not isinstance(historical_price_type, str):
            raise RP2TypeError(f"historical_price_type is not a string: {historical_price_type}")
        if historical_price_type not in HISTORICAL_PRICE_KEYWORD_SET:
            raise RP2TypeError(
                f"historical_price_type must be one of {', '.join(sorted(HISTORICAL_PRICE_KEYWORD_SET))}, instead it was: {historical_price_type}"
            )
        result = cast(Dict[AssetPairAndTimestamp, HistoricalBar], load_from_cache(self.cache_key()))
        self.__cache: Dict[AssetPairAndTimestamp, HistoricalBar] = result if result is not None else {}
        self.__historical_price_type: str = historical_price_type
        self.__session: Session = requests.Session()
        self.__fiat_list: List[str] = []
        self.__fiat_priority: List[str]
        self.__fiat_priority = loads(fiat_priority) if fiat_priority is not None else _FIAT_PRIORITY

    def name(self) -> str:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def cache_key(self) -> str:
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def _add_bar_to_cache(self, key: AssetPairAndTimestamp, historical_bar: HistoricalBar) -> None:
        self.__cache[self._floor_key(key)] = historical_bar

    def _get_bar_from_cache(self, key: AssetPairAndTimestamp) -> Optional[HistoricalBar]:
        return self.__cache.get(self._floor_key(key))

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
        try:
            response: Response = self.__session.get(_EXCHANGE_SYMBOLS_URL, timeout=self.__TIMEOUT)
            # {
            #     'motd':
            #         {
            #             'msg': 'If you or your company ...',
            #             'url': 'https://exchangerate.host/#/donate'
            #         },
            #     'success': True,
            #     'symbols':
            #         {
            #             'AED':
            #                 {
            #                     'description': 'United Arab Emirates Dirham',
            #                     'code': 'AED'
            #                 },
            #             ...
            #         }
            # }
            data: Any = response.json()
            if data[_SUCCESS]:
                self.__fiat_list = [fiat_iso for fiat_iso in data[_SYMBOLS] if fiat_iso != "BTC"]
            else:
                if "message" in data:
                    LOGGER.error("Error %d: %s: %s", response.status_code, _EXCHANGE_SYMBOLS_URL, data["message"])
                response.raise_for_status()

        except JSONDecodeError as exc:
            LOGGER.info("Fetching of fiat symbols failed. The server might be down. Please try again later.")
            raise RP2RuntimeError("JSON decode error") from exc

    def _add_fiat_edges_to_graph(self, graph: Dict[str, Dict[str, None]], markets: Dict[str, List[str]]) -> None:
        if not self.__fiat_list:
            self._build_fiat_list()

        for fiat in self.__fiat_list:
            to_fiat_list: Dict[str, None] = dict.fromkeys(self.__fiat_list.copy())
            del to_fiat_list[fiat]
            if graph.get(fiat):
                for to_be_added_fiat in to_fiat_list:
                    # add a pair if it doesn't exist
                    if to_be_added_fiat not in graph[fiat]:
                        graph[fiat][to_be_added_fiat] = None
            else:
                graph[fiat] = to_fiat_list

            for to_fiat in to_fiat_list:
                fiat_market = f"{fiat}{to_fiat}"
                markets[fiat_market] = [_FIAT_EXCHANGE]

            # Add prioritized fiat at the beginning
            for priority_fiat in reversed(self.__fiat_priority):
                if priority_fiat in graph[fiat]:
                    graph[fiat].pop(priority_fiat)
                    remainder: Dict[str, None] = graph[fiat]
                    graph[fiat] = {priority_fiat: None}
                    graph[fiat].update(remainder)

            LOGGER.debug("Added to assets for %s: %s", fiat, graph[fiat])

    def _is_fiat_pair(self, from_asset: str, to_asset: str) -> bool:
        return self._is_fiat(from_asset) and self._is_fiat(to_asset)

    def _is_fiat(self, asset: str) -> bool:
        if not self.__fiat_list:
            self._build_fiat_list()

        return asset in self.__fiat_list

    def _get_fiat_exchange_rate(self, timestamp: datetime, from_asset: str, to_asset: str) -> Optional[HistoricalBar]:
        result: Optional[HistoricalBar] = None
        params: Dict[str, Any] = {"base": from_asset, "symbols": to_asset}
        request_count: int = 0
        # exchangerate.host only gives us daily accuracy, which should be suitable for tax reporting
        while request_count < 5:
            try:
                response: Response = self.__session.get(f"{_EXCHANGE_BASE_URL}{timestamp.strftime('%Y-%m-%d')}", params=params, timeout=self.__TIMEOUT)
                # {
                #     'motd':
                #         {
                #             'msg': 'If you or your company ...',
                #             'url': 'https://exchangerate.host/#/donate'
                #         },
                #     'success': True,
                #     'historical': True,
                #     'base': 'EUR',
                #     'date': '2020-04-04',
                #     'rates':
                #         {
                #             'USD': 1.0847, ... // float, Lists all supported currencies unless you specify
                #         }
                # }
                data: Any = response.json()
                if data[_SUCCESS]:
                    result = HistoricalBar(
                        duration=timedelta(seconds=_DAYS_IN_SECONDS),
                        timestamp=timestamp,
                        open=RP2Decimal(str(data[_RATES][to_asset])),
                        high=RP2Decimal(str(data[_RATES][to_asset])),
                        low=RP2Decimal(str(data[_RATES][to_asset])),
                        close=RP2Decimal(str(data[_RATES][to_asset])),
                        volume=ZERO,
                    )
                break

            except (JSONDecodeError, ReadTimeout) as exc:
                LOGGER.debug("Fetching of fiat exchange rates failed. The server might be down. Retrying the connection.")
                request_count += 1
                if request_count > 4:
                    LOGGER.info("Giving up after 4 tries. Saving to Cache.")
                    self.save_historical_price_cache()
                    raise RP2RuntimeError("JSON decode error") from exc

        return result
