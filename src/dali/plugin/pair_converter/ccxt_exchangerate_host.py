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

from datetime import datetime, timedelta
from json import JSONDecodeError
from typing import Any, Dict, List, Optional

import requests
from requests.exceptions import ReadTimeout
from requests.models import Response
from requests.sessions import Session
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError, RP2ValueError

from dali.abstract_ccxt_pair_converter_plugin import (
    FIAT_PRIORITY,
    AbstractCcxtPairConverterPlugin,
)
from dali.abstract_pair_converter_plugin import AssetPairAndTimestamp
from dali.historical_bar import HistoricalBar
from dali.logger import LOGGER

# exchangerates.host urls
_EXCHANGE_BASE_URL: str = "http://api.exchangerate.host/historical"
_EXCHANGE_SYMBOLS_URL: str = "http://api.exchangerate.host/list"

_DAYS_IN_SECONDS: int = 86400
_FIAT_EXCHANGE: str = "exchangerate.host"

# exchangerates.host keywords
_ACCESS_KEY: str = "access_key"
_CURRENCIES: str = "currencies"
_DATE: str = "date"
_QUOTES: str = "quotes"
_SUCCESS: str = "success"

# Other Weights
_STANDARD_WEIGHT: float = 1
_STANDARD_INCREMENT: float = 1


class PairConverterPlugin(AbstractCcxtPairConverterPlugin):
    __TIMEOUT: int = 30

    def __init__(
        self,
        historical_price_type: str,
        fiat_access_key: str,
        default_exchange: Optional[str] = None,
        fiat_priority: Optional[str] = None,
        exchange_locked: Optional[bool] = None,
        untradeable_assets: Optional[str] = None,
        aliases: Optional[str] = None,
    ) -> None:
        cache_modifier = fiat_priority if fiat_priority else ""
        super().__init__(
            historical_price_type=historical_price_type,
            exchange_locked=exchange_locked,
            untradeable_assets=untradeable_assets,
            aliases=aliases,
            cache_modifier=cache_modifier,
        )
        self.__fiat_list: List[str] = []
        self._fiat_priority: Dict[str, float]
        if fiat_priority:
            weight: float = _STANDARD_WEIGHT
            for fiat in fiat_priority:
                self._fiat_priority[fiat] = weight
                weight += _STANDARD_INCREMENT
        else:
            self._fiat_priority = FIAT_PRIORITY
        self.__fiat_access_key = fiat_access_key
        self.__session: Session = requests.Session()

    def name(self) -> str:
        return "Fiat from exchangerate.host"

    def _get_fiat_exchange_rate(self, timestamp: datetime, from_asset: str, to_asset: str) -> Optional[HistoricalBar]:
        key: AssetPairAndTimestamp = AssetPairAndTimestamp(timestamp, from_asset, to_asset, _FIAT_EXCHANGE)
        historical_bar: Optional[HistoricalBar] = self._get_bar_from_cache(key)

        if historical_bar is not None:
            LOGGER.debug("Retrieved cache for %s/%s->%s for %s", timestamp, from_asset, to_asset, _FIAT_EXCHANGE)
            return historical_bar

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

                # Exchangerate.host only returns one rate for the whole day and does not provide OHLCV, so
                # all rates are the same.
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

                    # Exchangerate.host only returns one rate for the whole day and does not provide OHLCV, so
                    # all rates are the same.
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

                    result = usd_result
                    if from_asset != "USD":
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

    def _is_fiat(self, asset: str) -> bool:
        if not self.__fiat_list:
            self._build_fiat_list()

        return asset in self.__fiat_list

    def _is_fiat_pair(self, from_asset: str, to_asset: str) -> bool:
        return self._is_fiat(from_asset) and self._is_fiat(to_asset)

    def _build_fiat_list(self) -> None:
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
