# Copyright 2022 Neal Chambers
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

from datetime import datetime, timedelta, timezone
from json import JSONDecodeError
from typing import Any, Dict, Optional, Set, Tuple

import requests
from requests.exceptions import ReadTimeout
from requests.models import Response
from requests.sessions import Session
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError, RP2ValueError

from dali.abstract_ccxt_pair_converter_plugin import (
    FIAT_PRIORITY,
    STANDARD_INCREMENT,
    STANDARD_WEIGHT,
    AbstractCcxtPairConverterPlugin,
)
from dali.abstract_pair_converter_plugin import AssetPairAndTimestamp
from dali.historical_bar import HistoricalBar
from dali.logger import LOGGER

# Frankfurter URLs
_FRANKFURTER_EXCHANGE: str = "Frankfurter"
_EXCHANGE_BASE_URL: str = "https://api.frankfurter.app/"
_EXCHANGE_SYMBOLS_URL: str = "https://api.frankfurter.app/currencies"

# Params for Frankfurter API
_AMOUNT: str = "amount"
_FROM: str = "from"
_RATES: str = "rates"
_TO: str = "to"

_DAYS_IN_SECONDS: int = 86400


class PairConverterPlugin(AbstractCcxtPairConverterPlugin):
    __TIMEOUT: int = 30

    def __init__(
        self,
        historical_price_type: str,
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
        if fiat_priority:
            weight: float = STANDARD_WEIGHT
            for fiat in fiat_priority:
                self._fiat_priority[fiat] = weight
                weight += STANDARD_INCREMENT
        else:
            self._fiat_priority = FIAT_PRIORITY
        self.__session: Session = requests.Session()

    def name(self) -> str:
        return "Frankfurter"

    def _get_fiat_exchange_rate(self, timestamp: datetime, from_asset: str, to_asset: str) -> Optional[HistoricalBar]:
        # Floor the key to days because the API only provides daily rates
        key: AssetPairAndTimestamp = self._floor_key(AssetPairAndTimestamp(timestamp, from_asset, to_asset, _FRANKFURTER_EXCHANGE), True)
        historical_bar: Optional[HistoricalBar] = self._get_bar_from_cache(key)

        if historical_bar is not None:
            self._logger.debug("Retrieved cache for %s/%s->%s for %s", timestamp, from_asset, to_asset, _FRANKFURTER_EXCHANGE)
            return historical_bar

        result: Optional[HistoricalBar] = None

        params: Dict[str, Any] = {_FROM: from_asset, _TO: to_asset}
        request_count: int = 0

        beginning_of_year, end_of_year = self._year_start_end(timestamp)

        # frankfurter only provides daily rates, and does not provide rates for bank holidays.
        while request_count < 5:
            try:
                response: Response = self.__session.get(
                    f"{_EXCHANGE_BASE_URL}{beginning_of_year.strftime('%Y-%m-%d')}..{end_of_year.strftime('%Y-%m-%d')}", params=params, timeout=self.__TIMEOUT
                )
                # {
                #    "amount": 1.0,
                #    "base": "USD",
                #    "start_date": "2023-01-02",
                #    "end_date": "2023-12-29",
                #    "rates": {
                #        "2023-01-02": {
                #            "JPY": 130.69
                #        },
                #        "2023-01-03": {
                #            "JPY": 130.8
                #        },
                #        "2023-01-04": {
                #            "JPY": 130.9
                #        },
                #        [...]
                #     }
                # }
                data: Any = response.json()

                # This is a sanity check to make sure the format hasn't changed and that the request was successful.
                if data[_AMOUNT] == 1.0:
                    market: str = f"{from_asset}{to_asset}"
                    rates: Dict[str, Any] = data[_RATES]
                    previous_result: Optional[HistoricalBar] = None

                    for day in range((end_of_year - beginning_of_year).days + 1):
                        current_day: datetime = beginning_of_year + timedelta(days=day)

                        try:
                            forex_rate: RP2Decimal = RP2Decimal(str(rates[current_day.strftime("%Y-%m-%d")][to_asset]))
                            forex_result = HistoricalBar(
                                duration=timedelta(seconds=_DAYS_IN_SECONDS),
                                timestamp=current_day,
                                open=forex_rate,
                                high=forex_rate,
                                low=forex_rate,
                                close=forex_rate,
                                volume=ZERO,
                            )
                        except KeyError as exc:
                            if previous_result:
                                forex_result = HistoricalBar(
                                    duration=timedelta(seconds=_DAYS_IN_SECONDS),
                                    timestamp=current_day,
                                    open=previous_result.open,
                                    high=previous_result.high,
                                    low=previous_result.low,
                                    close=previous_result.close,
                                    volume=ZERO,
                                )
                            else:
                                raise RP2ValueError(f"No forex rate found for {current_day} for {from_asset} to {to_asset} in {market}") from exc

                        self._add_bar_to_cache(
                            self._floor_key(AssetPairAndTimestamp(current_day, from_asset, to_asset, _FRANKFURTER_EXCHANGE), True), forex_result
                        )
                        previous_result = forex_result
                        if current_day == key.timestamp.replace(tzinfo=timezone.utc):
                            result = forex_result

                    # Add weekends
                    extra_days: Set[datetime] = {current_day + timedelta(days=1)}
                    if (current_day + timedelta(days=2)).weekday() in [5, 6]:
                        extra_days.add(current_day + timedelta(days=2))
                    if (current_day + timedelta(days=3)).weekday() in [5, 6]:
                        extra_days.add(current_day + timedelta(days=3))
                    for extra_day in extra_days:
                        forex_rate = forex_result.close
                        forex_result = HistoricalBar(
                            duration=timedelta(seconds=_DAYS_IN_SECONDS),
                            timestamp=extra_day,
                            open=forex_rate,
                            high=forex_rate,
                            low=forex_rate,
                            close=forex_rate,
                            volume=ZERO,
                        )
                        if extra_day == key.timestamp.replace(tzinfo=timezone.utc):
                            result = forex_result
                        self._add_bar_to_cache(
                            self._floor_key(AssetPairAndTimestamp(extra_day, from_asset, to_asset, _FRANKFURTER_EXCHANGE), True), forex_result
                        )
                    break

            except (JSONDecodeError, ReadTimeout) as exc:
                self._logger.debug("Fetching of fiat exchange rates failed. The server might be down. Retrying the connection.")
                request_count += 1
                if request_count > 4:
                    self._logger.info("Giving up after 4 tries. Saving to Cache.")
                    self.save_historical_price_cache()
                    raise RP2RuntimeError("JSON decode error") from exc

        return result

    def _is_fiat(self, asset: str) -> bool:
        if not self._fiat_list:
            self._build_fiat_list()

        return asset in self._fiat_list

    def _build_fiat_list(self) -> None:
        try:
            response: Response = self.__session.get(_EXCHANGE_SYMBOLS_URL, timeout=self.__TIMEOUT)
            # {
            #     "AUD": "Australian Dollar",
            #     "BGN": "Bulgarian Lev",
            #     "BRL": "Brazilian Real",
            #     "CAD": "Canadian Dollar",
            #     "CHF": "Swiss Franc",
            #     "CNY": "Chinese Renminbi Yuan",
            #     "CZK": "Czech Koruna",
            #     "DKK": "Danish Krone",
            #     "EUR": "Euro",
            #     "GBP": "British Pound",
            #     "HKD": "Hong Kong Dollar",
            #     "HUF": "Hungarian Forint",
            #     "IDR": "Indonesian Rupiah",
            #     "ILS": "Israeli New Sheqel",
            #     "INR": "Indian Rupee",
            #     "ISK": "Icelandic Króna",
            #     "JPY": "Japanese Yen",
            #     "KRW": "South Korean Won",
            #     "MXN": "Mexican Peso",
            #     "MYR": "Malaysian Ringgit",
            #     "NOK": "Norwegian Krone",
            #     "NZD": "New Zealand Dollar",
            #     "PHP": "Philippine Peso",
            #     "PLN": "Polish Złoty",
            #     "RON": "Romanian Leu",
            #     "SEK": "Swedish Krona",
            #     "SGD": "Singapore Dollar",
            #     "THB": "Thai Baht",
            #     "TRY": "Turkish Lira",
            #     "USD": "United States Dollar",
            #     "ZAR": "South African Rand"
            # }

            data: dict[str, str] = response.json()

            self._fiat_list = list(data.keys())

        except JSONDecodeError as exc:
            LOGGER.info("Fetching of fiat symbols failed. The server might be down. Please try again later.")
            raise RP2RuntimeError("JSON decode error") from exc

    def _year_start_end(self, date: datetime) -> Tuple[datetime, datetime]:
        year: int = date.year
        # Check if the input date is January 1st (a holiday) or 2nd or 3rd and is a Saturday or Sunday
        # If it is, use the previous year's data to extrapolate the rates
        if (date.month == 1 and date.day <= 3) and ((date.weekday() in [5, 6]) or date.day == 1):
            year = date.year - 1

        start_of_year = datetime(year, 1, 2).replace(tzinfo=timezone.utc)

        # Adjust start_of_year if it falls on a weekend
        while start_of_year.weekday() >= 5:
            start_of_year += timedelta(days=1)

        end_of_year = datetime(year, 12, 31).replace(tzinfo=timezone.utc)

        return start_of_year, end_of_year
