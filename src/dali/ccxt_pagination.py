# Copyright 2022 macanudo527
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# CCXT documentation:
# https://docs.ccxt.com/en/latest/index.html

# pylint: disable=fixme

from datetime import datetime
from typing import Any, Dict, List, NamedTuple, Optional, Union

from rp2.rp2_error import RP2RuntimeError

_TIMESTAMP: str = "timestamp"

# Time period constants
_NINETY_DAYS_IN_MS: int = 7776000000
_THIRTY_DAYS_IN_MS: int = 2592000000
_ONE_DAY_IN_MS: int = 86400000
_MS_IN_SECOND: int = 1000

_DEFAULT_WINDOW: int = _THIRTY_DAYS_IN_MS


class PaginationDetails(NamedTuple):
    symbol: Optional[str]
    since: Optional[int]
    limit: Optional[int]
    params: Optional[Dict[str, Union[int, str, None]]]


class AbstractPaginationDetailSet:
    def __iter__(self) -> "AbstractPaginationDetailsIterator":
        raise NotImplementedError("Abstract method")


class DateBasedPaginationDetailSet(AbstractPaginationDetailSet):
    def __init__(
        self,
        exchange_start_time: int,
        limit: Optional[int] = None,
        markets: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[int, str, None]]] = None,
        window: Optional[int] = None,
    ) -> None:
        params = {} if params is None else params
        super().__init__()
        self.__exchange_start_time: int = exchange_start_time
        self.__limit: Optional[int] = limit
        self.__markets: Optional[List[str]] = markets
        self.__params: Optional[Dict[str, Union[int, str, None]]] = params
        self.__window: Optional[int] = window

    def __iter__(self) -> "DateBasedPaginationDetailsIterator":
        return DateBasedPaginationDetailsIterator(
            self.__exchange_start_time,
            self.__limit,
            self.__markets,
            self.__params,
            self.__window,
        )

    def _get_window(self) -> Optional[int]:
        return self.__window

    def _get_exchange_start_time(self) -> int:
        return self.__exchange_start_time

    def _get_limit(self) -> Optional[int]:
        return self.__limit

    def _get_markets(self) -> Optional[List[str]]:
        return self.__markets

    def _get_params(self) -> Optional[Dict[str, Union[int, str, None]]]:
        return self.__params


class CustomDateBasedPaginationDetailSet(DateBasedPaginationDetailSet):
    def __init__(
        self,
        exchange_start_time: int,
        start_time_key: str,
        end_time_key: str,
        window: int,
        limit: Optional[int] = None,
        markets: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[int, str, None]]] = None,
    ) -> None:

        super().__init__(exchange_start_time, limit, markets, params, window)
        self.__start_time_key: str = start_time_key
        self.__end_time_key: str = end_time_key

    def _get_window(self) -> int:
        if self.__window:
            return self.__window
        raise RP2RuntimeError("No window defined for iterator.")

    def __iter__(self) -> "CustomDateBasedPaginationDetailsIterator":
        return CustomDateBasedPaginationDetailsIterator(
            self._get_exchange_start_time(),
            self.__start_time_key,
            self.__end_time_key,
            self._get_window(),
            self._get_limit(),
            self._get_markets(),
            self._get_params(),
        )


class AbstractPaginationDetailsIterator:
    def __init__(self, limit: Optional[int], markets: Optional[List[str]] = None, params: Optional[Dict[str, Union[int, str, None]]] = None) -> None:
        params = {} if params is None else params
        self.__limit: Optional[int] = limit
        self.__markets: Optional[List[str]] = markets
        self.__market_count: int = 0
        self.__params: Optional[Dict[str, Union[int, str, None]]] = params

    def _get_market(self) -> Optional[str]:
        return self.__markets[self.__market_count] if self.__markets else None

    def _has_more_markets(self) -> bool:
        return self.__market_count < (len(self.__markets) - 1) if self.__markets else False

    def _next_market(self) -> None:
        self.__market_count += 1

    def _get_limit(self) -> Optional[int]:
        return self.__limit

    def _get_params(self) -> Optional[Dict[str, Union[int, str, None]]]:
        return self.__params

    def _get_since(self) -> Optional[int]:
        return None

    def update_fetched_elements(self, current_results: Any) -> None:
        raise NotImplementedError("Abstract method")

    def __next__(self) -> PaginationDetails:
        raise NotImplementedError("Abstract method")


class DateBasedPaginationDetailsIterator(AbstractPaginationDetailsIterator):
    def __init__(
        self,
        exchange_start_time: int,
        limit: Optional[int] = None,
        markets: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[int, str, None]]] = None,
        window: Optional[int] = None,
    ) -> None:

        super().__init__(limit, markets, params)
        self.__end_of_data = False
        self.__since: int = exchange_start_time
        self.__exchange_start_time: int = exchange_start_time
        self.__now: int = int(datetime.now().timestamp()) * _MS_IN_SECOND
        self.__window: Optional[int] = window

    def update_fetched_elements(self, current_results: Any) -> None:

        end_of_market: bool = False

        # Update Since if needed otherwise end_of_market
        if len(current_results) == self._get_limit():
            # All times are inclusive
            self.__since = current_results[len(current_results) - 1][_TIMESTAMP] + 1
        elif self.__window:
            self.__since += self.__window
            if self.__since > self.__now:
                end_of_market = True
        else:
            end_of_market = True

        if end_of_market:
            if self._has_more_markets():
                # we have reached the end of one market, now let's move on to the next
                self.__since = self.__exchange_start_time
                self._next_market()
            else:
                self.__end_of_data = True

    def _is_end_of_data(self) -> bool:
        return self.__end_of_data

    def _get_since(self) -> int:
        return self.__since

    def _get_end_of_window(self) -> int:
        if self.__window:
            return self.__since + self.__window
        raise RP2RuntimeError("No window defined for iterator.")

    def __next__(self) -> PaginationDetails:
        while not self._is_end_of_data():
            return PaginationDetails(
                symbol=self._get_market(),
                since=self._get_since(),
                limit=self._get_limit(),
                params=self._get_params(),
            )
        raise StopIteration(self)


class CustomDateBasedPaginationDetailsIterator(DateBasedPaginationDetailsIterator):
    def __init__(
        self,
        exchange_start_time: int,
        start_time_key: str,
        end_time_key: str,
        window: int,
        limit: Optional[int] = None,
        markets: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[int, str, None]]] = None,
    ) -> None:

        super().__init__(exchange_start_time, limit, markets, params, window)
        self.__start_time_key: str = start_time_key
        self.__end_time_key: str = end_time_key

    def __next__(self) -> PaginationDetails:

        while not self._is_end_of_data():
            base_details: PaginationDetails = super().__next__()
            if base_details.params:
                base_details.params[self.__start_time_key] = base_details.since
                base_details.params[self.__end_time_key] = self._get_end_of_window()
            else:
                base_details._replace(params={self.__start_time_key: base_details.since, self.__end_time_key: self._get_end_of_window()})
            return base_details
        raise StopIteration(self)


# TODO: Add IdBasedPaginationDetails and PageNumberBasedPaginationDetails classes
