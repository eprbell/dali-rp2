# Copyright 2026 Neal Chambers
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

# Kraken's public Trades endpoint returns the trades after any point in time, unlike its OHLC endpoint, which only returns
# the latest 720 candles. So it prices times that Kraken's CSV data doesn't cover yet (e.g. the current quarter) as
# accurately as the CSV data, at the cost of about one request per price.

from datetime import datetime, timedelta, timezone
from time import monotonic, sleep
from typing import Any, Dict, List, Optional, Tuple

from requests import RequestException
from requests.sessions import Session
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError

from dali.historical_bar import HistoricalBar

KRAKEN_TRADES_URL: str = "https://api.kraken.com/0/public/Trades"

# Timeframes (in minutes) searched for trades, the same ones Kraken's CSV candles are searched in
_TIMEFRAMES: Tuple[int, ...] = (1, 5, 15, 60, 720, 1440)
_TRADES_PER_REQUEST: int = 1000
_SECONDS_BETWEEN_REQUESTS: float = 1.0  # Kraken allows about one public request per second
_RETRIES: int = 5
_TIMEOUT: int = 30
_UNKNOWN_PAIR: str = "EQuery:Unknown asset pair"
_RATE_LIMITED: Tuple[str, ...] = ("EGeneral:Too many requests", "EAPI:Rate limit exceeded")


class KrakenTrades:
    def __init__(self, session: Session) -> None:
        self.__session: Session = session
        self.__last_request: float = 0.0

    # Candle of the trades in the smallest timeframe around the timestamp that has any, like Kraken's CSV candles.
    # None if Kraken doesn't know the pair or it had no trades that day.
    def find_bar(self, pair: str, timestamp: int) -> Optional[HistoricalBar]:
        for minutes in _TIMEFRAMES:
            start: int = timestamp - timestamp % (minutes * 60)
            trades: Optional[List[Tuple[RP2Decimal, RP2Decimal]]] = self.__trades(pair, start, start + minutes * 60)
            if trades is None:
                return None
            if trades:
                prices: List[RP2Decimal] = [price for price, _ in trades]
                return HistoricalBar(
                    duration=timedelta(minutes=minutes),
                    timestamp=datetime.fromtimestamp(start, timezone.utc),
                    open=prices[0],
                    high=max(prices),
                    low=min(prices),
                    close=prices[-1],
                    volume=sum((volume for _, volume in trades), ZERO),
                )
        return None

    # Price and volume of the trades in [start, end), or None if Kraken doesn't know the pair
    def __trades(self, pair: str, start: int, end: int) -> Optional[List[Tuple[RP2Decimal, RP2Decimal]]]:
        trades: List[Tuple[RP2Decimal, RP2Decimal]] = []
        since: str = str(start)
        while True:
            result: Optional[Dict[str, Any]] = self.__get(pair, since)
            if result is None:
                return None
            # The result has the trades under Kraken's name for the pair, and a cursor for the next request under "last"
            rows: List[List[Any]] = next(value for key, value in result.items() if key != "last")
            for price, volume, time, *_ in rows:
                if float(time) >= end:
                    return trades
                if float(time) >= start:
                    trades.append((RP2Decimal(str(price)), RP2Decimal(str(volume))))
            if len(rows) < _TRADES_PER_REQUEST:
                return trades
            since = str(result["last"])

    # The result of a Trades request, or None if Kraken doesn't know the pair.
    # Requests are spaced out to respect Kraken's rate limit, and retried when rate limited or on network errors.
    def __get(self, pair: str, since: str) -> Optional[Dict[str, Any]]:
        attempt: int = 0
        while True:
            sleep(max(0.0, self.__last_request + _SECONDS_BETWEEN_REQUESTS - monotonic()))
            self.__last_request = monotonic()
            try:
                response = self.__session.get(KRAKEN_TRADES_URL, params={"pair": pair, "since": since, "count": str(_TRADES_PER_REQUEST)}, timeout=_TIMEOUT)
                response.raise_for_status()
                body: Dict[str, Any] = response.json()
            except (RequestException, ValueError) as exc:
                failure: str = str(exc)
            else:
                errors: List[str] = body.get("error", [])
                if not errors:
                    return dict(body["result"])
                if _UNKNOWN_PAIR in errors:
                    return None
                if not any(error in _RATE_LIMITED for error in errors):
                    raise RP2RuntimeError(f"Kraken's Trades endpoint returned {', '.join(errors)} for {pair}")
                failure = ", ".join(errors)
            attempt += 1
            if attempt == _RETRIES:
                raise RP2RuntimeError(f"Couldn't get the {pair} trades from Kraken after {_RETRIES} attempts ({failure})")
            sleep(2**attempt)
