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
from os import path
from typing import List, Optional

from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.abstract_ccxt_pair_converter_plugin import (
    DEFAULT_FIAT_LIST,
    AbstractCcxtPairConverterPlugin,
)
from dali.abstract_pair_converter_plugin import AssetPairAndTimestamp
from dali.historical_bar import HistoricalBar

_FOREX_CSV_DOC_URL: str = "https://github.com/eprbell/dali-rp2/blob/main/docs/configuration_file.md"
_FIAT_EXCHANGE: str = "From CSV"


class PairConverterPlugin(AbstractCcxtPairConverterPlugin):
    __CSV_DIRECTORY: str = ".dali_cache/forex/"

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
        self._fiat_list = DEFAULT_FIAT_LIST
        super().__init__(
            historical_price_type=historical_price_type,
            exchange_locked=exchange_locked,
            untradeable_assets=untradeable_assets,
            aliases=aliases,
            cache_modifier=cache_modifier,
        )

    def name(self) -> str:
        return "Fiat from CSV"

    def _get_fiat_exchange_rate(self, timestamp: datetime, from_asset: str, to_asset: str) -> Optional[HistoricalBar]:
        key: AssetPairAndTimestamp = AssetPairAndTimestamp(timestamp, from_asset, to_asset, _FIAT_EXCHANGE)
        historical_bar: Optional[HistoricalBar] = self._get_bar_from_cache(key)

        if historical_bar is not None:
            self._logger.debug("Retrieved cache for %s/%s->%s for %s", timestamp, from_asset, to_asset, _FIAT_EXCHANGE)
            return historical_bar

        csv_file: str = f"{self.__CSV_DIRECTORY}{key.from_asset}_{key.to_asset}.csv"
        file_exists = path.exists(csv_file)
        reverse_pair: bool = False

        if not file_exists:
            csv_file = f"{self.__CSV_DIRECTORY}{key.to_asset}_{key.from_asset}.csv"
            reverse_pair = path.exists(csv_file)

            if not reverse_pair:
                self._logger.info("No CSV file found for %s for %s/%s", key.timestamp, key.from_asset, key.to_asset)
                self._logger.info("Please save a CSV file with pricing information named %s_%s.csv to %s ", key.from_asset, key.to_asset, self.__CSV_DIRECTORY)
                self._logger.info("And try to process your transactions again.")
                self._logger.info("For more details, check the documentation. %s", _FOREX_CSV_DOC_URL)
                return None

        try:
            with open(csv_file, encoding="utf-8") as file:
                lines: List[str] = file.readlines()
                for line in lines:
                    if line.startswith("Time"):
                        continue
                    parts: List[str] = line.split(",")
                    if len(parts) != 6:
                        continue
                    date: datetime = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
                    if date.strftime("%Y-%m-%d") == key.timestamp.strftime("%Y-%m-%d"):
                        result = HistoricalBar(
                            duration=timedelta(days=1),
                            timestamp=key.timestamp,
                            open=RP2Decimal(parts[1]),
                            high=RP2Decimal(parts[2]),
                            low=RP2Decimal(parts[3]),
                            close=RP2Decimal(parts[4]),
                            volume=RP2Decimal(parts[5]),
                        )
                        self._add_bar_to_cache(key, result)

                        reverse_key: AssetPairAndTimestamp = AssetPairAndTimestamp(key.timestamp, key.to_asset, key.from_asset, _FIAT_EXCHANGE)
                        reverse_result = HistoricalBar(
                            duration=timedelta(days=1),
                            timestamp=key.timestamp,
                            open=RP2Decimal("1") / result.close,
                            high=RP2Decimal("1") / result.low,
                            low=RP2Decimal("1") / result.high,
                            close=RP2Decimal("1") / result.open,
                            volume=ZERO,
                        )
                        self._add_bar_to_cache(reverse_key, reverse_result)
                        if reverse_pair:
                            return reverse_result
                        return result
        except FileNotFoundError:
            self._logger.debug("File not found: %s", csv_file)

        # No historical bar found or file not found, return None
        return None

    def _build_fiat_list(self) -> None:
        self._fiat_list = DEFAULT_FIAT_LIST
