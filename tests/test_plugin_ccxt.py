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

from datetime import datetime, timezone
from typing import Any, Dict

import pytest
from ccxt import binance
from prezzemolo.avl_tree import AVLTree
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.configuration import Keyword
from dali.mapped_graph import MappedGraph
from dali.plugin.pair_converter.ccxt import PairConverterPlugin

# Fiat to Fiat Test
EUR_USD_RATE: RP2Decimal = RP2Decimal("1.0847")
EUR_USD_VOLUME: RP2Decimal = RP2Decimal("1.0")


class TestCcxtPlugin:
    @pytest.mark.vcr
    def test_build_fiat_list(
        self, test_exchange: str, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]], mocker: Any
    ) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )

        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {test_exchange: ["WHATEVER"]})
        mocker.patch.object(plugin, "_generate_unoptimized_graph").return_value = graph_optimized
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_2_graph_tree", {test_exchange: simple_tree})
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchanges", {test_exchange: exchange})

        plugin._build_fiat_list()  # pylint: disable=protected-access

        # This will change over time, but the test will alert us to new pairs available
        assert plugin._fiat_list == [  # pylint: disable=protected-access
            "AUD",
            "BGN",
            "BRL",
            "CAD",
            "CHF",
            "CNY",
            "CZK",
            "DKK",
            "EUR",
            "GBP",
            "HKD",
            "HUF",
            "IDR",
            "ILS",
            "INR",
            "ISK",
            "JPY",
            "KRW",
            "MXN",
            "MYR",
            "NOK",
            "NZD",
            "PHP",
            "PLN",
            "RON",
            "SEK",
            "SGD",
            "THB",
            "TRY",
            "USD",
            "ZAR",
        ]

    @pytest.mark.vcr
    def test_get_fiat_exchange_rate(
        self, test_exchange: str, graph_optimized: MappedGraph[str], simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]], mocker: Any
    ) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )

        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {test_exchange: ["WHATEVER"]})
        mocker.patch.object(plugin, "_generate_unoptimized_graph").return_value = graph_optimized
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_2_graph_tree", {test_exchange: simple_tree})
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchanges", {test_exchange: exchange})
        mocker.patch.object(plugin, "_add_bar_to_cache").side_effect = plugin._add_bar_to_cache  # pylint: disable=protected-access

        # Friday rate is used for Saturday and Sunday since there is no trading for ECB on weekends
        friday_data = plugin._get_fiat_exchange_rate(   # pylint: disable=protected-access
            datetime(2020, 12, 31, 0, 0).replace(tzinfo=timezone.utc), "EUR", "USD"
        )
        saturday_data = plugin._get_fiat_exchange_rate( # pylint: disable=protected-access
            datetime(2021, 1, 2, 0, 0).replace(tzinfo=timezone.utc), "EUR", "USD"
        )

        assert friday_data
        assert saturday_data
        assert friday_data.timestamp == datetime(2020, 12, 31, 0, 0).replace(tzinfo=timezone.utc)
        assert saturday_data.timestamp == datetime(2021, 1, 2, 0, 0).replace(tzinfo=timezone.utc)
        assert friday_data.low == saturday_data.low == EUR_USD_RATE
        assert friday_data.high == saturday_data.high == EUR_USD_RATE
        assert friday_data.open == saturday_data.open == EUR_USD_RATE
        assert friday_data.close == saturday_data.close == EUR_USD_RATE
        assert friday_data.volume == saturday_data.volume == ZERO

        # Did it cache an entire year?
        # 365 + 2020 was a leap year (+1), Jan 1st, 2020 is a holiday and not generated (-1), but Jan 1st-3rd of 2021 is generated from Dec 31, 2020 (+3).
        assert plugin._add_bar_to_cache.call_count == 368  # type: ignore # pylint: disable=protected-access, no-member

        # Clear cache for the reverse test
        plugin._cache = {}  # pylint: disable=protected-access

        # Friday rate is used for Saturday and Sunday since there is no trading for ECB on weekends
        # Check to see if plugin will retrieve Friday rate when Saturday is requested first
        saturday_data = plugin._get_fiat_exchange_rate( # pylint: disable=protected-access
            datetime(2021, 1, 2, 0, 0).replace(tzinfo=timezone.utc), "EUR", "USD"
        )
        friday_data = plugin._get_fiat_exchange_rate(   # pylint: disable=protected-access
            datetime(2020, 12, 31, 0, 0).replace(tzinfo=timezone.utc), "EUR", "USD"
        )

        assert friday_data
        assert saturday_data
        assert friday_data.timestamp == datetime(2020, 12, 31, 0, 0).replace(tzinfo=timezone.utc)
        assert saturday_data.timestamp == datetime(2021, 1, 2, 0, 0).replace(tzinfo=timezone.utc)
        assert friday_data.low == saturday_data.low == EUR_USD_RATE
        assert friday_data.high == saturday_data.high == EUR_USD_RATE
        assert friday_data.open == saturday_data.open == EUR_USD_RATE
        assert friday_data.close == saturday_data.close == EUR_USD_RATE
        assert friday_data.volume == saturday_data.volume == ZERO

        # Did it cache an entire year, again? 368 x 2 = 736
        assert plugin._add_bar_to_cache.call_count == 736  # type: ignore # pylint: disable=protected-access, no-member
