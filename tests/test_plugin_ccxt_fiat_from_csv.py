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

from datetime import datetime, timezone
from typing import Any, Dict

from ccxt import binance
from prezzemolo.avl_tree import AVLTree
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.configuration import Keyword
from dali.mapped_graph import MappedGraph
from dali.plugin.pair_converter.ccxt_fiat_from_csv import PairConverterPlugin

# Fiat to Fiat Test
EUR_USD_RATE: RP2Decimal = RP2Decimal("1.0847")
EUR_USD_TIMESTAMP: datetime = datetime.fromtimestamp(1585958400, timezone.utc)


class TestCcxtFiatFromCsvPlugin:
    def test_get_rate_from_csv(
        self,
        mocker: Any,
        graph_optimized: MappedGraph[str],
        simple_tree: AVLTree[datetime, Dict[str, MappedGraph[str]]],
        test_exchange: str,
    ) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        exchange = binance(
            {
                "apiKey": "key",
                "secret": "secret",
            }
        )

        # Need to be mocked to prevent logger spam
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {test_exchange: ["WHATEVER"]})
        mocker.patch.object(plugin, "_generate_unoptimized_graph").return_value = graph_optimized
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_2_graph_tree", {test_exchange: simple_tree})
        mocker.patch.object(plugin, "_PairConverterPlugin__CSV_DIRECTORY", "input/")
        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchanges", {test_exchange: exchange})

        data = plugin._get_fiat_exchange_rate(EUR_USD_TIMESTAMP, "EUR", "USD")  # pylint: disable=protected-access

        assert data
        assert data.timestamp == EUR_USD_TIMESTAMP
        assert data.low == EUR_USD_RATE
        assert data.high == EUR_USD_RATE
        assert data.open == EUR_USD_RATE
        assert data.close == EUR_USD_RATE
        assert data.volume == ZERO
