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
from typing import Any, Dict, List, Optional, Set

import pytest
from prezzemolo.vertex import Vertex
from rp2.rp2_decimal import RP2Decimal

from dali.abstract_ccxt_pair_converter_plugin import (
    MARKET_PADDING_IN_WEEKS,
    AbstractCcxtPairConverterPlugin,
)
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.mapped_graph import MappedGraph

TEST_EXCHANGE: str = "Kraken"
TEST_MARKETS: Dict[str, List[str]] = {
    "AB": [TEST_EXCHANGE],
    "BC": [TEST_EXCHANGE],
}


class MockAbstractCcxtPairConverterPlugin(AbstractCcxtPairConverterPlugin):
    def name(self) -> str:
        return "MockPlugin"

    def _get_fiat_exchange_rate(self, timestamp: datetime, from_asset: str, to_asset: str) -> Optional[HistoricalBar]:
        pass

    def _build_fiat_list(self) -> None:
        pass


class TestAbstractCcxtPairConverterPlugin:
    @pytest.fixture
    def vertex_list(self) -> Dict[str, Vertex[str]]:
        return {
            "A": Vertex[str]("A"),
            "B": Vertex[str]("B"),
            "C": Vertex[str]("C"),
            "D": Vertex[str]("D"),
        }

    @pytest.fixture
    def unoptimized_graph(self, vertex_list: Dict[str, Vertex[str]]) -> MappedGraph[str]:
        graph = MappedGraph[str]("TestExchange")

        vertex_list["A"].add_neighbor(vertex_list["B"], 1.0)
        vertex_list["B"].add_neighbor(vertex_list["C"], 1.0)

        graph.add_vertex(vertex_list["A"])
        graph.add_vertex(vertex_list["B"])
        graph.add_vertex(vertex_list["C"])
        graph.add_vertex(vertex_list["D"])

        return graph

    ## _optimize_assets_for_exchange Tests ##
    def test_gather_optimization_candidates(self, unoptimized_graph: MappedGraph[str]) -> None:
        plugin = MockAbstractCcxtPairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        assets: Set[str] = {"A", "B"}

        # Test with 4 vertices, 2 assets
        candidates = plugin._gather_optimization_candidates(unoptimized_graph, assets)  # pylint: disable=protected-access

        assert len(candidates) == 3
        assert any(vertex.name == "A" for vertex in candidates)
        assert any(vertex.name == "B" for vertex in candidates)
        assert any(vertex.name == "C" for vertex in candidates)
        assert not any(vertex.name == "D" for vertex in candidates)

    def test_retrieve_historical_bars(self, mocker: Any, unoptimized_graph: MappedGraph[str], vertex_list: Dict[str, Vertex[str]]) -> None:
        plugin = MockAbstractCcxtPairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        unoptimized_assets: Set[str] = {"A", "B"}
        optimization_candidates: Set[Vertex[str]] = {vertex_list["A"], vertex_list["B"], vertex_list["C"]}
        week_start_date = datetime(2023, 1, 1)

        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {TEST_EXCHANGE: TEST_MARKETS})

        def find_historical_bars_side_effect(
            from_asset: str, to_asset: str, timestamp: datetime, exchange: str, all_bars: bool, timespan: str  # pylint: disable=unused-argument
        ) -> Optional[List[HistoricalBar]]:
            if from_asset == "A":
                return [
                    HistoricalBar(
                        duration=timedelta(weeks=1),
                        timestamp=week_start_date,
                        open=RP2Decimal("1.0"),
                        high=RP2Decimal("2.0"),
                        low=RP2Decimal("0.5"),
                        close=RP2Decimal("1.5"),
                        volume=RP2Decimal("100.0"),
                    )
                ]
            if from_asset == "B":
                return [
                    HistoricalBar(
                        duration=timedelta(weeks=1),
                        timestamp=week_start_date - timedelta(weeks=1),
                        open=RP2Decimal("1.1"),
                        high=RP2Decimal("2.1"),
                        low=RP2Decimal("0.6"),
                        close=RP2Decimal("1.6"),
                        volume=RP2Decimal("110.0"),
                    )
                ]
            return []

        mocker.patch.object(plugin, "find_historical_bars", side_effect=find_historical_bars_side_effect)

        child_bars, market_starts = plugin._retrieve_historical_bars(  # pylint: disable=protected-access
            unoptimized_assets, optimization_candidates, week_start_date, TEST_EXCHANGE, unoptimized_graph
        )

        assert len(child_bars) == 2
        assert "A" in child_bars
        assert "B" in child_bars
        assert len(child_bars["A"]) == 1
        assert len(child_bars["B"]) == 1
        assert len(market_starts) == 2
        assert "A" in market_starts
        assert "B" in market_starts
        assert market_starts["A"]["B"] == week_start_date - timedelta(weeks=MARKET_PADDING_IN_WEEKS)
        assert market_starts["B"]["C"] == week_start_date - timedelta(weeks=MARKET_PADDING_IN_WEEKS + 1)
