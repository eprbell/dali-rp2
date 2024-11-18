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

from datetime import datetime
from typing import Optional, Set

import pytest
from prezzemolo.vertex import Vertex

from dali.abstract_ccxt_pair_converter_plugin import AbstractCcxtPairConverterPlugin
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.mapped_graph import MappedGraph


class MockAbstractCcxtPairConverterPlugin(AbstractCcxtPairConverterPlugin):
    def name(self) -> str:
        return "MockPlugin"

    def _get_fiat_exchange_rate(self, timestamp: datetime, from_asset: str, to_asset: str) -> Optional[HistoricalBar]:
        pass

    def _build_fiat_list(self) -> None:
        pass


class TestAbstractCcxtPairConverterPlugin:
    @pytest.fixture
    def unoptimized_graph(self) -> MappedGraph[str]:
        graph = MappedGraph[str]("TestExchange")
        vertex_a = Vertex[str]("A")
        vertex_b = Vertex[str]("B")
        vertex_c = Vertex[str]("C")
        vertex_d = Vertex[str]("D")

        vertex_a.add_neighbor(vertex_b, 1.0)
        vertex_b.add_neighbor(vertex_c, 1.0)

        graph.add_vertex(vertex_a)
        graph.add_vertex(vertex_b)
        graph.add_vertex(vertex_c)
        graph.add_vertex(vertex_d)

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
