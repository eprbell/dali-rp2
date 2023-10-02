# Copyright 2023 Neal Chambers
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
from typing import Iterator, List, Optional

import pytest
from prezzemolo.vertex import Vertex
from rp2.rp2_decimal import RP2Decimal

from dali.historical_bar import HistoricalBar
from dali.mapped_graph import Alias, MappedGraph


class TestMappedGraph:
    # To test that the clones are clean and no references are copied over,
    # make the fixtures have class scope
    @pytest.fixture(scope="class")
    def basic_graph(self) -> MappedGraph[str]:
        current_graph = MappedGraph[str]("some exchange")

        self.add_vertexes_to_graph(current_graph)

        return current_graph

    @pytest.fixture(scope="class")
    def cloned_graph(self, basic_graph: MappedGraph[str]) -> MappedGraph[str]:
        optimizations = {
            "first parent": {"first child": 2.0, "second child": 1.0},
            "second parent": {"first child": 4.0, "second child": 3.0},
        }

        cloned_graph = basic_graph.clone_with_optimization(optimizations)

        return cloned_graph

    @pytest.fixture(scope="class")
    def basic_graph_with_aliases(self) -> MappedGraph[str]:
        aliases = {
            "UNIVERSAL": {
                Alias(from_asset="first parent alias", to_asset="first parent"): RP2Decimal("1"),
                Alias(from_asset="XBT", to_asset="BTC"): RP2Decimal("2"),  # Testing override
            },
            "some exchange": {Alias(from_asset="micro first parent", to_asset="first parent"): RP2Decimal("0.0001")},
            "some other exchange": {Alias(from_asset="macro first parent", to_asset="first parent"): RP2Decimal("10")},
        }

        current_graph = MappedGraph[str]("some exchange", aliases=aliases)

        self.add_vertexes_to_graph(current_graph)

        return current_graph

    def add_vertexes_to_graph(self, current_graph: MappedGraph[str]) -> MappedGraph[str]:
        first_parent = current_graph.get_or_set_vertex("first parent")
        first_child = current_graph.get_or_set_vertex("first child")
        second_parent = current_graph.get_or_set_vertex("second parent")
        second_child = current_graph.get_or_set_vertex("second child")

        first_parent.add_neighbor(first_child, 1.0)
        first_parent.add_neighbor(second_child, 2.0)
        second_parent.add_neighbor(first_child, 1.0)
        second_parent.add_neighbor(second_child, 2.0)

        return current_graph

    def test_basic_graph(self, basic_graph: MappedGraph[str]) -> None:
        first_parent_in_graph = basic_graph.get_vertex("first parent")
        first_child_in_graph = basic_graph.get_vertex("first child")

        assert first_parent_in_graph
        assert first_child_in_graph
        assert first_parent_in_graph in list(basic_graph.vertexes)
        assert first_child_in_graph in list(basic_graph.vertexes)
        assert first_parent_in_graph.name == "first parent"
        assert first_child_in_graph.name == "first child"
        assert first_parent_in_graph.has_neighbor(first_child_in_graph)
        assert not basic_graph.is_optimized(first_parent_in_graph.name)
        assert not basic_graph.is_optimized(first_child_in_graph.name)

    def test_basic_graph_with_aliases(self, basic_graph_with_aliases: MappedGraph[str]) -> None:
        first_parent_in_graph = basic_graph_with_aliases.get_vertex("first parent")
        first_child_in_graph = basic_graph_with_aliases.get_vertex("first child")
        first_parent_alias_in_graph = basic_graph_with_aliases.get_vertex("first parent alias")
        override_in_graph = basic_graph_with_aliases.get_vertex("XBT")
        micro_first_parent_in_graph = basic_graph_with_aliases.get_vertex("micro first parent")
        macro_first_parent_in_graph = basic_graph_with_aliases.get_vertex("macro first parent")

        assert first_parent_in_graph
        assert first_child_in_graph
        assert first_parent_alias_in_graph
        assert micro_first_parent_in_graph
        assert override_in_graph
        assert not macro_first_parent_in_graph
        assert first_parent_alias_in_graph in list(basic_graph_with_aliases.vertexes)
        assert micro_first_parent_in_graph in list(basic_graph_with_aliases.vertexes)
        assert first_parent_alias_in_graph.name == "first parent alias"
        assert micro_first_parent_in_graph.name == "micro first parent"
        assert first_parent_alias_in_graph.has_neighbor(first_parent_in_graph)
        assert micro_first_parent_in_graph.has_neighbor(first_parent_in_graph)
        assert basic_graph_with_aliases.is_alias(first_parent_alias_in_graph.name, first_parent_in_graph.name)
        assert basic_graph_with_aliases.is_alias(micro_first_parent_in_graph.name, first_parent_in_graph.name)
        assert not basic_graph_with_aliases.is_alias(first_parent_in_graph.name, first_parent_in_graph.name)
        alias_bar: Optional[HistoricalBar] = basic_graph_with_aliases.get_alias_bar(
            first_parent_alias_in_graph.name, first_parent_in_graph.name, datetime.now()
        )
        assert alias_bar
        assert alias_bar.high == RP2Decimal("1")

        micro_bar: Optional[HistoricalBar] = basic_graph_with_aliases.get_alias_bar(
            micro_first_parent_in_graph.name, first_parent_in_graph.name, datetime.now()
        )
        assert micro_bar
        assert micro_bar.high == RP2Decimal("0.0001")

        btc_bar: Optional[HistoricalBar] = basic_graph_with_aliases.get_alias_bar(override_in_graph.name, "BTC", datetime.now())
        assert btc_bar
        assert btc_bar.high == RP2Decimal("2")

        assert basic_graph_with_aliases.get_alias_bar(first_parent_in_graph.name, first_parent_in_graph.name, datetime.now()) is None

        pricing_path: Optional[Iterator[Vertex[str]]] = basic_graph_with_aliases.dijkstra(micro_first_parent_in_graph, first_child_in_graph, False)
        assert pricing_path
        pricing_path_list: List[str] = [v.name for v in pricing_path]
        assert pricing_path_list == [micro_first_parent_in_graph.name, first_parent_in_graph.name, first_child_in_graph.name]

    def test_cloned_graph(self, basic_graph: MappedGraph[str], cloned_graph: MappedGraph[str]) -> None:
        first_parent_in_graph = basic_graph.get_vertex("first parent")
        first_child_in_graph = basic_graph.get_vertex("first child")
        first_parent_in_clone = cloned_graph.get_vertex("first parent")
        first_child_in_clone = cloned_graph.get_vertex("first child")
        second_parent_in_clone = cloned_graph.get_vertex("second parent")
        second_child_in_clone = cloned_graph.get_vertex("second child")

        assert first_parent_in_clone is not first_parent_in_graph
        assert first_child_in_clone is not first_child_in_graph
        assert cloned_graph.optimized_assets is not basic_graph.optimized_assets
        assert first_parent_in_clone
        assert first_child_in_clone
        assert second_parent_in_clone
        assert second_child_in_clone
        assert first_parent_in_clone.get_weight(first_child_in_clone) == 2.0
        assert first_parent_in_clone.get_weight(second_child_in_clone) == 1.0
        assert second_parent_in_clone.get_weight(first_child_in_clone) == 4.0
        assert second_parent_in_clone.get_weight(second_child_in_clone) == 3.0
