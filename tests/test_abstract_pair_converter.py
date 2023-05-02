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

from typing import Optional

import pytest
from prezzemolo.vertex import Vertex
from rp2.rp2_error import RP2ValueError

from dali.abstract_pair_converter_plugin import MappedGraph


class TestAbstractPairConverterPlugin:
    @pytest.fixture
    def cloneable_graph(self) -> MappedGraph[str]:
        current_graph: MappedGraph[str] = MappedGraph[str]()

        current_graph.add_missing_vertex("BTC")
        current_graph.add_missing_vertex("ETH")
        current_graph.add_neighbor("ETH", "USD", 1.0)
        current_graph.add_neighbor("ETH", "USDT", 2.0)
        current_graph.add_neighbor("BTC", "USD", 1.0)
        current_graph.add_neighbor("BTC", "USDT", 2.0)

        return current_graph

    def test_mapped_graph_class(self) -> None:
        current_graph: MappedGraph[str] = MappedGraph[str]()

        first_vertex = current_graph.get_or_set_vertex("first vertex")
        second_vertex = current_graph.get_or_set_vertex("second vertex")

        first_vertex.add_neighbor(second_vertex, 1.0)

        first_vertex_in_graph: Optional[Vertex[str]] = current_graph.get_vertex("first vertex")
        second_vertex_in_graph: Optional[Vertex[str]] = current_graph.get_vertex("second vertex")

        assert first_vertex in list(current_graph.vertexes)
        assert second_vertex in list(current_graph.vertexes)
        assert first_vertex.name == "first vertex"
        assert second_vertex.name == "second vertex"
        assert first_vertex_in_graph
        assert second_vertex_in_graph
        assert first_vertex_in_graph.has_neighbor(second_vertex_in_graph)

    def test_mapped_graph_cloning_changing_weights(self, cloneable_graph: MappedGraph[str]) -> None:
        cloned_graph = cloneable_graph.clone_with_optimization("ETH", {"USD": 3.0})

        assert cloned_graph.is_optimized("ETH")
        assert not cloned_graph.is_optimized("BTC")
        assert cloned_graph.get_vertex("ETH").get_weight(cloned_graph.get_vertex("USD")) == 3.0  # type: ignore
        assert cloned_graph.get_vertex("ETH").get_weight(cloned_graph.get_vertex("USDT")) == 2.0  # type: ignore
        assert cloned_graph.get_vertex("BTC").get_weight(cloned_graph.get_vertex("USD")) == 1.0  # type: ignore

    def test_mapped_graph_clone_has_new_vertexes(self, cloneable_graph: MappedGraph[str]) -> None:
        cloned_graph = cloneable_graph.clone_with_optimization("", {})
        cloned_vertex = cloned_graph.get_vertex("ETH")

        for vertex in cloneable_graph.vertexes:
            assert not cloned_vertex is vertex

    def test_mapped_graph_cloning_deleting_neighbor(self, cloneable_graph: MappedGraph[str]) -> None:
        cloned_graph = cloneable_graph.clone_with_optimization("ETH", {"USD": -1.0, "USDT": 1.0})

        assert cloned_graph.is_optimized("ETH")
        assert not cloned_graph.is_optimized("BTC")
        assert not cloned_graph.get_vertex("ETH").has_neighbor(cloned_graph.get_vertex("USD"))  # type: ignore
        assert cloned_graph.get_vertex("BTC").has_neighbor(cloned_graph.get_vertex("USD"))  # type: ignore

    def test_mapped_graph_cloning_adding_neighbor(self, cloneable_graph: MappedGraph[str]) -> None:
        cloned_graph = cloneable_graph.clone_with_optimization("ETH", {"USDC": 3.0})

        assert cloned_graph.is_optimized("ETH")
        assert not cloned_graph.is_optimized("BTC")
        assert cloned_graph.get_vertex("ETH").has_neighbor(cloned_graph.get_vertex("USDC"))  # type: ignore
        assert not cloned_graph.get_vertex("BTC").has_neighbor(cloned_graph.get_vertex("USDC"))  # type: ignore

    def test_mapped_graph_throws_exception(self, cloneable_graph: MappedGraph[str]) -> None:
        cloned_graph = cloneable_graph.clone_with_optimization("ETH", {"USD": 3.0})
        with pytest.raises(RP2ValueError):
            cloned_graph.clone_with_optimization("ETH", {"USD": 3.0})

        cloned_graph2 = cloned_graph.clone_with_optimization("BTC", {"USD": 3.0})

        assert cloned_graph2.get_vertex("BTC").get_weight(cloned_graph2.get_vertex("USD")) == 3.0  # type: ignore
