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

from typing import DefaultDict

from prezzemolo.graph import Graph
from prezzemolo.vertex import Vertex

from dali.abstract_pair_converter_plugin import GraphVertexesDict


class TestAbstractPairConverterPlugin:
    def test_graph_vertexes_class(self) -> None:
        current_graph: Graph[str] = Graph[str]()
        graph_vertexes: DefaultDict[str, Vertex[str]] = GraphVertexesDict(current_graph)

        first_vertex = graph_vertexes["first vertex"]
        second_vertex = graph_vertexes["second vertex"]

        first_vertex.add_neighbor(second_vertex, 1.0)

        assert first_vertex in list(current_graph.vertexes)
        assert second_vertex in list(current_graph.vertexes)
        assert first_vertex.name == "first vertex"
        assert second_vertex.name == "second vertex"
        assert graph_vertexes["second vertex"] in graph_vertexes["first vertex"].neighbors
