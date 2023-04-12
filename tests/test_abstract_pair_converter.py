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

from prezzemolo.vertex import Vertex

from dali.abstract_pair_converter_plugin import MappedGraph


class TestAbstractPairConverterPlugin:
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
