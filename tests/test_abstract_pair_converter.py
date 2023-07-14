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

from dali.mapped_graph import MappedGraph


class TestAbstractPairConverterPlugin:
    def test_mapped_graph_class(self) -> None:
        current_graph = MappedGraph[str]()

        first_parent = current_graph.get_or_set_vertex("first parent")
        first_child = current_graph.get_or_set_vertex("first child")
        second_parent = current_graph.get_or_set_vertex("second parent")
        second_child = current_graph.get_or_set_vertex("second child")

        first_parent.add_neighbor(first_child, 1.0)
        first_parent.add_neighbor(second_child, 2.0)
        second_parent.add_neighbor(first_child, 1.0)
        second_parent.add_neighbor(second_child, 2.0)

        first_parent_in_graph = current_graph.get_vertex("first parent")
        first_child_in_graph = current_graph.get_vertex("first child")

        assert first_parent in list(current_graph.vertexes)
        assert first_child in list(current_graph.vertexes)
        assert first_parent.name == "first parent"
        assert first_child.name == "first child"
        assert first_parent_in_graph
        assert first_child_in_graph
        assert first_parent_in_graph.has_neighbor(first_child_in_graph)
        assert not current_graph.is_optimized(first_parent_in_graph.name)
        assert not current_graph.is_optimized(first_child_in_graph.name)

        optimizations = {
            "first parent": {"first child": 2.0, "second child": 1.0},
            "second parent": {"first child": 4.0, "second child": 3.0},
        }

        cloned_graph = current_graph.clone_with_optimization(optimizations)

        first_parent_in_clone = cloned_graph.get_vertex("first parent")
        first_child_in_clone = cloned_graph.get_vertex("first child")
        second_parent_in_clone = cloned_graph.get_vertex("second parent")
        second_child_in_clone = cloned_graph.get_vertex("second child")

        assert first_parent_in_clone is not first_parent_in_graph
        assert first_child_in_clone is not first_child_in_graph
        assert cloned_graph.optimized_assets is not current_graph.optimized_assets
        assert first_parent_in_clone
        assert first_child_in_clone
        assert second_parent_in_clone
        assert second_child_in_clone
        assert first_parent_in_clone.get_weight(first_child_in_clone) == 2.0
        assert first_parent_in_clone.get_weight(second_child_in_clone) == 1.0
        assert second_parent_in_clone.get_weight(first_child_in_clone) == 4.0
        assert second_parent_in_clone.get_weight(second_child_in_clone) == 3.0
