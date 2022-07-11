# Copyright 2022 macanudo527
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

from typing import Dict, List, Optional


class BFS:
    @staticmethod
    def bfs_cyclic(graph: Dict[str, List[str]], start: str, end: str) -> Optional[List[str]]:

        # maintain a queue of paths
        queue: List[List[str]] = []
        visited: List[str] = []

        # push the first path into the queue
        queue.append([start])

        while queue:
            # get the first path from the queue
            path: List[str] = queue.pop(0)

            # get the last node from the path
            node: str = path[-1]

            # path found
            if node == end:
                return path

            # enumerate all adjacent nodes, construct a new path and push it into the queue
            for adjacent in graph.get(node, []):

                # prevents an infinite loop.
                if adjacent not in visited:
                    new_path: List[str] = list(path)
                    new_path.append(adjacent)
                    queue.append(new_path)
                    visited.append(adjacent)

        # No path found
        return None
