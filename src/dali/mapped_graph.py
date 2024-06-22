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

from datetime import datetime, timedelta
from typing import Dict, Iterator, List, NamedTuple, Optional, Set

from prezzemolo.graph import Graph
from prezzemolo.utility import ValueType
from prezzemolo.vertex import Vertex
from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2TypeError

from dali.historical_bar import HistoricalBar
from dali.logger import LOGGER


class Alias(NamedTuple):
    from_asset: str
    to_asset: str


# Hard-coded base aliases
_UNIVERSAL_ALIASES = {
    Alias(from_asset="LUNA", to_asset="LUNC"): RP2Decimal("1"),
    Alias(from_asset="XBT", to_asset="BTC"): RP2Decimal("1"),
}

_EXCHANGE_SPECIFIC_ALIASES = {
    "Coinbase": {
        Alias(from_asset="ETH2", to_asset="ETH"): RP2Decimal("1"),
    },
    "Coinbase Pro": {
        Alias(from_asset="ETH2", to_asset="ETH"): RP2Decimal("1"),
    },
    "Pionex": {
        Alias(from_asset="MBTC", to_asset="BTC"): RP2Decimal("0.001"),
        Alias(from_asset="METH", to_asset="ETH"): RP2Decimal("0.001"),
    },
}

_UNIVERSAL: str = "UNIVERSAL"


class MappedGraph(Graph[ValueType]):
    def __init__(
        self,
        exchange: str,  # This is temporary until teleportation is implemented
        vertexes: Optional[List["Vertex[ValueType]"]] = None,
        optimized_assets: Optional[Set[str]] = None,
        fiat_assets: Optional[Set[str]] = None,
        aliases: Optional[Dict[str, Dict[Alias, RP2Decimal]]] = None,
    ) -> None:
        super().__init__(vertexes)
        self.__exchange: str = exchange  # Temporary until teleportation
        self.__name_to_vertex: Dict[str, Vertex[ValueType]] = {vertex.name: vertex for vertex in vertexes} if vertexes else {}
        self.__optimized_assets: Set[str] = set() if optimized_assets is None else optimized_assets
        self.__fiat_assets: Set[str] = set() if fiat_assets is None else fiat_assets
        self.__aliases: Dict[Alias, RP2Decimal] = {}
        self.__aliases.update(_UNIVERSAL_ALIASES)
        self.__aliases.update(_EXCHANGE_SPECIFIC_ALIASES.get(exchange, {}))  # To be removed for teleportation
        # TO BE IMPLEMENTED - exchange specific aliases when teleportation is implemented
        # self.__exchange_aliases: Dict[str, Dict[Alias, RP2Decimal]]

        if aliases:
            self.__aliases.update(aliases.get(_UNIVERSAL, {}))
            self.__aliases.update(aliases.get(exchange, {}))
        self.__add_aliases(self.__aliases)

    @property
    def aliases(self) -> Iterator[Alias]:
        return iter(self.__aliases.keys())

    def __str__(self) -> str:
        return (
            f"MappedGraph("
            f"exchange={self.__exchange}, "
            f"vertexes={self.__name_to_vertex}, "
            f"optimized_assets_count={len(self.__optimized_assets)}, "
            f"fiat_assets_count={len(self.__fiat_assets)}, "
            f"alias_count={len(self.__aliases)}"
            f")"
        )

    def add_vertex_if_missing(self, name: str) -> None:
        if not self.__name_to_vertex.get(name):
            self.add_vertex(Vertex[ValueType](name=name))

    def get_all_children_of_vertex(self, vertex: Vertex[ValueType], visited: Optional[Set[Vertex[ValueType]]] = None) -> Set[Vertex[ValueType]]:
        # We need to keep track of the visited vertexes to prevent infinite recursion
        visited = set() if visited is None else visited
        children = set(vertex.neighbors)
        visited.add(vertex)
        for neighbor in vertex.neighbors:
            if neighbor not in visited:
                children.update(self.get_all_children_of_vertex(neighbor, visited))
        return children

    def add_vertex(self, vertex: Vertex[ValueType]) -> None:
        super().add_vertex(vertex)
        self.__name_to_vertex[vertex.name] = vertex

    def get_alias_bar(self, from_asset: str, to_asset: str, timestamp: datetime) -> Optional[HistoricalBar]:
        alias_pair = Alias(from_asset, to_asset)
        factor: Optional[RP2Decimal] = self.__aliases.get(alias_pair)

        if factor:
            return HistoricalBar(
                duration=timedelta(seconds=60),
                timestamp=timestamp,
                open=factor,
                high=factor,
                low=factor,
                close=factor,
                volume=RP2Decimal("1.0"),
            )
        return None

    def get_vertex(self, name: str) -> Optional[Vertex[ValueType]]:
        if not isinstance(name, str):
            raise RP2TypeError(f"Internal Error: parameter {name} is not a str.")
        return self.__name_to_vertex.get(name)

    def get_or_set_vertex(self, name: str) -> Vertex[ValueType]:
        if not isinstance(name, str):
            raise RP2TypeError(f"Internal Error: parameter {name} is not a str.")
        existing_vertex: Optional[Vertex[ValueType]] = self.get_vertex(name)
        if existing_vertex:
            return existing_vertex

        new_vertex: Vertex[ValueType] = Vertex[ValueType](name=name)
        self.add_vertex(new_vertex)
        return new_vertex

    def is_alias(self, from_asset: str, to_asset: str) -> bool:
        current_alias: Alias = Alias(from_asset, to_asset)
        return current_alias in self.__aliases

    def is_optimized(self, asset: str) -> bool:
        LOGGER.debug("Checking if %s is in %s", asset, self.__optimized_assets)
        return bool(asset in self.__optimized_assets)

    @property
    def optimized_assets(self) -> Set[str]:
        return self.__optimized_assets

    # Optimization contains a dict with a key of the optimized asset and a value of a dict with the optimized weights for each neighbor
    # Optimized assets are tracked to prevent requesting prices for unoptimized assets
    # Negative weights will get deleted.
    def clone_with_optimization(self, optimization: Dict[str, Dict[str, float]]) -> "MappedGraph[ValueType]":
        # exchange is used here again temporarily
        cloned_mapped_graph: MappedGraph[ValueType] = MappedGraph(
            self.__exchange, optimized_assets=self.__optimized_assets.copy(), fiat_assets=self.__fiat_assets.copy(), aliases={_UNIVERSAL: self.__aliases}
        )

        for original_vertex in self.vertexes:
            if len(list(original_vertex.neighbors)) == 0 and original_vertex.name not in self.__fiat_assets:
                cloned_mapped_graph.add_vertex_if_missing(original_vertex.name)
                continue
            # Add existing neighbors
            for neighbor in original_vertex.neighbors:
                neighbor_weight: float
                optimized: bool = False
                if original_vertex.name in set(optimization.keys()):
                    neighbor_weight = optimization[original_vertex.name].pop(neighbor.name, original_vertex.get_weight(neighbor))
                    optimized = True
                else:
                    neighbor_weight = original_vertex.get_weight(neighbor)

                # Delete neighbor if negative weight
                if neighbor_weight >= 0.0:
                    cloned_mapped_graph.add_neighbor(original_vertex.name, neighbor.name, neighbor_weight, optimized)
                elif neighbor.name in self.__fiat_assets:
                    LOGGER.debug("Adding fiat neighbor while cloning %s to %s", original_vertex.name, neighbor.name)
                    cloned_mapped_graph.add_fiat_neighbor(original_vertex.name, neighbor.name, neighbor_weight, optimized)
                else:
                    cloned_mapped_graph.add_vertex_if_missing(original_vertex.name)

        # Add new neighbors
        for optimized_asset, neighbor_weights in optimization.items():
            for neighbor_name in neighbor_weights.keys():
                if self.get_vertex(optimized_asset) in list(self.vertexes):
                    cloned_mapped_graph.add_neighbor(optimized_asset, neighbor_name, neighbor_weights[neighbor_name], True)
                    LOGGER.debug("Added while cloning %s to %s", optimized_asset, neighbor_name)

        return cloned_mapped_graph

    # More and more markets are added over time, so we need to prune the first graph down to what is available during the first trade
    def prune_graph(self, optimization: Dict[str, Dict[str, float]]) -> "MappedGraph[ValueType]":
        pruned_mapped_graph: MappedGraph[ValueType] = MappedGraph(
            self.__exchange, optimized_assets=self.__optimized_assets.copy(), aliases={_UNIVERSAL: self.__aliases}
        )
        LOGGER.debug("Pruning with these optimizations - %s", optimization)
        added_assets: Set[str] = set()
        pruned_assets: Set[str] = set()
        for vertex in self.vertexes:
            for neighbor in vertex.neighbors:
                if neighbor.name in optimization.get(vertex.name, {}) or (vertex.name in self.__fiat_assets and neighbor.name in self.__fiat_assets):
                    pruned_mapped_graph.add_neighbor(vertex.name, neighbor.name, 0.0, False)
                    added_assets.add(f"#{vertex.name}:#{neighbor.name}")
                else:
                    pruned_mapped_graph.add_vertex_if_missing(vertex.name)
                    pruned_assets.add(f"#{vertex.name}:#{neighbor.name}")

        LOGGER.debug("Added assets: %s", added_assets)
        LOGGER.debug("Pruned assets: %s", pruned_assets)

        return pruned_mapped_graph

    # Marking weights as optimized prevents REST API calls to re-optimize them
    def add_neighbor(self, vertex_name: str, neighbor_name: str, weight: float = 0.0, optimized: bool = False) -> None:
        vertex: Vertex[ValueType] = self.get_or_set_vertex(vertex_name)
        neighbor: Vertex[ValueType] = self.get_or_set_vertex(neighbor_name)
        if not vertex.has_neighbor(neighbor):
            vertex.add_neighbor(neighbor, weight)
        if optimized:
            self.__optimized_assets.add(vertex_name)

    def add_fiat_neighbor(self, vertex_name: str, neighbor_name: str, weight: float = 0.0, optimized: bool = False) -> None:
        LOGGER.debug("Added fiat neighbor %s to %s", vertex_name, neighbor_name)
        self.__fiat_assets.add(vertex_name)
        self.add_neighbor(vertex_name, neighbor_name, weight, optimized)

    # No adding aliases after cloning, because that would make MappedGraph mutable
    def __add_aliases(self, aliases: Dict[Alias, RP2Decimal]) -> None:
        for market in aliases.keys():
            # Aliases have a zero weight since they are virtually the same asset
            # They are automatically optimized
            self.add_neighbor(market.from_asset, market.to_asset, 0.0, True)
