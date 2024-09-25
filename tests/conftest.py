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

from datetime import datetime, timezone
from typing import Dict, List, Union

import pytest
from prezzemolo.avl_tree import AVLTree
from prezzemolo.vertex import Vertex

from dali.mapped_graph import MappedGraph

TEST_EXCHANGE: str = "Kraken"


@pytest.fixture(scope="session")
def test_exchange() -> str:
    return TEST_EXCHANGE


@pytest.fixture(scope="session", name="vertex_list")
def vertex_list_fixture() -> List[Vertex[str]]:
    beth: Vertex[str] = Vertex[str](name="BETH")
    btc: Vertex[str] = Vertex[str](name="BTC")
    eth: Vertex[str] = Vertex[str](name="ETH")
    gbp: Vertex[str] = Vertex[str](name="GBP")
    jpy: Vertex[str] = Vertex[str](name="JPY")
    usdc: Vertex[str] = Vertex[str](name="USDC")
    usdt: Vertex[str] = Vertex[str](name="USDT")
    usd: Vertex[str] = Vertex[str](name="USD")

    beth.add_neighbor(eth, 1.0)
    btc.add_neighbor(usdc, 2.0)  # Has higher volume, but we don't want to disrupt other tests
    btc.add_neighbor(usdt, 1.0)
    btc.add_neighbor(gbp, 2.0)
    eth.add_neighbor(usdt, 1.0)
    usdc.add_neighbor(usd, 2.0)
    usdt.add_neighbor(usd, 1.0)
    usd.add_neighbor(jpy, 50.0)

    return [beth, btc, eth, gbp, jpy, usdc, usdt, usd]


@pytest.fixture(scope="session", name="graph_optimized")
def graph_optimized_fixture(vertex_list: List[Vertex[str]]) -> MappedGraph[str]:
    return MappedGraph[str](TEST_EXCHANGE, vertex_list, {"BETH", "BTC", "ETH", "GBP", "JPY", "USDC", "USDT", "USD"})


@pytest.fixture(scope="session")
def simple_tree(graph_optimized: MappedGraph[str]) -> AVLTree[datetime, MappedGraph[str]]:
    tree: AVLTree[datetime, MappedGraph[str]] = AVLTree()

    # The original unoptimized graph is placed at the earliest possible time
    tree.insert_node(datetime.fromtimestamp(1504541580, timezone.utc), graph_optimized)

    return tree


# This section configures pytest-recording, which uses vcrpy under the hood.
# Documentation: https://github.com/kiwicom/pytest-recording
@pytest.fixture(scope="session")
def vcr_config() -> Dict[str, Union[bool, List[str], str]]:
    return {
        "filter_headers": ["authorization"],
        "filter_query_parameters": ["access_key"],
        "ignore_localhost": True,
        "record_mode": "once",
    }
