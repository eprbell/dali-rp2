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

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import pytest
from prezzemolo.vertex import Vertex
from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2ValueError

from dali.abstract_ccxt_pair_converter_plugin import (
    _BINANCE,
    _COINBASE_PRO,
    _ONE_HOUR,
    _SIX_HOUR,
    _TIME_GRANULARITY,
    _TIME_GRANULARITY_DICT,
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

MARKET_START: str = "market_start"
ONE_WEEK_EARLIER: str = "one_week_earlier"


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

    @pytest.fixture
    def historical_bars(self) -> Dict[str, HistoricalBar]:
        now_time = datetime.now(timezone.utc)
        return {
            MARKET_START: HistoricalBar(
                duration=timedelta(weeks=1),
                timestamp=now_time,
                open=RP2Decimal("1.0"),
                high=RP2Decimal("2.0"),
                low=RP2Decimal("0.5"),
                close=RP2Decimal("1.5"),
                volume=RP2Decimal("100.0"),
            ),
            ONE_WEEK_EARLIER: HistoricalBar(
                duration=timedelta(weeks=1),
                timestamp=now_time - timedelta(weeks=1),
                open=RP2Decimal("1.1"),
                high=RP2Decimal("2.1"),
                low=RP2Decimal("0.6"),
                close=RP2Decimal("1.6"),
                volume=RP2Decimal("110.0"),
            ),
        }

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

    def test_retrieve_historical_bars(
        self, mocker: Any, unoptimized_graph: MappedGraph[str], vertex_list: Dict[str, Vertex[str]], historical_bars: Dict[str, HistoricalBar]
    ) -> None:
        plugin = MockAbstractCcxtPairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        unoptimized_assets: Set[str] = {"A", "B"}
        optimization_candidates: Set[Vertex[str]] = {vertex_list["A"], vertex_list["B"], vertex_list["C"]}
        week_start_date = historical_bars[MARKET_START].timestamp

        mocker.patch.object(plugin, "_AbstractCcxtPairConverterPlugin__exchange_markets", {TEST_EXCHANGE: TEST_MARKETS})

        def find_historical_bars_side_effect(
            from_asset: str, to_asset: str, timestamp: datetime, exchange: str, all_bars: bool, timespan: str  # pylint: disable=unused-argument
        ) -> Optional[List[HistoricalBar]]:
            if from_asset == "A":
                return [historical_bars[MARKET_START]]
            if from_asset == "B":
                return [historical_bars[ONE_WEEK_EARLIER]]
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

    def test_generate_optimizations(self, historical_bars: Dict[str, HistoricalBar]) -> None:
        plugin = MockAbstractCcxtPairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        week_start_date = historical_bars[MARKET_START].timestamp

        child_bars = {"A": {"B": [historical_bars[MARKET_START], historical_bars[ONE_WEEK_EARLIER]]}}

        market_starts = {"A": {"B": week_start_date}}

        optimizations = plugin._generate_optimizations(child_bars, market_starts, week_start_date)  # pylint: disable=protected-access

        assert week_start_date in optimizations
        assert "A" in optimizations[week_start_date]
        assert "B" in optimizations[week_start_date]["A"]
        assert optimizations[week_start_date]["A"]["B"] == 100.00
        # We want to delete the market if the bar is before market_start
        # This will cause an error if we try to price an asset that is untradeable (doesn't have a market) at the time it is being priced for
        # The user can then mark it as untradeable in the config file
        assert optimizations[week_start_date - timedelta(weeks=1)]["A"]["B"] == -1.0

    def test_refine_and_finalize_optimizations(self) -> None:
        plugin = MockAbstractCcxtPairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        optimizations = {
            datetime(2023, 1, 1): {"A": {"B": 100.0, "C": -1.0}, "D": {"E": 50.0}},
            datetime(2023, 1, 2): {"A": {"B": 200.0, "C": 150.0}, "D": {"E": 50.0}},
            datetime(2023, 1, 3): {"A": {"B": 200.0, "C": 150.0}, "D": {"E": 50.0}},
            datetime(2023, 1, 4): {"A": {"B": 150.0, "C": 200.0}, "D": {"F": 50.0}},
        }

        refined_optimizations = plugin._refine_and_finalize_optimizations(optimizations)  # pylint: disable=protected-access

        assert datetime(2023, 1, 1) in refined_optimizations
        assert datetime(2023, 1, 2) in refined_optimizations
        assert datetime(2023, 1, 3) not in refined_optimizations  # Duplicate snapshot should be removed
        assert datetime(2023, 1, 4) in refined_optimizations

        assert refined_optimizations[datetime(2023, 1, 1)]["A"]["B"] == 1.0
        assert refined_optimizations[datetime(2023, 1, 1)]["A"]["C"] == -1.0
        assert refined_optimizations[datetime(2023, 1, 1)]["D"]["E"] == 1.0

        assert refined_optimizations[datetime(2023, 1, 2)]["A"]["B"] == 1.0
        assert refined_optimizations[datetime(2023, 1, 2)]["A"]["C"] == 2.0
        assert refined_optimizations[datetime(2023, 1, 2)]["D"]["E"] == 1.0

        assert refined_optimizations[datetime(2023, 1, 4)]["A"]["B"] == 2.0
        assert refined_optimizations[datetime(2023, 1, 4)]["A"]["C"] == 1.0
        assert refined_optimizations[datetime(2023, 1, 4)]["D"]["F"] == 1.0
        assert "E" not in refined_optimizations[datetime(2023, 1, 4)]["D"]

    def test_initialize_retry_count(self) -> None:
        plugin = MockAbstractCcxtPairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)

        assert plugin._initialize_retry_count(_BINANCE, _ONE_HOUR) == _TIME_GRANULARITY.index(_ONE_HOUR)  # pylint: disable=protected-access
        assert plugin._initialize_retry_count(_COINBASE_PRO, _SIX_HOUR) == _TIME_GRANULARITY_DICT[_COINBASE_PRO].index(  # pylint: disable=protected-access
            _SIX_HOUR
        )
        with pytest.raises(RP2ValueError):
            # Binance does not support 6 hour granularity
            assert plugin._initialize_retry_count(_BINANCE, _SIX_HOUR)  # pylint: disable=protected-access
            assert plugin._initialize_retry_count(_COINBASE_PRO, "invalid")  # pylint: disable=protected-access

    def test_find_historical_bars_guard_clause(self, mocker: Any, historical_bars: Dict[str, HistoricalBar]) -> None:
        plugin = MockAbstractCcxtPairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)

        mocker.patch.object(plugin, "_get_bundle_from_cache", return_value=[historical_bars[MARKET_START]])

        bars = plugin.find_historical_bars("A", "B", datetime(2023, 1, 1), TEST_EXCHANGE, True)

        assert bars
        assert len(bars) == 1
        assert bars[0] == historical_bars[MARKET_START]

    # To be enabled when _fetch_historical_bars is implemented
    def disabled_test_find_historical_bars_add_to_cache(self, mocker: Any, historical_bars: Dict[str, HistoricalBar]) -> None:
        plugin = MockAbstractCcxtPairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)

        mocker.patch.object(plugin, "_get_bundle_from_cache", return_value=historical_bars[ONE_WEEK_EARLIER])
        mocker.patch.object(plugin, "_fetch_historical_bars", return_value=[historical_bars[MARKET_START]])  # function that calls the API

        bars = plugin.find_historical_bars("A", "B", datetime(2023, 1, 1), TEST_EXCHANGE, True)

        assert bars
        assert len(bars) == 2
        assert bars[0] == historical_bars[ONE_WEEK_EARLIER]
        assert bars[1] == historical_bars[MARKET_START]
