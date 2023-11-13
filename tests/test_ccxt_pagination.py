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

# pylint: disable=protected-access

from typing import Dict, List, Union

import pytest

from dali.ccxt_pagination import (
    IdBasedPaginationDetailSet,
    IdBasedPaginationDetailsIterator,
)


class TestCCXTPagination:
    # Define a fixture to create an instance of the iterator with sample data
    @pytest.fixture
    def sample_iterator(self) -> IdBasedPaginationDetailsIterator:
        # Create an instance of IdBasedPaginationDetailsIterator with sample data
        return iter(IdBasedPaginationDetailSet(id_param="id", limit=4, markets=["market1", "market2"]))

    @pytest.fixture
    def sample_results(self) -> List[Dict[str, Union[str, int]]]:
        data: List[Dict[str, Union[str, int]]] = [
            {"id": 1, "value": "Data1"},
            {"id": 2, "value": "Data2"},
            {"id": 3, "value": "Data3"},
        ]
        return data

    # Test case for checking if the iterator initializes correctly
    def test_iterator_initialization(self, sample_iterator: IdBasedPaginationDetailsIterator) -> None:
        assert sample_iterator._get_market() == "market1"
        assert sample_iterator._get_limit() == 4
        assert sample_iterator._get_params() == {"id": None}
        assert not sample_iterator._is_end_of_data()

    # Test case for iterating through the iterator
    def test_iterator_iteration(self, sample_iterator: IdBasedPaginationDetailsIterator, sample_results: Dict[str, Union[str, int]]) -> None:
        results = []
        try:
            while True:
                results.append(next(sample_iterator))
                sample_iterator.update_fetched_elements(sample_results)
        except StopIteration:
            # End of pagination details
            pass
        assert len(results) == 2  # Two markets
        assert sample_iterator._is_end_of_data()

    # Test case for raising StopIteration
    def test_stop_iteration(self, sample_iterator: IdBasedPaginationDetailsIterator) -> None:
        # Force the iterator to reach the end of data
        sample_iterator._next_market()
        sample_iterator._next_market()
        with pytest.raises(StopIteration):
            next(sample_iterator)
