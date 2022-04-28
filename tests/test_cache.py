# Copyright 2022 eprbell
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


import os
import unittest
from pathlib import Path
from typing import Dict, List

from dali.abstract_transaction import AbstractTransaction
from dali.cache import CACHE_DIR, load_from_cache, save_to_cache
from dali.in_transaction import InTransaction
from dali.out_transaction import OutTransaction

ROOT_PATH: Path = Path(os.path.dirname(__file__)).parent.absolute()
OUTPUT_PATH: Path = ROOT_PATH / Path("output")


class TestCache(unittest.TestCase):
    def setUp(self) -> None:  # pylint: disable=invalid-name
        self.maxDiff = None  # pylint: disable=invalid-name

    def test_list_cache(self) -> None:
        cache_name: str = "test_list_cache"
        try:
            (ROOT_PATH / CACHE_DIR / cache_name).unlink()
        except FileNotFoundError:
            pass
        transaction_list: List[AbstractTransaction] = [
            InTransaction(
                plugin="my plugin 1",
                unique_id="my unique_id 1",
                raw_data="my raw_data 1",
                timestamp="2021-01-02T08:42:43.882Z",
                asset="BTC",
                exchange="BlockFi",
                holder="Bob",
                transaction_type="inTerest",
                spot_price="1000.0",
                crypto_in="2.0002",
                fiat_fee="0",
                fiat_in_no_fee="2000.2",
                fiat_in_with_fee="2000.2",
            ),
            OutTransaction(
                plugin="my plugin 2",
                unique_id="my unique_id 2",
                raw_data="my raw_data 2",
                timestamp="6/1/2020 3:59:59 -04:00",
                asset="ETH",
                exchange="Coinbase Pro",
                holder="Bob",
                transaction_type="SELL",
                spot_price="900.9",
                crypto_out_no_fee="2.2",
                crypto_fee="0.01",
                notes="my notes 2",
            ),
        ]
        save_to_cache(cache_name, transaction_list)
        loaded_transaction_list: List[AbstractTransaction] = load_from_cache(cache_name)
        self.assertEqual(len(transaction_list), len(loaded_transaction_list))
        for transaction1, transaction2 in zip(transaction_list, loaded_transaction_list):
            self.assertEqual(transaction1, transaction2)
            self.assertEqual(transaction1.__class__, transaction2.__class__)
            self.assertEqual(transaction1.timestamp, transaction2.timestamp)
            self.assertEqual(transaction1.raw_data, transaction2.raw_data)
            if isinstance(transaction1, InTransaction) and isinstance(transaction2, InTransaction):
                self.assertEqual(transaction1.spot_price, transaction2.spot_price)
                self.assertEqual(transaction1.crypto_in, transaction2.crypto_in)
                self.assertEqual(transaction1.crypto_fee, transaction2.crypto_fee)
                self.assertEqual(transaction1.fiat_fee, transaction2.fiat_fee)
            elif isinstance(transaction1, OutTransaction) and isinstance(transaction2, OutTransaction):
                self.assertEqual(transaction1.spot_price, transaction2.spot_price)
                self.assertEqual(transaction1.crypto_out_no_fee, transaction2.crypto_out_no_fee)
                self.assertEqual(transaction1.crypto_fee, transaction2.crypto_fee)
            self.assertEqual(transaction1.notes, transaction2.notes)

    def test_dict_cache(self) -> None:
        cache_name: str = "test_dict_cache"
        try:
            (ROOT_PATH / CACHE_DIR / cache_name).unlink()
        except FileNotFoundError:
            pass
        dictionary: Dict[str, int] = {
            "abc": 12,
            "def": 87,
            "ghi": 98,
        }
        save_to_cache(cache_name, dictionary)
        loaded_dictionary: Dict[str, int] = load_from_cache(cache_name)
        self.assertEqual(dictionary, loaded_dictionary)


if __name__ == "__main__":
    unittest.main()
