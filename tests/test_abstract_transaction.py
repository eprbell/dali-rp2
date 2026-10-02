# Copyright 2026 Neal Chambers
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

from dali.configuration import Keyword
from dali.in_transaction import InTransaction


def _transaction(plugin: str = "Plugin", asset: str = "BTC") -> InTransaction:
    return InTransaction(
        plugin=plugin,
        unique_id="1",
        raw_data="raw",
        timestamp="2021-01-01 00:00:00+0000",
        asset=asset,
        exchange="Kraken",
        holder="test",
        transaction_type=Keyword.BUY.value,
        spot_price="1",
        crypto_in="1",
        crypto_fee=None,
        fiat_in_no_fee=None,
        fiat_in_with_fee=None,
        fiat_fee=None,
        notes="notes",
    )


class TestAbstractTransaction:
    def test_transactions_with_the_same_id_plugin_and_asset_are_equal(self) -> None:
        assert _transaction() == _transaction()
        assert hash(_transaction()) == hash(_transaction())

    # Equal transactions must have equal hashes, and the hash includes the plugin and asset
    def test_transactions_from_different_plugins_are_not_equal(self) -> None:
        assert _transaction(plugin="Plugin A") != _transaction(plugin="Plugin B")

    def test_transactions_of_different_assets_are_not_equal(self) -> None:
        assert _transaction(asset="BTC") != _transaction(asset="ETH")
