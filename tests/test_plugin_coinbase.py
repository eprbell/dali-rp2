# Copyright 2022 QP Hou
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

from typing import Any, Dict, List

from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2RuntimeError

from dali.in_transaction import InTransaction
from dali.out_transaction import OutTransaction
from dali.plugin.input.rest.coinbase import InputPlugin


class TestTrade:
    def test_eth2_stake(self, mocker: Any) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            api_key="a",
            api_secret="b",
            native_fiat="USD",
            thread_count=1,
        )

        eth_account_id = "bbbbbbbb-dddd-4444-8888-000000000000"
        eth2_account_id = "eeeeeeee-4444-5555-aaaa-cccccccccccc"
        mocker.patch.object(plugin, "_InputPlugin__get_accounts").return_value = [
            {
                "id": eth_account_id,
                "name": "ETH Wallet",
                "primary": False,
                "type": "wallet",
                "currency": {
                    "code": "ETH",
                    "name": "Ethereum",
                    "type": "crypto",
                    "address_regex": "^(?:0x)?[0-9a-fA-F]{40}$",
                    "slug": "ethereum",
                },
                "balance": {"amount": "0.00500000", "currency": "ETH"},
                "created_at": "1999-02-08T01:18:06Z",
                "updated_at": "2022-01-06T04:18:23Z",
                "resource": "account",
                "resource_path": f"/v2/accounts/{eth_account_id}",
                "allow_deposits": True,
                "allow_withdrawals": True,
            },
            {
                "id": eth2_account_id,
                "name": "ETH2 Wallet",
                "primary": False,
                "type": "wallet",
                "currency": {
                    "code": "ETH2",
                    "name": "Ethereum 2",
                    "type": "crypto",
                    "address_regex": "^(?:0x)?[0-9a-fA-F]{40}$",
                    "slug": "ethereum-2",
                },
                "balance": {"amount": "0.10000000", "currency": "ETH2"},
                "created_at": "2000-12-11T07:00:55Z",
                "updated_at": "2021-01-01T10:08:59Z",
                "resource": "account",
                "resource_path": f"/v2/accounts/{eth2_account_id}",
                "allow_deposits": False,
                "allow_withdrawals": False,
                "rewards": {"apy": "0.03675", "formatted_apy": "3.68%", "label": "3.68% APR"},
            },
        ]

        def mock_get_transaction(account_id: str) -> List[Dict[str, Any]]:
            trade_id = "aaaaaaaa-cccc-0000-ffff-bbbbbbbbbbbb"
            if account_id == eth_account_id:
                transaction_id = "e1111111-7777-cccc-6666-ffffffffffff"
                return [
                    {
                        "id": transaction_id,
                        "type": "trade",
                        "status": "completed",
                        "amount": {"amount": "-0.10000000", "currency": "ETH"},
                        "native_amount": {"amount": "-300.00", "currency": "USD"},
                        "description": None,
                        "created_at": "2020-12-11T00:20:59Z",
                        "updated_at": "2020-12-11T00:20:59Z",
                        "resource": "transaction",
                        "resource_path": f"/v2/accounts/{eth_account_id}/transactions/{transaction_id}",
                        "instant_exchange": False,
                        "trade": {
                            "id": trade_id,
                            "resource": "trade",
                            "resource_path": f"/v2/accounts/{eth2_account_id}/trades/{trade_id}",
                        },
                        "details": {
                            "title": "Converted from Ethereum",
                            "subtitle": "Using ETH Wallet",
                            "header": "Converted 0.10000000 ETH ($300.00)",
                            "health": "positive",
                            "payment_method_name": "ETH Wallet",
                        },
                        "hide_native_amount": False,
                    },
                ]
            if account_id == eth2_account_id:
                transaction_id = "e2222222-cccc-ffff-aaaa-000000000000"
                return [
                    {
                        "id": transaction_id,
                        "type": "trade",
                        "status": "completed",
                        "amount": {"amount": "0.10000000", "currency": "ETH2"},
                        "native_amount": {"amount": "300.00", "currency": "USD"},
                        "description": None,
                        "created_at": "2020-12-11T00:20:59Z",
                        "updated_at": "2020-12-11T00:20:59Z",
                        "resource": "transaction",
                        "resource_path": f"/v2/accounts/{eth2_account_id}/transactions/{transaction_id}",
                        "instant_exchange": False,
                        "trade": {"id": trade_id, "resource": "trade", "resource_path": f"/v2/accounts/{eth2_account_id}/trades/{trade_id}"},
                        "details": {
                            "title": "Converted to Ethereum 2",
                            "subtitle": "Using ETH Wallet",
                            "header": "Converted 0.10000000 ETH2",
                            "health": "positive",
                            "payment_method_name": "ETH Wallet",
                        },
                        "hide_native_amount": True,
                    },
                ]

            raise RP2RuntimeError("Invalid account id: " + account_id)

        mocker.patch.object(plugin, "_InputPlugin__get_transactions").side_effect = mock_get_transaction

        mocker.patch.object(plugin, "_InputPlugin__get_buys").return_value = []
        mocker.patch.object(plugin, "_InputPlugin__get_sells").return_value = []

        result = plugin.load()
        assert len(result) == 2

        out_transaction: OutTransaction = result[0]  # type: ignore
        in_transaction: InTransaction = result[1]  # type: ignore

        # coinbase doesn't charge fees for eth2 stake
        assert out_transaction.asset == "ETH"
        assert out_transaction.timestamp == "2020-12-11 00:20:59+0000"
        assert out_transaction.transaction_type == "Sell"
        assert RP2Decimal(out_transaction.spot_price) == RP2Decimal("3E+3")
        assert RP2Decimal(out_transaction.crypto_out_no_fee) == RP2Decimal("0.10000000")
        assert RP2Decimal(out_transaction.crypto_fee) == RP2Decimal("0.000")
        assert RP2Decimal(out_transaction.crypto_out_with_fee) == RP2Decimal("0.10000000")  # type: ignore
        assert RP2Decimal(out_transaction.fiat_out_no_fee) == RP2Decimal("300.00")  # type: ignore
        assert RP2Decimal(out_transaction.fiat_fee) == RP2Decimal("0")  # type: ignore

        assert in_transaction.asset == "ETH2"
        assert in_transaction.timestamp == "2020-12-11 00:20:59+0000"
        assert in_transaction.transaction_type == "Buy"
        assert RP2Decimal(in_transaction.spot_price) == RP2Decimal("3E+3")
        assert RP2Decimal(in_transaction.crypto_in) == RP2Decimal("0.10000000")
        assert in_transaction.crypto_fee is None
        assert RP2Decimal(in_transaction.fiat_in_no_fee) == RP2Decimal("300.00")  # type: ignore
        assert RP2Decimal(in_transaction.fiat_in_with_fee) == RP2Decimal("300.00")  # type: ignore
        assert RP2Decimal(in_transaction.fiat_fee) == RP2Decimal("0")  # type: ignore
