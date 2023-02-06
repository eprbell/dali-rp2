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

from rp2.rp2_error import RP2RuntimeError

from dali.in_transaction import InTransaction
from dali.out_transaction import OutTransaction
from dali.plugin.input.rest.coinbase_pro import InputPlugin


class TestSwapFill:
    def test_buy_side(self, mocker: Any) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            api_key="a",
            api_secret="b",
            api_passphrase="c",
            native_fiat="USD",
            thread_count=1,
        )

        btc_account_id = "bbbbbbbb-dddd-4444-8888-000000000000"
        eth_account_id = "eeeeeeee-4444-5555-aaaa-cccccccccccc"
        mocker.patch.object(plugin, "_InputPlugin__get_accounts").return_value = [
            {
                "id": btc_account_id,
                "currency": "BTC",
            },
            {
                "id": eth_account_id,
                "currency": "ETH",
            },
        ]

        def mock_get_transaction(account_id: str) -> List[Dict[str, Any]]:
            if account_id == eth_account_id:
                return [
                    {
                        "id": "1111111111",
                        "amount": "10.0000000000000000",
                        "balance": "11.0000000000000000",
                        "created_at": "2020-12-11T00:20:14.693676Z",
                        "type": "match",
                        "details": {
                            "order_id": "33333333-bbbb-4444-cccc-aaaaaaaaaaaa",
                            "product_id": "ETH-BTC",
                            "trade_id": "134567890",
                        },
                    },
                ]

            if account_id == btc_account_id:
                return [
                    {
                        "id": "1111111111",
                        "amount": "-0.0005000000000000",
                        "balance": "0.9995000000000000",
                        "created_at": "2020-12-11T00:20:14.693676Z",
                        "type": "fee",
                        "details": {
                            "order_id": "33333333-bbbb-4444-cccc-aaaaaaaaaaaa",
                            "product_id": "ETH-BTC",
                            "trade_id": "134567890",
                        },
                    },
                    {
                        "id": "1111111108",
                        "amount": "-0.5000000000000000",
                        "balance": "1.0000000000000000",
                        "created_at": "2020-12-11T00:20:14.693676Z",
                        "type": "match",
                        "details": {
                            "order_id": "33333333-bbbb-4444-cccc-aaaaaaaaaaaa",
                            "product_id": "ETH-BTC",
                            "trade_id": "134567890",
                        },
                    },
                ]

            raise RP2RuntimeError("Invalid account id: " + account_id)

        mocker.patch.object(plugin, "_InputPlugin__get_transactions").side_effect = mock_get_transaction

        mocker.patch.object(plugin, "_InputPlugin__get_fills").return_value = [
            {
                "created_at": "2020-12-11T00:20:14.693676Z",
                "trade_id": "134567890",
                "product_id": "ETH-BTC",
                "order_id": "33333333-bbbb-4444-cccc-aaaaaaaaaaaa",
                "liquidity": "M",
                "price": "0.05000000",
                "size": "10.00000000",
                "fee": "0.0005000000000000",
                "side": "buy",
                "settled": True,
                "usd_volume": "25000.000000000000000000000000",
            }
        ]

        result = plugin.load()
        assert len(result) == 2

        in_transaction: InTransaction = result[0]  # type: ignore
        out_transaction: OutTransaction = result[1]  # type: ignore

        assert out_transaction.asset == "BTC"
        assert out_transaction.timestamp == "2020-12-11 00:20:14.693676+0000"
        assert out_transaction.transaction_type == "Sell"
        assert out_transaction.spot_price == "50000.00000000"
        assert out_transaction.crypto_out_no_fee == "0.5000000000000000"
        assert out_transaction.crypto_fee == "0.0005000000000000"
        assert out_transaction.crypto_out_with_fee is None
        assert out_transaction.fiat_out_no_fee is None
        assert out_transaction.fiat_fee is None

        assert in_transaction.asset == "ETH"
        assert in_transaction.timestamp == "2020-12-11 00:20:14.693676+0000"
        assert in_transaction.transaction_type == "Buy"
        assert in_transaction.spot_price == "2500.0000000000000000"
        assert in_transaction.crypto_in == "10.00000000"
        assert in_transaction.crypto_fee == "0"
        assert in_transaction.fiat_in_no_fee is None
        assert in_transaction.fiat_in_with_fee is None
        assert in_transaction.fiat_fee is None
