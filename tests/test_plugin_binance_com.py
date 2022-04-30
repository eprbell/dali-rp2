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

from typing import Any, Dict, List

from rp2.rp2_decimal import RP2Decimal

from dali.in_transaction import InTransaction
from dali.out_transaction import OutTransaction
from dali.plugin.input.rest.binance_com import InputPlugin
from dali.dali_configuration import Keyword


class TestBinance:

    # pylint: disable=no-self-use
    def test_deposits(self, mocker: Any) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            api_key="a",
            api_secret="b",
            username="user",
        )

        mocker.patch.object(plugin.client, "sapiGetFiatPayments").return_value = {
               "code": "000000",
               "message": "success",
               "data": [
               {
                  "orderNo": "353fca443f06466db0c4dc89f94f027a",
                  "sourceAmount": "20.0",  # Fiat trade amount
                  "fiatCurrency": "EUR",   # Fiat token
                  "obtainAmount": "4.462", # Crypto trade amount
                  "cryptoCurrency": "LUNA",  # Crypto token
                  "totalFee": "0.2",     # Trade fee
                  "price": "4.437472", 
                  "status": "Completed",  # Processing, Completed, Failed, Refunded
                  "createTime": 1624529919000,
                  "updateTime": 1624529919000  
               },
               {               
                  "orderNo": "353fca443f06466db0c4dc89f94f027a",
                  "sourceAmount": "40.0",  # Fiat trade amount
                  "fiatCurrency": "EUR",   # Fiat token
                  "obtainAmount": "8.924", # Crypto trade amount
                  "cryptoCurrency": "LUNA",  # Crypto token
                  "totalFee": "0.4",     # Trade fee
                  "price": "4.437472", 
                  "status": "Failed",  # Processing, Completed, Failed, Refunded
                  "createTime": 1624529919000,
                  "updateTime": 1624529919000  
               }
               ],
               "total": 2,
               "success": True
        }

        mocker.patch.object(plugin.client, "fetch_deposits").return_value = {}
        mocker.patch.object(plugin.client, "sapiGetFiatOrders").return_value = {}
        mocker.patch.object(plugin, "_process_trades").return_value = None
        mocker.patch.object(plugin, "_process_gains").return_value = None

        result = plugin.load()
        assert len(result) == 1

        fiat_in_transaction: InTransaction = result[0]

        assert fiat_in_transaction.asset == "LUNA"
        assert fiat_in_transaction.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1624529919000)
        assert fiat_in_transaction.transaction_type == Keyword.BUY.value
        assert RP2Decimal(fiat_in_transaction.spot_price) == RP2Decimal("20.0") / RP2Decimal("4.462")
        assert RP2Decimal(fiat_in_transaction.crypto_in) == RP2Decimal("4.462")
        assert fiat_in_transaction.crypto_fee == None
        assert RP2Decimal(fiat_in_transaction.fiat_in_no_fee) == RP2Decimal("20.0")
        assert RP2Decimal(fiat_in_transaction.fiat_in_with_fee) == RP2Decimal("19.8")
        assert RP2Decimal(fiat_in_transaction.fiat_fee) == RP2Decimal("0.2")
        # assert fiat_in_transaction.fiat_iso_code == "EUR"

