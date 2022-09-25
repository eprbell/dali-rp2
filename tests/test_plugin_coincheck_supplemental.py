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

from rp2.rp2_decimal import RP2Decimal

from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.plugin.input.csv.coincheck_supplemental import InputPlugin


class TestCoincheck:
    def test_trades(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            buys_csv_file="input/test_coincheck_buys.csv",
            native_fiat="USD",
        )

        result = plugin.load()

        # 1 0.005 BTC purchase
        assert len(result) == 1

        btc_buy_in: InTransaction = result[0]  # type: ignore

        assert btc_buy_in.asset == "BTC"
        assert btc_buy_in.timestamp == "2022-04-20 16:20:00+0000"
        assert btc_buy_in.transaction_type == Keyword.BUY.value.capitalize()
        assert RP2Decimal(str(btc_buy_in.spot_price)) == RP2Decimal("2000000")
        assert RP2Decimal(btc_buy_in.crypto_in) == RP2Decimal("0.005")
        assert btc_buy_in.crypto_fee is None
        assert RP2Decimal(str(btc_buy_in.fiat_in_no_fee)) == RP2Decimal("10000")
        assert RP2Decimal(str(btc_buy_in.fiat_in_with_fee)) == RP2Decimal("10000")
        assert btc_buy_in.fiat_fee is None
