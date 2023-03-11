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

from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.out_transaction import OutTransaction
from dali.plugin.input.ods.rp2_input import InputPlugin


class TestRP2InputOds:
    def test_btc_transactions(self) -> None:
        plugin = InputPlugin(
            configuration_path='input/test_ods_rp2_input.ini',
            input_file='input/test_ods_rp2_input.ods',
            country='us',
            native_fiat="USD"
        )
        result = plugin.load()
        assert len(result) == 6

        btc_transaction_in_1: InTransaction = result[0]  # type: ignore
        # btc_transaction_in_2: InTransaction = result[1]  # type: ignore
        # btc_transaction_intra_1: InTransaction = result[2]  # type: ignore
        # btc_transaction_intra_2: InTransaction = result[3]  # type: ignore
        # btc_transaction_intra_3: InTransaction = result[4]  # type: ignore
        # btc_transaction_out_1: OutTransaction = result[5]  # type: ignore

        # Buy BTC
        assert btc_transaction_in_1.asset == "BTC"
        assert btc_transaction_in_1.timestamp == "2020-06-28 14:38:40+0000"
        assert btc_transaction_in_1.transaction_type == Keyword.BUY.value.capitalize()
        assert RP2Decimal(btc_transaction_in_1.spot_price) == RP2Decimal('12000.00')
        assert RP2Decimal(btc_transaction_in_1.crypto_in) == RP2Decimal("0.01")
        assert btc_transaction_in_1.crypto_fee is None
        assert RP2Decimal(btc_transaction_in_1.fiat_in_no_fee) == RP2Decimal('120.00')
        assert RP2Decimal(btc_transaction_in_1.fiat_in_with_fee) == RP2Decimal('130.00')
        assert RP2Decimal(btc_transaction_in_1.fiat_fee) == RP2Decimal('10.00')
