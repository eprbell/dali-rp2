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
from dali.intra_transaction import IntraTransaction
from dali.plugin.input.csv.bitbank_supplemental import InputPlugin


class TestBitbank:
    def test_withdrawals(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            withdrawals_csv_file="input/test_bitbank_withdrawals.csv",
            withdrawals_code="XLM",
            deposits_csv_file="input/test_bitbank_deposits.csv",
            deposits_code="JPY",
            native_fiat="USD",
        )

        result = plugin.load()

        # 1 XLM withdrawal for 900 XLM
        # 1 JPY deposit for 6900 JPY
        assert len(result) == 2

        xlm_transaction: IntraTransaction = result[0]  # type: ignore
        jpy_transaction: InTransaction = result[1]  # type: ignore

        assert xlm_transaction.asset == "XLM"
        assert xlm_transaction.timestamp == "2022-04-20 07:20:00+0000"
        assert xlm_transaction.unique_id == "TXID12345"
        assert xlm_transaction.spot_price == Keyword.UNKNOWN.value
        assert xlm_transaction.crypto_received == Keyword.UNKNOWN.value
        assert RP2Decimal(xlm_transaction.crypto_sent) == RP2Decimal("900.01")
        assert xlm_transaction.from_exchange == "Bitbank.cc"
        assert xlm_transaction.from_holder == "tester"
        assert xlm_transaction.to_exchange == Keyword.UNKNOWN.value
        assert xlm_transaction.to_holder == Keyword.UNKNOWN.value

        assert jpy_transaction.asset == "JPY"
        assert jpy_transaction.timestamp == "2022-04-19 22:19:00+0000"
        assert RP2Decimal(jpy_transaction.spot_price) == RP2Decimal("1")
        assert RP2Decimal(jpy_transaction.crypto_in) == RP2Decimal("6900")
        assert jpy_transaction.crypto_fee is None
        assert RP2Decimal(str(jpy_transaction.fiat_in_no_fee)) == RP2Decimal("6900")
        assert RP2Decimal(str(jpy_transaction.fiat_in_with_fee)) == RP2Decimal("6900")
        assert jpy_transaction.fiat_ticker == "JPY"
