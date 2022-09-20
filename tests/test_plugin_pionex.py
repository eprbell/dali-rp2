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
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction
from dali.plugin.input.csv.pionex import InputPlugin


class TestPionex:
    def test_trades(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            trades_csv_file="input/test_pionex_trades.csv",
            transfers_csv_file=None,
            native_fiat="USD",
        )

        result = plugin.load()

        # 1 ETH in transaction for 0.1 ETH +
        # 1 BUSD out transaction for 200 BUSD +
        # 1 BUSD in transaction for 0.05 ETH +
        # 1 ETH out transaction for 110 BUSD = 4
        assert len(result) == 4

        eth_transaction_in: InTransaction = result[0]  # type: ignore
        busd_transaction_out: OutTransaction = result[1]  # type: ignore
        busd_transaction_in: InTransaction = result[2]  # type: ignore
        eth_transaction_out: OutTransaction = result[3]  # type: ignore

        # Buy ETH autoinvest
        assert eth_transaction_in.asset == "ETH"
        assert eth_transaction_in.timestamp == "2022-02-27 12:09:00+0000"
        assert eth_transaction_in.transaction_type == Keyword.BUY.value.capitalize()
        assert eth_transaction_in.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(eth_transaction_in.crypto_in) == RP2Decimal("0.1")
        assert RP2Decimal(str(eth_transaction_in.crypto_fee)) == RP2Decimal("0.00995")
        assert eth_transaction_in.fiat_in_no_fee is None
        assert eth_transaction_in.fiat_in_with_fee is None
        assert eth_transaction_in.fiat_fee is None

        assert busd_transaction_out.asset == "BUSD"
        assert busd_transaction_out.timestamp == "2022-02-27 12:09:00+0000"
        assert busd_transaction_out.transaction_type == Keyword.SELL.value.capitalize()
        assert busd_transaction_out.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(busd_transaction_out.crypto_out_no_fee) == RP2Decimal("200.00")
        assert RP2Decimal(busd_transaction_out.crypto_fee) == ZERO
        assert RP2Decimal(str(busd_transaction_out.crypto_out_with_fee)) == RP2Decimal("200.00")
        assert busd_transaction_out.fiat_out_no_fee is None
        assert busd_transaction_out.fiat_fee is None

        # Buy BTC autoinvest
        assert busd_transaction_in.asset == "BUSD"
        assert busd_transaction_in.timestamp == "2022-02-27 12:11:00+0000"
        assert busd_transaction_in.transaction_type == Keyword.BUY.value.capitalize()
        assert busd_transaction_in.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(busd_transaction_in.crypto_in) == RP2Decimal("110.00")
        assert RP2Decimal(str(busd_transaction_in.crypto_fee)) == RP2Decimal("0.055")
        assert busd_transaction_in.fiat_in_no_fee is None
        assert busd_transaction_in.fiat_in_with_fee is None
        assert busd_transaction_in.fiat_fee is None

        assert eth_transaction_out.asset == "ETH"
        assert eth_transaction_out.timestamp == "2022-02-27 12:11:00+0000"
        assert eth_transaction_out.transaction_type == Keyword.SELL.value.capitalize()
        assert eth_transaction_out.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(eth_transaction_out.crypto_out_no_fee) == RP2Decimal("0.05")
        assert RP2Decimal(eth_transaction_out.crypto_fee) == ZERO
        assert RP2Decimal(str(eth_transaction_out.crypto_out_with_fee)) == RP2Decimal("0.05")
        assert eth_transaction_out.fiat_out_no_fee is None
        assert eth_transaction_out.fiat_fee is None

    def test_transfers(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            trades_csv_file=None,
            transfers_csv_file="input/test_pionex_transfers.csv",
            native_fiat="USD",
        )

        result = plugin.load()

        # 1 BUSD deposit +
        # 1 BUSD withdrawal = 2
        assert len(result) == 2

        busd_deposit_transaction: IntraTransaction = result[0]  # type: ignore
        busd_withdrawal_transaction: IntraTransaction = result[1]  # type: ignore

        assert busd_deposit_transaction.asset == "BUSD"
        assert busd_deposit_transaction.timestamp == "2022-02-27 12:02:00+0000"
        assert busd_deposit_transaction.unique_id == "12345"
        assert busd_deposit_transaction.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(busd_deposit_transaction.crypto_received) == RP2Decimal("115")
        assert busd_deposit_transaction.crypto_sent == Keyword.UNKNOWN.value
        assert busd_deposit_transaction.from_exchange == Keyword.UNKNOWN.value
        assert busd_deposit_transaction.from_holder == Keyword.UNKNOWN.value
        assert busd_deposit_transaction.to_exchange == "Pionex"
        assert busd_deposit_transaction.to_holder == "tester"

        assert busd_withdrawal_transaction.asset == "BUSD"
        assert busd_withdrawal_transaction.timestamp == "2022-03-25 11:02:00+0000"
        assert busd_withdrawal_transaction.unique_id == "12346"
        assert busd_withdrawal_transaction.spot_price == Keyword.UNKNOWN.value
        assert busd_withdrawal_transaction.crypto_received == Keyword.UNKNOWN.value
        assert RP2Decimal(busd_withdrawal_transaction.crypto_sent) == RP2Decimal("100")
        assert busd_withdrawal_transaction.from_exchange == "Pionex"
        assert busd_withdrawal_transaction.from_holder == "tester"
        assert busd_withdrawal_transaction.to_exchange == Keyword.UNKNOWN.value
        assert busd_withdrawal_transaction.to_holder == Keyword.UNKNOWN.value
