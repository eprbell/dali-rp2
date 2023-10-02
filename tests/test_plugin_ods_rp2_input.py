# Copyright 2023 jamesbaber1
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
import unittest
from typing import List

from rp2.plugin.country.us import US
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction
from dali.plugin.input.ods.rp2_input import InputPlugin


class TestRP2InputOds(unittest.TestCase):
    transactions: List[AbstractTransaction]

    @classmethod
    def setUpClass(cls) -> None:
        plugin = InputPlugin(configuration_path="input/test_ods_rp2_input.ini", input_file="input/test_ods_rp2_input.ods", native_fiat="USD")
        cls.transactions = plugin.load(US())

    def test_all_transactions_found(self) -> None:
        assert len(self.transactions) == 9

    def test_in_transactions(self) -> None:
        # Buy BTC transaction 1
        btc_transaction_in_1: InTransaction = self.transactions[0]  # type: ignore
        assert btc_transaction_in_1.asset == "BTC"
        assert btc_transaction_in_1.exchange == "FTX"
        assert btc_transaction_in_1.holder == "Bob"
        assert btc_transaction_in_1.timestamp == "2020-06-28 14:38:40+0000"
        assert btc_transaction_in_1.transaction_type == Keyword.BUY.value.capitalize()
        assert RP2Decimal(str(btc_transaction_in_1.spot_price)) == RP2Decimal("12000.00")
        assert RP2Decimal(str(btc_transaction_in_1.crypto_in)) == RP2Decimal("0.01")
        assert btc_transaction_in_1.crypto_fee is None
        assert RP2Decimal(str(btc_transaction_in_1.fiat_in_no_fee)) == RP2Decimal("120.00")
        assert RP2Decimal(str(btc_transaction_in_1.fiat_in_with_fee)) == RP2Decimal("130.00")
        assert RP2Decimal(str(btc_transaction_in_1.fiat_fee)) == RP2Decimal("10.00")

        # Buy BTC transaction 2
        btc_transaction_in_2: InTransaction = self.transactions[1]  # type: ignore
        assert btc_transaction_in_2.asset == "BTC"
        assert btc_transaction_in_1.exchange == "FTX"
        assert btc_transaction_in_1.holder == "Bob"
        assert btc_transaction_in_2.timestamp == "2022-01-02 18:11:09+0000"
        assert btc_transaction_in_2.transaction_type == Keyword.BUY.value.capitalize()
        assert RP2Decimal(str(btc_transaction_in_2.spot_price)) == RP2Decimal("35000.00")
        assert RP2Decimal(str(btc_transaction_in_2.crypto_in)) == RP2Decimal("0.99")
        assert btc_transaction_in_2.crypto_fee is None
        assert RP2Decimal(str(btc_transaction_in_2.fiat_in_no_fee)) == RP2Decimal("34650.00")
        assert RP2Decimal(str(btc_transaction_in_2.fiat_in_with_fee)) == RP2Decimal("35140.00")
        assert RP2Decimal(str(btc_transaction_in_2.fiat_fee)) == RP2Decimal("490.00")

        # Buy ETH transaction 1
        eth_transaction_in_1: InTransaction = self.transactions[6]  # type: ignore
        assert eth_transaction_in_1.asset == "ETH"
        assert eth_transaction_in_1.exchange == "Coinbase"
        assert eth_transaction_in_1.holder == "Bob"
        assert eth_transaction_in_1.timestamp == "2020-06-03 11:23:00+0000"
        assert eth_transaction_in_1.transaction_type == Keyword.BUY.value.capitalize()
        assert RP2Decimal(str(eth_transaction_in_1.spot_price)) == RP2Decimal("244")
        assert RP2Decimal(str(eth_transaction_in_1.crypto_in)) == RP2Decimal("10")
        assert eth_transaction_in_1.crypto_fee is None
        assert RP2Decimal(str(eth_transaction_in_1.fiat_in_no_fee)) == RP2Decimal("2440")
        assert RP2Decimal(str(eth_transaction_in_1.fiat_in_with_fee)) == RP2Decimal("2465")
        assert RP2Decimal(str(eth_transaction_in_1.fiat_fee)) == RP2Decimal("25.00")

    def test_out_transactions(self) -> None:
        # Sell BTC
        btc_transaction_out_1: OutTransaction = self.transactions[5]  # type: ignore
        assert btc_transaction_out_1.asset == "BTC"
        assert btc_transaction_out_1.timestamp == "2022-02-09 11:45:34-0800"
        assert btc_transaction_out_1.transaction_type == Keyword.GIFT.value.capitalize()
        assert RP2Decimal(str(btc_transaction_out_1.spot_price)) == RP2Decimal("44628.22")
        assert RP2Decimal(str(btc_transaction_out_1.crypto_out_no_fee)) == RP2Decimal("0.02")
        assert RP2Decimal(str(btc_transaction_out_1.crypto_out_with_fee)) == RP2Decimal("0.02")
        assert RP2Decimal(str(btc_transaction_out_1.crypto_fee)) == ZERO
        assert btc_transaction_out_1.fiat_fee is None

    def test_intra_transactions(self) -> None:
        # BTC transfer 1
        btc_transaction_intra_1: IntraTransaction = self.transactions[2]  # type: ignore
        assert btc_transaction_intra_1.asset == "BTC"
        assert btc_transaction_intra_1.timestamp == "2022-01-14 03:23:38-0800"
        assert RP2Decimal(str(btc_transaction_intra_1.spot_price)) == RP2Decimal("41952.11")
        assert btc_transaction_intra_1.from_exchange == "FTX"
        assert btc_transaction_intra_1.to_exchange == "Green Trezor"
        assert btc_transaction_intra_1.from_holder == "Bob"
        assert btc_transaction_intra_1.to_holder == "Bob"
        assert RP2Decimal(str(btc_transaction_intra_1.crypto_sent)) == RP2Decimal("0.2002")
        assert RP2Decimal(str(btc_transaction_intra_1.crypto_received)) == RP2Decimal("0.20")

        # BTC transfer 2
        btc_transaction_intra_2: IntraTransaction = self.transactions[3]  # type: ignore
        assert btc_transaction_intra_2.asset == "BTC"
        assert btc_transaction_intra_2.timestamp == "2022-01-18 15:27:18+0000"
        assert RP2Decimal(str(btc_transaction_intra_2.spot_price)) == RP2Decimal("41736.81")
        assert btc_transaction_intra_2.from_exchange == "Green Trezor"
        assert btc_transaction_intra_2.to_exchange == "FTX"
        assert btc_transaction_intra_2.from_holder == "Bob"
        assert btc_transaction_intra_2.to_holder == "Bob"
        assert RP2Decimal(str(btc_transaction_intra_2.crypto_sent)) == RP2Decimal("0.1001")
        assert RP2Decimal(str(btc_transaction_intra_2.crypto_received)) == RP2Decimal("0.10")

        # BTC transfer 3
        btc_transaction_intra_3: IntraTransaction = self.transactions[4]  # type: ignore
        assert btc_transaction_intra_3.asset == "BTC"
        assert btc_transaction_intra_3.timestamp == "2022-01-25 02:58:40-0800"
        assert RP2Decimal(str(btc_transaction_intra_3.spot_price)) == RP2Decimal("36463.4")
        assert btc_transaction_intra_3.from_exchange == "Green Trezor"
        assert btc_transaction_intra_3.to_exchange == "My Fave Trezor"
        assert btc_transaction_intra_3.from_holder == "Bob"
        assert btc_transaction_intra_3.to_holder == "Alice"
        assert RP2Decimal(str(btc_transaction_intra_3.crypto_sent)) == RP2Decimal("0.0503")
        assert RP2Decimal(str(btc_transaction_intra_3.crypto_received)) == RP2Decimal("0.05")
