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

from rp2.plugin.country.us import US
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
            deposits_csv_file="input/test_bitbank_deposits.csv",
            fiat_deposits_csv_file="input/test_bitbank_fiat_deposits.csv",
            native_fiat="USD",
        )

        result = plugin.load(US())

        # 1 XLM withdrawal for 900 XLM
        # 1 ETH withdrawal for 0.125 ETH
        # 1 XRP deposit for 54.54 XRP
        # 1 BTC deposit for 0.00001 BTC
        # 1 JPY deposit for 6900 JPY
        assert len(result) == 5

        xlm_withdrawal: IntraTransaction = result[0]  # type: ignore
        eth_withdrawal: IntraTransaction = result[1]  # type: ignore
        xrp_deposit: IntraTransaction = result[2]  # type: ignore
        btc_deposit: IntraTransaction = result[3]  # type: ignore
        jpy_transaction: InTransaction = result[4]  # type: ignore

        assert xlm_withdrawal.asset == "XLM"
        assert xlm_withdrawal.timestamp == "2022-04-20 07:20:00+0000"
        assert xlm_withdrawal.unique_id == "TXID12345"
        assert xlm_withdrawal.spot_price == Keyword.UNKNOWN.value
        assert xlm_withdrawal.crypto_received == Keyword.UNKNOWN.value
        assert RP2Decimal(xlm_withdrawal.crypto_sent) == RP2Decimal("900.01")
        assert xlm_withdrawal.from_exchange == "Bitbank.cc"
        assert xlm_withdrawal.from_holder == "tester"
        assert xlm_withdrawal.to_exchange == Keyword.UNKNOWN.value
        assert xlm_withdrawal.to_holder == Keyword.UNKNOWN.value

        assert eth_withdrawal.asset == "ETH"
        assert eth_withdrawal.timestamp == "2022-04-25 07:21:00+0000"
        assert eth_withdrawal.unique_id == "TXID12346"
        assert eth_withdrawal.spot_price == Keyword.UNKNOWN.value
        assert eth_withdrawal.crypto_received == Keyword.UNKNOWN.value
        assert RP2Decimal(eth_withdrawal.crypto_sent) == RP2Decimal("0.1251")
        assert eth_withdrawal.from_exchange == "Bitbank.cc"
        assert eth_withdrawal.from_holder == "tester"
        assert eth_withdrawal.to_exchange == Keyword.UNKNOWN.value
        assert eth_withdrawal.to_holder == Keyword.UNKNOWN.value

        assert xrp_deposit.asset == "XRP"
        assert xrp_deposit.timestamp == "2022-11-12 02:57:01+0000"
        assert xrp_deposit.unique_id == "TXID12348"
        assert xrp_deposit.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(xrp_deposit.crypto_received) == RP2Decimal("54.54")
        assert xrp_deposit.crypto_sent == Keyword.UNKNOWN.value
        assert xrp_deposit.from_exchange == Keyword.UNKNOWN.value
        assert xrp_deposit.from_holder == Keyword.UNKNOWN.value
        assert xrp_deposit.to_exchange == "Bitbank.cc"
        assert xrp_deposit.to_holder == "tester"

        assert btc_deposit.asset == "BTC"
        assert btc_deposit.timestamp == "2022-10-13 01:57:01+0000"
        assert btc_deposit.unique_id == "TXID12349"
        assert btc_deposit.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(btc_deposit.crypto_received) == RP2Decimal("0.00001")
        assert btc_deposit.crypto_sent == Keyword.UNKNOWN.value
        assert btc_deposit.from_exchange == Keyword.UNKNOWN.value
        assert btc_deposit.from_holder == Keyword.UNKNOWN.value
        assert btc_deposit.to_exchange == "Bitbank.cc"
        assert btc_deposit.to_holder == "tester"

        assert jpy_transaction.asset == "JPY"
        assert jpy_transaction.timestamp == "2022-04-19 22:19:00+0000"
        assert RP2Decimal(jpy_transaction.spot_price) == RP2Decimal("1")
        assert RP2Decimal(jpy_transaction.crypto_in) == RP2Decimal("6900")
        assert jpy_transaction.crypto_fee is None
        assert RP2Decimal(str(jpy_transaction.fiat_in_no_fee)) == RP2Decimal("6900")
        assert RP2Decimal(str(jpy_transaction.fiat_in_with_fee)) == RP2Decimal("6900")
        assert jpy_transaction.fiat_ticker == "JPY"
