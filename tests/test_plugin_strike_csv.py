# Copyright 2026 anlach
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
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.configuration import Keyword
from dali.intra_transaction import IntraTransaction
from dali.plugin.input.csv.strike import InputPlugin


class TestStrikeCsv:
    def test_single_file(self) -> None:
        """Test loading a single CSV file."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="strike_wallet",
            strike_csv_files="input/test_strike.csv",
            timezone="UTC",
            native_fiat="USD",
        )

        result = plugin.load(US())

        # Test data has:
        # - 2 Deposit (fiat only - skipped)
        # - 2 Purchase (2 crypto in transactions)
        # - 2 Send (2 crypto out transactions)
        # - 1 Receive (1 crypto in transaction)
        # Total: 5 crypto transactions
        assert len(result) == 5

        # Verify we have the right types
        receive_count = sum(1 for t in result if isinstance(t, IntraTransaction))
        assert receive_count == 5

        # Check a Receive transaction (Jan 25 2024)
        receive_tx = next((t for t in result if "0.00350000" in t.crypto_received), None)
        assert receive_tx is not None
        assert receive_tx.asset == "BTC"
        assert receive_tx.plugin == "Strike"

        # Check a Send transaction (Jan 20 2024)
        send_tx = next((t for t in result if "0.00500000" in t.crypto_sent), None)
        assert send_tx is not None
        assert send_tx.asset == "BTC"

        # Check a Purchase transaction (Jan 15 2024)
        purchase_tx = next((t for t in result if "0.02150000" in t.crypto_received), None)
        assert purchase_tx is not None
        assert purchase_tx.asset == "BTC"

    def test_multiple_files(self) -> None:
        """Test loading multiple CSV files (comma-separated)."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="strike_wallet",
            strike_csv_files="input/test_strike.csv,input/test_strike_2.csv",
            timezone="UTC",
            native_fiat="USD",
        )

        result = plugin.load(US())

        # test_strike.csv has 5 transactions
        # test_strike_2.csv has 3 transactions (1 Receive, 1 Send, 1 Purchase)
        # Total: 8 transactions
        assert len(result) == 8

    def test_transaction_types(self) -> None:
        """Test that different transaction types are handled correctly."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="strike_wallet",
            strike_csv_files="input/test_strike.csv",
            timezone="UTC",
            native_fiat="USD",
        )

        result = plugin.load(US())

        # Verify we got crypto transactions (not fiat deposits)
        for tx in result:
            assert isinstance(tx, IntraTransaction)
            assert tx.asset == "BTC"
            # All transactions should have either crypto_sent or crypto_received
            assert tx.crypto_sent != Keyword.UNKNOWN.value or tx.crypto_received != Keyword.UNKNOWN.value

    def test_send_includes_fee(self) -> None:
        """Test that Send transactions include fees in the sent amount."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="strike_wallet",
            strike_csv_files="input/test_strike.csv",
            timezone="UTC",
            native_fiat="USD",
        )

        result = plugin.load(US())

        # The Send transactions in test_strike.csv have no fee BTC,
        # so total_sent should equal abs(amount_btc)
        # Let's verify the send transaction from Jan 20 2024 has 0.005 BTC sent
        send_tx = next((t for t in result if t.from_exchange == "strike_wallet"), None)
        assert send_tx is not None
        # The test data has -0.00500000 BTC for send
        assert RP2Decimal(send_tx.crypto_sent) == RP2Decimal("0.005")

    def test_purchase_transaction(self) -> None:
        """Test Purchase transactions receive crypto."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="strike_wallet",
            strike_csv_files="input/test_strike.csv",
            timezone="UTC",
            native_fiat="USD",
        )

        result = plugin.load(US())

        # Find a purchase transaction (should have crypto_received, not crypto_sent)
        purchase_txs = [t for t in result if t.to_exchange == "strike_wallet" and t.from_exchange == Keyword.UNKNOWN.value]
        assert len(purchase_txs) >= 2  # Two purchases in test data