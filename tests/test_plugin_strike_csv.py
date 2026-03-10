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
from dali.in_transaction import InTransaction
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
        # - 2 Purchase (2 InTransaction)
        # - 2 Send (2 IntraTransaction)
        # - 1 Receive (1 IntraTransaction)
        # Total: 5 transactions
        assert len(result) == 5

        # Verify we have the right types
        intra_count = sum(1 for t in result if isinstance(t, IntraTransaction))
        assert intra_count == 3  # 2 Send + 1 Receive

        in_tx_count = sum(1 for t in result if isinstance(t, InTransaction))
        assert in_tx_count == 2  # 2 Purchase (BTC in)

        # Check a Receive transaction (IntraTransaction)
        receive_tx = next((t for t in result if isinstance(t, IntraTransaction) and t.crypto_received and "0.0035" in t.crypto_received), None)
        assert receive_tx is not None
        assert receive_tx.asset == "BTC"
        assert receive_tx.plugin == "Strike"

        # Check a Purchase InTransaction (BTC received)
        purchase_in = next((t for t in result if isinstance(t, InTransaction) and "0.0215" in t.crypto_in), None)
        assert purchase_in is not None
        assert purchase_in.asset == "BTC"
        assert purchase_in.transaction_type == "Buy"

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

        # test_strike.csv has 7 transactions (as above)
        # test_strike_2.csv has transactions - check based on actual data
        # Just verify we have transactions from both files
        assert len(result) > 7

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

        # Verify we got all transaction types
        # IntraTransactions: Send, Receive
        intra_txs = [t for t in result if isinstance(t, IntraTransaction)]
        assert len(intra_txs) == 3

        # InTransactions: Purchase (crypto received)
        in_txs = [t for t in result if isinstance(t, InTransaction)]
        assert len(in_txs) == 2
        for tx in in_txs:
            assert tx.asset == "BTC"
            assert tx.transaction_type == "Buy"

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

        # Find Send transactions (IntraTransaction where from_exchange = strike_wallet)
        send_tx = next((t for t in result if isinstance(t, IntraTransaction) and t.from_exchange == "strike_wallet"), None)
        assert send_tx is not None
        # The test data has -0.00500000 BTC for send
        assert RP2Decimal(send_tx.crypto_sent) == RP2Decimal("0.005")

    def test_purchase_transaction(self) -> None:
        """Test Purchase transactions create InTransaction (crypto in)."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="strike_wallet",
            strike_csv_files="input/test_strike.csv",
            timezone="UTC",
            native_fiat="USD",
        )

        result = plugin.load(US())

        # Find purchase InTransaction (BTC in)
        purchase_in = [t for t in result if isinstance(t, InTransaction) and t.transaction_type == "Buy"]
        assert len(purchase_in) >= 2  # Two purchases in test data

        # Verify the first purchase has correct values
        first_purchase_in = purchase_in[0]
        assert first_purchase_in.asset == "BTC"
        assert "0.0215" in first_purchase_in.crypto_in
        assert first_purchase_in.spot_price == "65000.00"

    def test_unique_id_uses_transaction_hash(self) -> None:
        """Test that unique_id uses transaction hash when present."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="strike_wallet",
            strike_csv_files="input/test_strike.csv",
            timezone="UTC",
            native_fiat="USD",
        )

        result = plugin.load(US())

        # Find a transaction with transaction hash (Send has hash in test data)
        send_tx = next((t for t in result if isinstance(t, IntraTransaction) and t.from_exchange == "strike_wallet"), None)
        assert send_tx is not None
        # The Send transaction has a transaction hash in the CSV
        assert "a" in send_tx.unique_id  # Hash starts with 'a'
        # Should NOT be the transaction ID (which starts with 'aaaaaaaa-bbbb')
        assert not send_tx.unique_id.startswith("aaaaaaaa-bbbb")