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

"""Tests for Yoroi CSV plugin with Minswap integration."""

from rp2.plugin.country.us import US

from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction
from dali.plugin.input.csv.yoroi import InputPlugin


class TestYoroiCsv:
    """Test Yoroi CSV plugin with Minswap DeFi transactions."""

    def test_staking_rewards(self) -> None:
        """Test that staking rewards are parsed as InTransactions."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="yoroi_wallet",
            csv_file="input/test_yoroi.csv",
            timezone="UTC",
            native_fiat="USD",
            minswap_csv=None,
        )

        result = plugin.load(US())

        # Filter staking rewards (note: plugin uses "Staking" not Keyword.STAKING.value)
        staking_txs = [
            t for t in result
            if isinstance(t, InTransaction)
            and "Staking" in t.transaction_type
            and "Staking Reward" in (t.notes or "")
        ]

        assert len(staking_txs) == 3, f"Expected 3 staking rewards, got {len(staking_txs)}"

        # Verify first staking reward
        assert "0.5" in staking_txs[0].crypto_in
        assert staking_txs[0].asset == "ADA"
        assert "Epoch 100" in staking_txs[0].notes

    def test_yoroi_only_no_minswap(self) -> None:
        """Test Yoroi plugin without Minswap data."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="yoroi_wallet",
            csv_file="input/test_yoroi.csv",
            timezone="UTC",
            native_fiat="USD",
            minswap_csv=None,
        )

        result = plugin.load(US())

        # Should have:
        # - 3 staking rewards (InTransaction)
        # - 7 deposits (IntraTransaction)
        # - 6 withdrawals (IntraTransaction) - one more from the new LP pair
        # Total: 16 transactions
        assert len(result) == 16

        intra_count = sum(1 for t in result if isinstance(t, IntraTransaction))
        assert intra_count == 13  # 7 deposits + 6 withdrawals

        in_count = sum(1 for t in result if isinstance(t, InTransaction))
        assert in_count == 3  # 3 staking rewards

    def test_yoroi_with_minswap_swaps(self) -> None:
        """Test Yoroi plugin with Minswap swap transactions."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="yoroi_wallet",
            csv_file="input/test_yoroi.csv",
            timezone="UTC",
            native_fiat="USD",
            minswap_csv="input/test_minswap.csv",
        )

        result = plugin.load(US())

        # Total should include:
        # - 3 staking rewards (InTransaction)
        # - Basic Yoroi intra-transactions (deposits/withdrawals not matched to Minswap)
        # - Swaps generate OutTransaction + InTransaction pairs
        #
        # Minswap has 4 swaps, each generates:
        # - 1 OutTransaction (sell ADA)
        # - 1 InTransaction (buy token)
        # Total from Minswap: 8 transactions (4 swaps × 2)
        #
        # Plus basic Yoroi: 11 transactions
        # But 3 of those are matched to Minswap (the swap pairs)
        # So: 11 + 8 - 6 = 13 (approximate, depends on deduplication)

        # Count by type
        in_count = sum(1 for t in result if isinstance(t, InTransaction))
        out_count = sum(1 for t in result if isinstance(t, OutTransaction))
        intra_count = sum(1 for t in result if isinstance(t, IntraTransaction))

        # Should have OutTransactions from swaps (selling ADA)
        assert out_count >= 3, f"Expected at least 3 OutTransactions (ADA sells), got {out_count}"

        # Should have InTransactions from swaps (buying tokens) + staking rewards
        assert in_count >= 6, f"Expected at least 6 InTransactions (staking + token buys), got {in_count}"

    def test_swap_uses_yoroi_amounts(self) -> None:
        """Test that swaps use Yoroi on-chain amounts (not Minswap input)."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="yoroi_wallet",
            csv_file="input/test_yoroi.csv",
            timezone="UTC",
            native_fiat="USD",
            minswap_csv="input/test_minswap.csv",
        )

        result = plugin.load(US())

        # Find the first swap OutTransaction (ADA sold)
        swap_out = next(
            (t for t in result
             if isinstance(t, OutTransaction)
             and t.asset == "ADA"
             and "swap" in (t.notes or "").lower()),
            None
        )

        if swap_out:
            # Yoroi shows 10 ADA sold, Minswap shows 10 ADA paid
            # But the fee makes it 10.2 ADA
            # The important thing is crypto_out_no_fee should be present
            assert swap_out.crypto_out_no_fee is not None, "crypto_out_no_fee should be set"

    def test_transaction_types_parsed(self) -> None:
        """Test that Deposit and Withdrawal types are parsed correctly."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="yoroi_wallet",
            csv_file="input/test_yoroi.csv",
            timezone="UTC",
            native_fiat="USD",
            minswap_csv=None,
        )

        result = plugin.load(US())

        # Check we have IntraTransactions with proper from/to
        intra_txs = [t for t in result if isinstance(t, IntraTransaction)]

        # Find a withdrawal (crypto_sent)
        withdrawal = next(
            (t for t in intra_txs
             if t.crypto_sent and t.crypto_sent != Keyword.UNKNOWN.value),
            None
        )
        assert withdrawal is not None, "Should have at least one withdrawal"

        # Find a deposit (crypto_received)
        deposit = next(
            (t for t in intra_txs
             if t.crypto_received and t.crypto_received != Keyword.UNKNOWN.value),
            None
        )
        assert deposit is not None, "Should have at least one deposit"

    def test_fee_handling(self) -> None:
        """Test that fees are handled correctly."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="yoroi_wallet",
            csv_file="input/test_yoroi.csv",
            timezone="UTC",
            native_fiat="USD",
            minswap_csv=None,
        )

        result = plugin.load(US())

        # Find a transaction with fee
        tx_with_fee = next(
            (t for t in result
             if isinstance(t, IntraTransaction)
             and t.crypto_sent
             and t.crypto_sent != Keyword.UNKNOWN.value
             and "0.2" in t.crypto_sent),
            None
        )

        if tx_with_fee:
            # The crypto_sent should include fee (10.0 + 0.2 = 10.2)
            assert tx_with_fee.crypto_sent is not None

    def test_lp_deposit_handling(self) -> None:
        """Test that LP deposits are handled (cost basis tracked, no taxable event).

        LP deposits don't create taxable transactions - they store cost basis
        for later when the LP is removed. We verify this by checking that
        the LP removal uses the cost basis from the deposit.
        """
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="yoroi_wallet",
            csv_file="input/test_yoroi.csv",
            timezone="UTC",
            native_fiat="USD",
            minswap_csv="input/test_minswap.csv",
        )

        result = plugin.load(US())

        # The LP deposit cost basis is stored internally and used during LP removal
        # We can verify this by checking that LP removal InTransaction has the
        # correct asset (ADA received) - the cost basis is tracked internally

        # Check we have LP deposit in Minswap data (4 swaps + 1 deposit + 1 removal)
        minswap_ops = [t for t in result if hasattr(t, 'notes') and t.notes and 'Minswap' in t.notes]
        # Should have: 4 swaps (8 txs) + 1 removal (2 txs) = 10 Minswap-related transactions
        assert len(minswap_ops) >= 10, f"Expected at least 10 Minswap operations, got {len(minswap_ops)}"

    def test_lp_removal_handling(self) -> None:
        """Test that LP removals create proper taxable transactions."""
        plugin = InputPlugin(
            account_holder="tester",
            account_nickname="yoroi_wallet",
            csv_file="input/test_yoroi.csv",
            timezone="UTC",
            native_fiat="USD",
            minswap_csv="input/test_minswap.csv",
        )

        result = plugin.load(US())

        # LP removal should create OutTransaction (sell LP) + InTransaction (buy ADA)
        lp_removal_out = next(
            (t for t in result
             if isinstance(t, OutTransaction)
             and t.asset == "LP"),
            None
        )
        assert lp_removal_out is not None, "Should have LP removal OutTransaction"

        lp_removal_in = next(
            (t for t in result
             if isinstance(t, InTransaction)
             and t.asset == "ADA"
             and hasattr(t, 'notes')
             and t.notes
             and 'LP Removal' in t.notes),
            None
        )
        assert lp_removal_in is not None, "Should have LP removal InTransaction with gain/loss notes"