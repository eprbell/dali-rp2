import math

from rp2.plugin.country.us import US
from rp2.rp2_decimal import RP2Decimal

from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction
from dali.plugin.input.csv.crypto_com_app import CryptoComAppTransaction, InputPlugin


class TestCryptoComApp:
    def test_load_transactions_no_remove_reverted(self) -> None:
        plugin = InputPlugin(account_holder="tester", in_csv_file="input/test_crypto_com_app.csv", native_fiat="USD", remove_reverted_transactions=False)

        result = plugin.load(US())
        assert len(result) == 37

    def test_load_transactions_remove_reverted(self) -> None:
        plugin = InputPlugin(account_holder="tester", in_csv_file="input/test_crypto_com_app.csv", native_fiat="USD", remove_reverted_transactions=True)

        result = plugin.load(US())
        assert len(result) == 35

    def test_handle_exchange_transaction(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            in_csv_file="input/test_crypto_com_app.csv",
            native_fiat="USD",
        )

        transaction = CryptoComAppTransaction(
            raw_data="6/15/2022 21:58,CRO -> ETH,CRO,-259.8504164,ETH,0.025,USD,30.8912303,30.8912303,crypto_exchange",
            time=plugin.format_time("6/15/2022 21:58"),
            description="CRO -> ETH",
            currency="CRO",
            amount="-259.8504164",
            native_currency="USD",
            native_amount="30.8912303",
            native_amount_usd="30.8912303",
            transaction_kind="crypto_exchange",
            to_currency="ETH",
            to_amount="0.025",
        )

        result = plugin.handle_exchange_transaction(transaction)

        assert len(result) == 2

        out_transaction = result[0]
        in_transaction = result[1]

        assert isinstance(out_transaction, OutTransaction)
        assert out_transaction.asset == "CRO"
        assert out_transaction.timestamp == "2022-06-15 21:58:00+0000"
        assert out_transaction.transaction_type.lower() == Keyword.SELL.value.lower()
        assert out_transaction.spot_price == "0.11888081892640755"
        assert out_transaction.crypto_out_no_fee is not None
        assert out_transaction.crypto_out_with_fee is not None
        assert RP2Decimal(out_transaction.crypto_out_no_fee) == RP2Decimal("259.8504164")
        assert RP2Decimal(out_transaction.crypto_out_with_fee) == RP2Decimal("259.8504164")
        assert out_transaction.crypto_fee == "0"
        assert out_transaction.fiat_out_no_fee == "30.8912303"

        assert isinstance(in_transaction, InTransaction)
        assert in_transaction.asset == "ETH"
        assert in_transaction.timestamp == "2022-06-15 21:58:00+0000"
        assert in_transaction.transaction_type.lower() == Keyword.BUY.value.lower()
        assert in_transaction.spot_price == "1235.649212"
        assert RP2Decimal(in_transaction.crypto_in) == RP2Decimal("0.025")
        assert in_transaction.fiat_in_with_fee == "30.8912303"
        assert in_transaction.fiat_in_no_fee == "30.8912303"
        assert in_transaction.crypto_fee == "0"

    def test_handle_intra_transaction(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            in_csv_file="input/test_crypto_com_app.csv",
            native_fiat="USD",
        )

        transaction = CryptoComAppTransaction(
            raw_data="11/21/2022 22:08,Withdraw ETH (ERC20),ETH,-0.3877,USD,421.5213882,USD,421.5213882,"
            "crypto_withdrawal,0x1646a7c9f57f5f543c498f5ebff14dab16f9c9b09dbfb50405622564b8c62762",
            time=plugin.format_time("11/21/2022 22:08"),
            description="Withdraw ETH (ERC20)",
            currency="ETH",
            amount="-0.3877",
            native_currency="USD",
            native_amount="421.5213882",
            native_amount_usd="421.5213882",
            transaction_kind="crypto_withdrawal",
        )

        result = plugin.handle_intra_type_transaction(transaction)

        assert isinstance(result, IntraTransaction)
        assert result.asset == "ETH"
        assert result.timestamp == "2022-11-21 22:08:00+0000"
        assert result.from_exchange == "Crypto.com App"
        assert result.to_exchange == "tester_External"
        assert result.from_holder == "tester"
        assert result.to_holder == "tester"
        assert math.isclose(float(result.crypto_sent), float(transaction.amount), rel_tol=1e-9)
        assert RP2Decimal(result.crypto_sent) == RP2Decimal("0.3877")
        assert RP2Decimal(result.crypto_received) == RP2Decimal("0.3877")

    def test_handle_in_transaction(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            in_csv_file="input/test_crypto_com_app.csv",
            native_fiat="USD",
        )

        transaction = CryptoComAppTransaction(
            raw_data="3/31/2022 0:03,Sign-up Bonus Unlocked,CRO,61.73662329,USD,25,USD,25,25,referral_gift",
            time=plugin.format_time("3/31/2022 0:03"),
            description="Sign-up Bonus Unlocked",
            currency="CRO",
            amount="61.73662329",
            native_currency="USD",
            native_amount="25",
            native_amount_usd="25",
            transaction_kind="referral_gift",
        )

        result = plugin.handle_in_type_transaction(transaction)

        assert isinstance(result, InTransaction)
        assert result.asset == "CRO"
        assert result.timestamp == "2022-03-31 00:03:00+0000"
        assert result.transaction_type.lower() == Keyword.INCOME.value.lower()
        assert math.isclose(float(result.crypto_in), float(transaction.amount), rel_tol=1e-9)
        assert RP2Decimal(result.crypto_in) == RP2Decimal("61.73662329")
        assert result.fiat_in_with_fee == "25.0"
        assert result.fiat_in_no_fee == "25.0"
        assert result.crypto_fee == "0"

    def test_handle_out_transaction(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            in_csv_file="input/test_crypto_com_app.csv",
            native_fiat="USD",
        )

        transaction = CryptoComAppTransaction(
            raw_data="2/29/2024 6:42,BTC -> USD,BTC,-0.0259378,USD,1617.26,USD,1617.26,1617.26,crypto_viban_exchange",
            time=plugin.format_time("2/29/2024 6:42"),
            description="BTC -> USD",
            currency="BTC",
            amount="-0.0259378",
            native_currency="USD",
            native_amount="1617.26",
            native_amount_usd="1617.26",
            transaction_kind="crypto_viban_exchange",
        )

        result = plugin.handle_out_type_transaction(transaction)

        assert isinstance(result, OutTransaction)
        assert result.asset == "BTC"
        assert result.timestamp == "2024-02-29 06:42:00+0000"
        assert result.transaction_type.lower() == Keyword.SELL.value.lower()
        assert math.isclose(float(result.crypto_out_no_fee), float(transaction.amount), rel_tol=1e-9)
        assert RP2Decimal(result.crypto_out_no_fee) == RP2Decimal("0.0259378")
        assert result.fiat_out_no_fee == "1617.26"
        assert result.crypto_fee == "0"
        assert result.crypto_out_with_fee == "0.0259378"

    def test_remove_ignored_transactions(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            in_csv_file="input/test_crypto_com_app.csv",
            native_fiat="USD",
        )

        transactions = [
            CryptoComAppTransaction(
                raw_data="3/31/2022 15:41,Instant Buy Precredit,USD,5000,USD,5000,USD,5000,5000,viban_deposit_precredit",
                time=plugin.format_time("3/31/2022 15:41"),
                description="Instant Buy Precredit",
                currency="USD",
                amount="5000",
                native_currency="USD",
                native_amount="5000",
                native_amount_usd="5000",
                transaction_kind="viban_deposit_precredit",
            ),
            CryptoComAppTransaction(
                raw_data="3/31/2022 0:03,Sign-up Bonus Unlocked,CRO,61.73662329,USD,25,USD,25,25,referral_gift",
                time=plugin.format_time("3/31/2022 0:03"),
                description="Sign-up Bonus Unlocked",
                currency="CRO",
                amount="61.73662329",
                native_currency="USD",
                native_amount="25",
                native_amount_usd="25",
                transaction_kind="referral_gift",
            ),
        ]

        result = plugin.remove_ignored_transactions(transactions)
        assert len(result) == 1
        assert result[0].transaction_kind == "referral_gift"

    def test_revert_reverted_transactions(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            in_csv_file="input/test_crypto_com_app.csv",
            native_fiat="USD",
        )

        transactions = [
            CryptoComAppTransaction(
                raw_data="4/8/2022 1:16,CRO Rewards,CRO,2.38339735,USD,1.16,USD,1.16,1.16,referral_card_cashback",
                time=plugin.format_time("4/8/2022 1:16"),
                description="CRO Rewards",
                currency="CRO",
                amount="-2.38339735",
                native_currency="USD",
                native_amount="1.16",
                native_amount_usd="1.16",
                transaction_kind="card_cashback_reverted",
            ),
            CryptoComAppTransaction(
                raw_data="4/5/2022 1:16,CRO Rewards,CRO,2.38339735,USD,1.16,USD,1.16,1.16,referral_card_cashback",
                time=plugin.format_time("4/5/2022 1:16"),
                description="CRO Rewards",
                currency="CRO",
                amount="2.38339735",
                native_currency="USD",
                native_amount="1.16",
                native_amount_usd="1.16",
                transaction_kind="referral_card_cashback",
            ),
        ]

        result = plugin.remove_reverted_csv_transactions(transactions)
        assert len(result) == 0

    def test_date_conversion(self) -> None:
        # Test the date conversion from string to datetime
        in_date_str = "2/29/2024 6:42"
        expected_date = "2024-02-29T06:42:00+00:00"

        plugin = InputPlugin(account_holder="tester", in_csv_file="input/test_crypto_com_app.csv", native_fiat="USD", remove_reverted_transactions=False)
        plugin.load(US())

        converted_date = plugin.format_time(in_date_str)
        assert expected_date == converted_date
