# Copyright 2025 FrancoETrujillo
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
import logging
import uuid
from csv import reader
from datetime import datetime, timezone
from typing import List, Optional

from rp2.abstract_country import AbstractCountry
from rp2.logger import create_logger

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# IN Crypto.com app CSV Format headers:
# Timestamp (UTC),
# Transaction Description,
# Currency,
# Amount,
# To Currency,
# To Amount,
# Native Currency,
# Native Amount,
# Native Amount (in USD),
# Transaction Kind,
# Transaction Hash

# These are the types from a csv and what I understand so far.
# 1- viban_deposit_precredit: is a deposit from my bank account waiting to be cleared, but available to spend in my USD account
# 2- viban_deposit_precredit_repayment: is the payment of the pre-credit when the deposit is cleared.
# 3- lockup_lock: means that the money is locked and not available to spend on transactions
# 4- lockup_unlock: means money unlocked to be used
# 5- referral_gift: is the sign in bonus for signing to the platform
# 6- crypto_earn_program_created: it is a lock on the money to receive interest later
# 7- viban_purchase: is a crypto buy with fiat
# 8- card_top_up: is a transfer to fund the crypto.com debit card it will convert from crypto to fiat
# 9- reimbursement: is a cashback reward in crypto from buying specific services like Netflix
# 10- reimbursement_reverted: is a cancellation of a previous reward given
# 11- referral_card_cashback: is cashback from using the debit card
# 12- mco_stake_reward: is crypto paid to me from locking into the staking program
# 13- crypto_earn_interest_paid: is crypto paid to me from looking into the earn program
# 14- crypto_purchase: is a crypto buy with fiat
# 15- crypto_exchange: exchange between crypto
# 16- crypto_withdrawal: sending crypto to another wallet
# 17- crypto_viban_exchange: selling crypto to get fiat
# 18- crypto_earn_program_withdrawn: is unlocking money for use
# 19- card_cashback_reverted: is a cancellation of a previous reward given

# IN-transactions describe crypto flowing in (e.g. airdrop, buy, hard fork, income, interest, mining, staking, wages)
_IN_TYPE_MAPPING = {
    "referral_gift": Keyword.INCOME.value,  # Signup bonus or promotions
    "viban_purchase": Keyword.BUY.value,  # Buying crypto with fiat
    "reimbursement": Keyword.INCOME.value,  # Cashback from services like Netflix
    "referral_card_cashback": Keyword.INCOME.value,  # Cashback from using the debit card
    "mco_stake_reward": Keyword.STAKING.value,  # Rewards from staking MCO
    "crypto_earn_interest_paid": Keyword.INTEREST.value,  # Interest paid from the earn program
    "crypto_purchase": Keyword.BUY.value,  # Buying crypto with fiat
}

# OUT-transactions describe crypto flowing out (e.g. donate, fee, gift, sell)
_OUT_TYPE_MAPPING = {
    "crypto_viban_exchange": Keyword.SELL.value,
    "card_top_up": Keyword.SELL.value,
    "reimbursement_reverted": Keyword.SELL.value,
    "card_cashback_reverted": Keyword.SELL.value,
}

# INTRA-transactions describe crypto moving across accounts
_INTRA_MAPPING = {"crypto_withdrawal", Keyword.MOVE.value}

# Reversal transactions that could be removed from the records
_REVERSAL_MAPPING = {"reimbursement_reverted", "card_cashback_reverted"}

# Exchange between currencies to be converted into a sell and buy orders.
_TRANSACTION_MAPPING = {"crypto_exchange"}

# Ignored transactions that do not need to be recorded
_IGNORED_MAPPING = {
    "viban_deposit_precredit",
    "viban_deposit_precredit_repayment",
    "lockup_lock",
    "lockup_unlock",
    "crypto_earn_program_created",
    "crypto_earn_program_withdrawn",
}


class CryptoComAppTransaction:
    def __init__(
        self,
        raw_data: str,
        time: str,
        description: str,
        currency: str,
        amount: str,
        native_currency: str,
        native_amount: str,
        native_amount_usd: str,
        transaction_kind: str,
        to_currency: Optional[str] = None,
        to_amount: Optional[str] = None,
        transaction_hash: Optional[str] = None,
        notes: Optional[str] = None,
    ):
        self.raw_data = raw_data
        self.timestamp = time
        self.abs_float_amount = abs(float(amount))
        self.abs_float_native_amount_usd = abs(float(native_amount_usd))
        self.abs_float_native_amount = abs(float(native_amount))
        self.abs_float_to_amount = abs(float(to_amount)) if to_amount else None

        self.description = description
        self.currency = currency
        self.amount = str(self.abs_float_amount)
        self.to_currency = to_currency
        self.to_amount = str(self.abs_float_to_amount) if to_amount else None
        self.native_currency = native_currency
        self.native_amount = str(self.abs_float_native_amount)
        self.native_amount_usd = str(self.abs_float_native_amount_usd)
        self.transaction_kind = transaction_kind
        self.transaction_hash = transaction_hash
        self.notes = notes

    def __str__(self) -> str:
        return (
            f"CryptoComAppTransaction(timestamp={self.timestamp}, "
            f"description={self.description}, currency={self.currency}, "
            f"amount={self.amount}, transaction_kind={self.transaction_kind})"
        )


class InputPlugin(AbstractInputPlugin):
    __CRYPTO_COM_APP: str = "Crypto.com App"

    __IN_TIME_INDEX = 0
    __IN_T_DESCRIPTION_INDEX = 1
    __IN_CURRENCY_INDEX: int = 2
    __IN_AMOUNT_INDEX: int = 3
    __IN_TO_CURRENCY_INDEX: int = 4
    __IN_TO_AMOUNT_INDEX: int = 5
    __IN_NATIVE_CURRENCY_INDEX: int = 6
    __IN_NATIVE_AMOUNT_INDEX: int = 7
    __IN_NATIVE_AMOUNT_USD_INDEX: int = 8
    __IN_TRANSACTION_KIND_INDEX: int = 9
    __IN_TRANSACTION_HASH_INDEX: int = 10

    __DELIMITER = ","

    def __init__(
        self,
        account_holder: str,
        in_csv_file: Optional[str] = None,
        remove_reverted_transactions: Optional[bool] = False,
        native_fiat: Optional[str] = None,
    ) -> None:
        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__in_csv_file: Optional[str] = in_csv_file
        self.__logger: logging.Logger = create_logger(self.__CRYPTO_COM_APP)
        self.__remove_reverted: bool = False if remove_reverted_transactions is None else remove_reverted_transactions

    def load(self, country: AbstractCountry) -> List[AbstractTransaction]:
        csv_transactions: List[CryptoComAppTransaction] = []

        if not self.__in_csv_file:
            self.__logger.error("No input CSV file specified.")
            raise ValueError("Input CSV file is not specified.")

        with open(self.__in_csv_file, encoding="utf-8") as transaction_csv_file:
            lines = reader(transaction_csv_file)

            # Skip header line
            header = next(lines)
            self.__logger.debug("Header: %s", header)
            transaction_count = 0
            try:
                for line in lines:
                    transaction_count += 1
                    raw_data: str = self.__DELIMITER.join(line)
                    formatted_timestamp = self.format_time(line[self.__IN_TIME_INDEX].strip())
                    transaction_kind = line[self.__IN_TRANSACTION_KIND_INDEX].strip()
                    transaction_description = line[self.__IN_T_DESCRIPTION_INDEX].strip()
                    notes = f"{transaction_kind} <-> {transaction_description}"

                    csv_transactions.append(
                        CryptoComAppTransaction(
                            raw_data=raw_data,
                            time=formatted_timestamp,
                            description=transaction_description,
                            currency=line[self.__IN_CURRENCY_INDEX].strip(),
                            amount=line[self.__IN_AMOUNT_INDEX].strip(),
                            native_currency=line[self.__IN_NATIVE_CURRENCY_INDEX].strip(),
                            native_amount=line[self.__IN_NATIVE_AMOUNT_INDEX].strip(),
                            native_amount_usd=line[self.__IN_NATIVE_AMOUNT_USD_INDEX].strip(),
                            transaction_kind=transaction_kind,
                            to_currency=line[self.__IN_TO_CURRENCY_INDEX].strip() if len(line) > self.__IN_TO_CURRENCY_INDEX else None,
                            to_amount=line[self.__IN_TO_AMOUNT_INDEX].strip() if len(line) > self.__IN_TO_AMOUNT_INDEX else None,
                            transaction_hash=line[self.__IN_TRANSACTION_HASH_INDEX].strip() if len(line) > self.__IN_TRANSACTION_HASH_INDEX else None,
                            notes=notes,
                        )
                    )
            except IndexError as e:
                self.__logger.error("Error reading CSV line. Please check the CSV format.", exc_info=e)
                raise

        result = self.process_csv_transactions(csv_transactions)
        self.__logger.debug("%d transactions read - %d transactions extracted.", transaction_count, len(result))

        return result

    def process_csv_transactions(self, csv_transactions: List[CryptoComAppTransaction]) -> List[AbstractTransaction]:
        """
        Process the CSV transactions and convert them into AbstractTransaction objects.
        """
        result: List[AbstractTransaction] = []
        filtered_transactions = csv_transactions if not self.__remove_reverted else self.remove_reverted_csv_transactions(csv_transactions)
        filtered_transactions = self.remove_ignored_transactions(filtered_transactions)

        for i, transaction in enumerate(filtered_transactions):
            if transaction.transaction_kind in _IN_TYPE_MAPPING:
                result.append(self.handle_in_type_transaction(transaction))
            elif transaction.transaction_kind in _OUT_TYPE_MAPPING:
                result.append(self.handle_out_type_transaction(transaction))
            elif transaction.transaction_kind in _INTRA_MAPPING:
                result.append(self.handle_intra_type_transaction(transaction))
            elif transaction.transaction_kind in _TRANSACTION_MAPPING:
                result.extend(self.handle_exchange_transaction(transaction))
            else:
                self.__logger.info(
                    "Transaction [%d] with kind '%s' is not recognized and will be ignored: %s", i, transaction.transaction_kind, transaction.raw_data
                )

        return result

    def handle_exchange_transaction(self, transaction: CryptoComAppTransaction) -> List[AbstractTransaction]:
        """
        Handle exchange  transactions by splitting them into sell and buy transactions.
        """
        self.__logger.debug("Processing exchange transaction %s", transaction)

        if not transaction.to_currency or not transaction.to_amount or not transaction.abs_float_to_amount:
            self.__logger.warning("Exchange transaction missing 'to_currency' or 'to_amount', skipping: %s", transaction)
            return []
        exchange_transactions_out: List[AbstractTransaction] = []

        try:
            spot_price_sell = str(transaction.abs_float_native_amount_usd / transaction.abs_float_amount)
            spot_price_buy = str(transaction.abs_float_native_amount_usd / transaction.abs_float_to_amount)
        except (ValueError, ZeroDivisionError) as e:
            self.__logger.error("error calculating spot price for exchange transaction", exc_info=e)
            spot_price_buy = Keyword.UNKNOWN.value
            spot_price_sell = Keyword.UNKNOWN.value

        exchange_transactions_out.append(
            OutTransaction(
                plugin=self.__CRYPTO_COM_APP,
                unique_id=f"ex_out_{uuid.uuid4()}",
                raw_data=transaction.raw_data,
                timestamp=transaction.timestamp,
                asset=transaction.currency,
                exchange=self.__CRYPTO_COM_APP,
                holder=self.account_holder,
                transaction_type=Keyword.SELL.value,
                spot_price=spot_price_sell,
                crypto_out_no_fee=transaction.amount,
                crypto_out_with_fee=transaction.amount,
                crypto_fee="0",
                fiat_out_no_fee=transaction.native_amount_usd,
                notes=transaction.notes,
            )
        )

        buy_amount = str(abs(float(transaction.to_amount)))
        exchange_transactions_out.append(
            InTransaction(
                plugin=self.__CRYPTO_COM_APP,
                unique_id=f"ex_in_{uuid.uuid4()}",
                raw_data=transaction.raw_data,
                timestamp=transaction.timestamp,
                asset=transaction.to_currency,
                exchange=self.__CRYPTO_COM_APP,
                holder=self.account_holder,
                transaction_type=Keyword.BUY.value,
                spot_price=spot_price_buy,
                crypto_in=buy_amount,
                fiat_in_with_fee=transaction.native_amount_usd,
                fiat_in_no_fee=transaction.native_amount_usd,
                crypto_fee="0",
                notes=transaction.notes,
            )
        )
        return exchange_transactions_out

    def handle_intra_type_transaction(self, transaction: CryptoComAppTransaction) -> IntraTransaction:
        """
        Handle intra-account transactions (transfers within the same holder accounts)
        """
        self.__logger.debug("Processing intra-account transaction: %s", transaction)

        try:
            spot_price = str(transaction.abs_float_native_amount_usd / transaction.abs_float_amount)
        except (ValueError, ZeroDivisionError) as e:
            self.__logger.error("Error calculating spot price for transaction", exc_info=e)
            spot_price = Keyword.UNKNOWN.value

        return IntraTransaction(
            plugin=self.__CRYPTO_COM_APP,
            unique_id=f"intra_{uuid.uuid4()}",
            raw_data=transaction.raw_data,
            timestamp=transaction.timestamp,
            asset=transaction.currency,
            from_exchange=self.__CRYPTO_COM_APP,
            from_holder=self.account_holder,
            to_exchange=f"{self.account_holder}_External",
            to_holder=self.account_holder,  # Typically same as from_holder
            spot_price=spot_price,
            crypto_sent=transaction.amount,
            crypto_received=transaction.amount,
            notes=transaction.notes,
        )

    def handle_out_type_transaction(self, transaction: CryptoComAppTransaction) -> OutTransaction:
        """
        Handle outgoing transactions (sells, donations, losses, etc.)
        """

        self.__logger.debug("Processing out transaction %s", transaction)
        transaction_type = _OUT_TYPE_MAPPING[transaction.transaction_kind]
        try:
            spot_price = str(transaction.abs_float_native_amount_usd / transaction.abs_float_amount)
        except (ValueError, ZeroDivisionError) as e:
            self.__logger.error("Error calculating spot price for transaction", exc_info=e)
            spot_price = Keyword.UNKNOWN.value

        return OutTransaction(
            plugin=self.__CRYPTO_COM_APP,
            unique_id=f"out_{uuid.uuid4()}",
            raw_data=transaction.raw_data,
            timestamp=transaction.timestamp,
            asset=transaction.currency,
            exchange=self.__CRYPTO_COM_APP,
            holder=self.account_holder,
            transaction_type=transaction_type,
            spot_price=spot_price,
            crypto_out_no_fee=transaction.amount,
            crypto_fee="0",
            notes=transaction.notes,
            fiat_out_no_fee=transaction.native_amount_usd,
            crypto_out_with_fee=transaction.amount,
        )

    def handle_in_type_transaction(self, transaction: CryptoComAppTransaction) -> InTransaction:
        """
        Handle incoming transactions (buys, income, interest, staking, etc.)
        """

        self.__logger.debug("Processing in transaction %s", transaction)
        if transaction.to_currency and transaction.to_amount:
            # If there is a to_currency, we assume is a purchase from usd to the currency
            in_currency = transaction.to_currency
            crypto_in = transaction.to_amount
        else:
            in_currency = transaction.currency
            crypto_in = transaction.amount

        transaction_type = _IN_TYPE_MAPPING[transaction.transaction_kind]
        try:
            amount = float(crypto_in)
            native_amount = float(transaction.native_amount_usd)
            spot_price = str(native_amount / amount)
        except (ValueError, ZeroDivisionError) as e:
            self.__logger.error("Error calculating spot price for transaction", exc_info=e)
            spot_price = Keyword.UNKNOWN.value

        crypto_in = str(abs(float(crypto_in)))
        native_amount_usd = str(abs(float(transaction.native_amount_usd)))

        return InTransaction(
            plugin=self.__CRYPTO_COM_APP,
            unique_id=f"in_{uuid.uuid4()}",
            raw_data=transaction.raw_data,
            timestamp=transaction.timestamp,
            asset=in_currency,
            exchange=self.__CRYPTO_COM_APP,
            holder=self.account_holder,
            transaction_type=transaction_type,
            spot_price=spot_price,
            crypto_in=crypto_in,
            fiat_in_with_fee=native_amount_usd,
            fiat_in_no_fee=native_amount_usd,
            crypto_fee="0",
            notes=transaction.notes,
        )

    def format_time(self, time: str) -> str:
        """
        Convert time from "MM/DD/YYYY HH:MM" format to ISO 8601 format with UTC timezone.
        """
        try:
            dt = datetime.strptime(time, "%m/%d/%Y %H:%M")
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError as e:
            self.__logger.error("Error parsing time: %s", exc_info=e)
            return time

    def remove_ignored_transactions(self, csv_transactions: List[CryptoComAppTransaction]) -> List[CryptoComAppTransaction]:
        """
        Remove not relevant transactions based on the ignored mapping.
        """
        self.__logger.debug("Removing ignored transactions...")
        filtered_transactions = []
        for transaction in csv_transactions:
            if transaction.transaction_kind not in _IGNORED_MAPPING:
                filtered_transactions.append(transaction)
            else:
                self.__logger.debug("Ignored transaction: %s", transaction)

        self.__logger.debug(
            "Removed ignored transactions: %d ignored out of %d total transactions.", len(csv_transactions) - len(filtered_transactions), len(csv_transactions)
        )
        return filtered_transactions

    def remove_reverted_csv_transactions(self, csv_transactions: List[CryptoComAppTransaction]) -> List[CryptoComAppTransaction]:
        """
        Remove transactions that are reversals and their corresponding original transactions.
        """
        self.__logger.debug("Removing reverted transactions...")
        reversals_indices: List[int] = []
        transaction_indices_to_remove: List[int] = []

        for i, transaction in enumerate(csv_transactions):
            if transaction.transaction_kind in _REVERSAL_MAPPING:
                reversals_indices.append(i)

        for reversal_index in reversals_indices:
            reversal_transaction = csv_transactions[reversal_index]
            for i, transaction in enumerate(csv_transactions):
                if (
                    i != reversal_index
                    and transaction.currency == reversal_transaction.currency
                    and abs(float(transaction.amount)) == abs(float(reversal_transaction.amount))
                    and transaction.currency == reversal_transaction.currency
                ):
                    self.__logger.debug("transaction %s is a reversal of transaction %s", transaction, reversal_transaction)
                    transaction_indices_to_remove.append(i)
                    transaction_indices_to_remove.append(reversal_index)

        self.__logger.debug("reversals found: %d, removing %d transactions", len(reversals_indices), len(transaction_indices_to_remove))
        csv_transactions = [transaction for i, transaction in enumerate(csv_transactions) if i not in transaction_indices_to_remove]
        return csv_transactions
