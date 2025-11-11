# Copyright 2022 eprbell, 2025 randumari
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

# Coinbase REST plugin links:
# REST API: https://developers.coinbase.com/api/v2, https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api
# Authentication: https://developers.coinbase.com/docs/wallet/api-key-authentication
# Endpoint: https://api.coinbase.com
# Coinbase Python Client: https://github.com/coinbase/coinbase-advanced-py

import hashlib
import hmac
import json
import logging
import os
import pickle
import time
from multiprocessing.pool import ThreadPool
from typing import Any, Dict, List, NamedTuple, Optional, cast

import requests
import requests.exceptions
from requests import PreparedRequest
from requests.auth import AuthBase
from requests.models import Response
from requests.sessions import Session
from rp2.abstract_country import AbstractCountry
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError
from coinbase.rest import RESTClient

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# Native format keywords
_ADDRESS: str = "address"
_ADVANCED_TRADE_FILL: str = "advanced_trade_fill"
_AMOUNT: str = "amount"
_BALANCE: str = "balance"
_BUY: str = "buy"
_CARDBUYBACK: str = "cardbuyback"
_CARDSPEND: str = "cardspend"
_CODE: str = "code"
_COMMISSION: str = "commission"
_CREATED_AT: str = "created_at"
_CURRENCY: str = "currency"
_DESCRIPTION: str = "description"
_DETAILS: str = "details"
_EMAIL: str = "email"
_EXCHANGE_DEPOSIT: str = "exchange_deposit"
_EXCHANGE_WITHDRAWAL: str = "exchange_withdrawal"
_FEE: str = "fee"
_FIAT_DEPOSIT: str = "fiat_deposit"
_FIAT_WITHDRAWAL: str = "fiat_withdrawal"
_FILL_PRICE: str = "fill_price"
_FROM: str = "from"
_HASH: str = "hash"
_ID: str = "id"
_ID_SUFFIX_IN: str = "_in"
_ID_SUFFIX_OUT: str = "_out"
_INTEREST: str = "interest"
_INFLATION_REWARD: str = "inflation_reward"
_NATIVE_AMOUNT: str = "native_amount"
_NETWORK: str = "network"
_NETWORK_NAME: str = "network_name"
_OFF_BLOCKCHAIN: str = "off_blockchain"
_PRIME_WITHDRAWAL: str = "prime_withdrawal"
_PRO_DEPOSIT: str = "pro_deposit"
_PRO_WITHDRAWAL: str = "pro_withdrawal"
_PRODUCT_ID: str = "product_id"
_RESOURCE: str = "resource"
_SELL: str = "sell"
_SEND: str = "send"
_STAKING_REWARD: str = "staking_reward"
_STAKING_TRANSFER: str = "staking_transfer"
_STATUS: str = "status"
_SUBTITLE: str = "subtitle"
_TITLE: str = "title"
_TRANSACTION_FEE: str = "transaction_fee"
_TO: str = "to"
_TRADE: str = "trade"
_TYPE: str = "type"
_UNIT_PRICE: str = "unit_price"
_UPDATED_AT: str = "updated_at"
_USER: str = "user"
_STABLECOIN_INTERMEDIARIES: List[str] = ["USDC"]

class _ProcessAccountResult(NamedTuple):
    in_transactions: List[InTransaction]
    out_transactions: List[OutTransaction]
    intra_transactions: List[IntraTransaction]
    in_transaction_2_trade_id: Dict[InTransaction, str]
    trade_id_2_out_transaction: Dict[str, OutTransaction]
    out_transaction_2_trade_id: Dict[OutTransaction, str]
    trade_id_2_in_transaction: Dict[str, InTransaction]


class _InTransactionAndIndex(NamedTuple):
    in_transaction: InTransaction
    in_transaction_index: int


class _OutTransactionAndIndex(NamedTuple):
    out_transaction: OutTransaction
    out_transaction_index: int


class _SwapPair(NamedTuple):
    in_transaction: Optional[_InTransactionAndIndex]
    out_transaction: Optional[_OutTransactionAndIndex]


class _CoinbaseAuth(AuthBase):
    __API_VERSION: str = "2017-11-27"

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.__api_key: str = api_key
        self.__api_secret: str = api_secret.replace('\\n', "\n")
        self.__client: RESTClient = RESTClient(api_key=api_key, api_secret=api_secret)

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        timestamp: str = str(int(time.time()))
        message: str = f"{timestamp}{request.method}{request.path_url}{(request.body or '')}"  # type: ignore
        secret: str = self.__api_secret
        signature: str = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()
        request.headers.update(
            {
                "CB-VERSION": self.__API_VERSION,
                "CB-ACCESS-KEY": self.__api_key,
                "CB-ACCESS-SIGN": signature,
                "CB-ACCESS-TIMESTAMP": timestamp,
            }
        )
        return request

    def _request(self, method: str, endpoint: str, params: Any, data: Any, timeout: int):
        if not method or method.lower() != "get":
            raise NotImplementedError("non-get request methods have not been implemented.")
        return self.__client.get(endpoint, **params, data=data, timeout=timeout)
    
    def _reauthenticate(self):
        self.__client = RESTClient(api_key=self.__api_key, api_secret=self.__api_secret)
        

class InputPlugin(AbstractInputPlugin):
    __API_URL: str = "https://api.coinbase.com"

    # Coinbase returns very low precision fiat data (only 2 decimal digits): if the value is less than 1c Coinbase
    # rounds it to zero, which causes various computation problems (spot_price, etc.). As a workaround, when this
    # condition is detected the plugin sets affected fields to UNKNOWN or None (depending on their nature), so that
    # they can be filled later by the transaction resolver and RP2.
    __MINIMUM_FIAT_PRECISION: RP2Decimal = RP2Decimal("0.01")
    __TIMEOUT: int = 30

    __COINBASE: str = "Coinbase"
    __COINBASE_PRO: str = "Coinbase Pro"

    def __init__(
        self,
        account_holder: str,
        api_key: str,
        api_secret: str,
        native_fiat: Optional[str] = None,
        pickle_api_cache_enabled: Optional[str] = None
    ) -> None:
        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__api_url: str = InputPlugin.__API_URL
        self.__auth: _CoinbaseAuth = _CoinbaseAuth(api_key, api_secret)
        self.__logger: logging.Logger = create_logger(f"{self.__COINBASE}/{self.account_holder}")
        self.__logger.setLevel(logging.DEBUG)
        self.__cache_key: str = f"{self.__COINBASE.lower()}-{account_holder}"
        self.__pickle_api_cache_enabled = pickle_api_cache_enabled != None and pickle_api_cache_enabled != "0" and pickle_api_cache_enabled.lower() != "false" 

    def cache_key(self) -> Optional[str]:
        return self.__cache_key

    def load(self, country: AbstractCountry) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []
        in_transaction_2_trade_id: Dict[InTransaction, str] = {}
        trade_id_2_out_transaction: Dict[str, OutTransaction] = {}
        out_transaction_2_trade_id: Dict[OutTransaction, str] = {}
        trade_id_2_in_transaction: Dict[str, InTransaction] = {}
        process_account_result_list: List[Optional[_ProcessAccountResult]] = []

        accounts = []
        
        if self.__pickle_api_cache_enabled and os.path.isfile("accounts.pkl"):
            with open("accounts.pkl", "rb") as file:
                accounts = pickle.load(file)
        else:
            accounts = list(self.__get_accounts())
            if self.__pickle_api_cache_enabled:
                with open("accounts.pkl", "wb") as file:
                    pickle.dump(accounts, file)
            
        for account in accounts:
            process_account_result_list.append(self._process_account(account))
            time.sleep(1) # backoff due to Coinbase rejecting authorization if too quick
        
        for process_account_result in process_account_result_list:
            if process_account_result is None:
                continue
            if process_account_result.in_transactions:
                result.extend(process_account_result.in_transactions)
            if process_account_result.out_transactions:
                result.extend(process_account_result.out_transactions)
            if process_account_result.intra_transactions:
                result.extend(process_account_result.intra_transactions)
            if process_account_result.in_transaction_2_trade_id:
                in_transaction_2_trade_id.update(process_account_result.in_transaction_2_trade_id)
            if process_account_result.trade_id_2_out_transaction:
                trade_id_2_out_transaction.update(process_account_result.trade_id_2_out_transaction)
            if process_account_result.out_transaction_2_trade_id:
                out_transaction_2_trade_id.update(process_account_result.out_transaction_2_trade_id)
            if process_account_result.trade_id_2_in_transaction:
                trade_id_2_in_transaction.update(process_account_result.trade_id_2_in_transaction)

        self._postprocess_swaps(
            transaction_list=result,
            in_transaction_2_trade_id=in_transaction_2_trade_id,
            trade_id_2_out_transaction=trade_id_2_out_transaction,
            out_transaction_2_trade_id=out_transaction_2_trade_id,
            trade_id_2_in_transaction=trade_id_2_in_transaction,
        )

        return result
        
    def __transaction_details_or_default(self, transaction: Dict, sub_value: str):
        return transaction[_DETAILS][sub_value] if _DETAILS in transaction.keys() and sub_value in transaction[_DETAILS].keys() else ''
    
    def __transaction_description_or_default(self, transaction: Dict):
        return transaction[_DESCRIPTION] if _DESCRIPTION in transaction.keys() else ''

    @staticmethod
    def _is_buy_side_of_swap(
        transaction: AbstractTransaction,
        in_transaction_2_trade_id: Dict[InTransaction, str],
        trade_id_2_out_transaction: Dict[str, OutTransaction],
    ) -> bool:
        return (
            isinstance(transaction, InTransaction)
            and transaction in in_transaction_2_trade_id
            and in_transaction_2_trade_id[transaction] in trade_id_2_out_transaction
        )

    @staticmethod
    def _is_sell_side_of_swap(
        transaction: AbstractTransaction,
        out_transaction_2_trade_id: Dict[OutTransaction, str],
        trade_id_2_in_transaction: Dict[str, InTransaction],
    ) -> bool:
        return (
            isinstance(transaction, OutTransaction)
            and transaction in out_transaction_2_trade_id
            and out_transaction_2_trade_id[transaction] in trade_id_2_in_transaction
        )

    # Crypto swap postprocessing (update fiat fields, notes, etc.)
    def _postprocess_swaps(
        self,
        transaction_list: List[AbstractTransaction],
        in_transaction_2_trade_id: Dict[InTransaction, str],
        trade_id_2_out_transaction: Dict[str, OutTransaction],
        out_transaction_2_trade_id: Dict[OutTransaction, str],
        trade_id_2_in_transaction: Dict[str, InTransaction],
    ) -> None:
        trade_id_2_swap_pair: Dict[str, _SwapPair] = {}
        in_transaction: InTransaction
        out_transaction: OutTransaction
        trade_id: str

        # Collect both sides of the swap
        for index, transaction in enumerate(transaction_list):
            swap_pair: _SwapPair
            if self._is_buy_side_of_swap(transaction, in_transaction_2_trade_id, trade_id_2_out_transaction):
                in_transaction = cast(InTransaction, transaction)
                trade_id = in_transaction_2_trade_id[in_transaction]
                out_transaction = trade_id_2_out_transaction[trade_id]
                if in_transaction.asset == out_transaction.asset:
                    raise RP2RuntimeError(
                        f"Internal error: detected a crypto swap with same asset ({in_transaction.asset}): {in_transaction} // {out_transaction}"
                    )
                in_transaction_and_index: _InTransactionAndIndex = _InTransactionAndIndex(
                    in_transaction=in_transaction,
                    in_transaction_index=index,
                )
                if trade_id in trade_id_2_swap_pair:
                    swap_pair = trade_id_2_swap_pair[trade_id]
                    swap_pair = _SwapPair(
                        in_transaction=in_transaction_and_index,
                        out_transaction=swap_pair.out_transaction,
                    )
                else:
                    swap_pair = _SwapPair(
                        in_transaction=in_transaction_and_index,
                        out_transaction=None,
                    )
            elif self._is_sell_side_of_swap(transaction, out_transaction_2_trade_id, trade_id_2_in_transaction):
                out_transaction = cast(OutTransaction, transaction)
                trade_id = out_transaction_2_trade_id[out_transaction]
                in_transaction = trade_id_2_in_transaction[trade_id]
                if in_transaction.asset == out_transaction.asset:
                    raise RP2RuntimeError(
                        f"Internal error: detected a crypto swap with same asset ({in_transaction.asset}): {in_transaction} // {out_transaction}"
                    )
                out_transaction_and_index: _OutTransactionAndIndex = _OutTransactionAndIndex(
                    out_transaction=out_transaction,
                    out_transaction_index=index,
                )
                if trade_id in trade_id_2_swap_pair:
                    swap_pair = trade_id_2_swap_pair[trade_id]
                    swap_pair = _SwapPair(
                        in_transaction=swap_pair.in_transaction,
                        out_transaction=out_transaction_and_index,
                    )
                else:
                    swap_pair = _SwapPair(
                        in_transaction=None,
                        out_transaction=out_transaction_and_index,
                    )
            else:
                # Not part of a swap
                continue

            trade_id_2_swap_pair[trade_id] = swap_pair

        # Process swaps
        for trade_id, swap_pair in trade_id_2_swap_pair.items():
            if swap_pair.in_transaction is None or swap_pair.out_transaction is None:
                raise RP2RuntimeError(f"Internal error: unmatched swap pair: {swap_pair}")
            in_transaction = swap_pair.in_transaction.in_transaction
            out_transaction = swap_pair.out_transaction.out_transaction

            # Ensure fees are not yet computed
            if in_transaction.crypto_fee is not None or in_transaction.fiat_fee is None or RP2Decimal(in_transaction.fiat_fee) != ZERO:
                raise RP2RuntimeError(f"Internal error: in-transaction crypto_fee is not None or fiat_fee != 0: {in_transaction}")
            if (
                out_transaction.crypto_fee is None
                or RP2Decimal(out_transaction.crypto_fee) != ZERO
                or out_transaction.fiat_fee is None
                or RP2Decimal(out_transaction.fiat_fee) != ZERO
            ):
                raise RP2RuntimeError(f"Internal error: out-transaction crypto_fee != 0 or fiat_fee != 0: {out_transaction}")

            fiat_out_no_fee: RP2Decimal
            # The fiat_fee is paid in the InTransaction, unless the InTransaction is fiat (which is ignored in RP2):
            # in this case the fiat_fee is paid in the OutTransaction
            if not self.is_native_fiat(in_transaction.asset):
                if not in_transaction.fiat_in_no_fee or not in_transaction.fiat_in_with_fee or not out_transaction.fiat_out_no_fee:
                    raise RP2RuntimeError(f"Internal error: swap transactions have incomplete fiat data: {in_transaction}//{out_transaction}")
                fiat_in_no_fee: RP2Decimal = RP2Decimal(in_transaction.fiat_in_no_fee)
                fiat_in_with_fee: RP2Decimal = RP2Decimal(in_transaction.fiat_in_with_fee)
                fiat_out_no_fee = RP2Decimal(out_transaction.fiat_out_no_fee)
                fiat_in_fee: RP2Decimal = fiat_out_no_fee - fiat_in_no_fee
                if fiat_in_with_fee > fiat_out_no_fee:
                    # Sometimes Coinbase produces conversions with fiat_in_with_fee > fiat_out_no_fee
                    # (e.g. see https://github.com/eprbell/dali-rp2/issues/34). This is clearly incorrect
                    # but the Coinbase API sometimes provides imprecise fiat data (see also
                    # https://github.com/eprbell/dali-rp2/issues/20 for more on this problem).
                    self.__logger.warning(
                        "Coinbase returned imprecise data: fiat_in_with_fee > fiat_out_no_fee: artificially setting fiat fee to 0: %s // %s",
                        str(in_transaction),
                        str(out_transaction),
                    )
                    fiat_in_fee = ZERO  # Set fiat_fee to ZERO, otherwise it would be negative

                # crypto_fee is always None (see swap logic in _process_fill()). Update notes and fiat_fee-related fields
                transaction_list[swap_pair.in_transaction.in_transaction_index] = InTransaction(
                    plugin=self.__COINBASE,
                    unique_id=in_transaction.unique_id,
                    raw_data=in_transaction.raw_data,
                    timestamp=in_transaction.timestamp,
                    asset=in_transaction.asset,
                    exchange=in_transaction.exchange,
                    holder=in_transaction.holder,
                    transaction_type=in_transaction.transaction_type,
                    spot_price=in_transaction.spot_price,
                    crypto_in=in_transaction.crypto_in,
                    crypto_fee=in_transaction.crypto_fee,
                    fiat_in_no_fee=in_transaction.fiat_in_no_fee,
                    fiat_in_with_fee=str(fiat_out_no_fee),
                    fiat_fee=str(fiat_in_fee),
                    notes=(
                        f"{in_transaction.notes + '; ' if in_transaction.notes else ''} Buy side of conversion from "
                        f"{out_transaction.crypto_out_with_fee} {out_transaction.asset} -> "
                        f"{in_transaction.crypto_in} {in_transaction.asset} "
                        f"({out_transaction.asset} out-transaction unique id: {out_transaction.unique_id})"
                    ),
                )
                # crypto_fee and fiat_fee are always 0 (see swap logic in _process_fill()). Update notes
                transaction_list[swap_pair.out_transaction.out_transaction_index] = OutTransaction(
                    plugin=self.__COINBASE,
                    unique_id=out_transaction.unique_id,
                    raw_data=out_transaction.raw_data,
                    timestamp=out_transaction.timestamp,
                    asset=out_transaction.asset,
                    exchange=out_transaction.exchange,
                    holder=out_transaction.holder,
                    transaction_type=out_transaction.transaction_type,
                    spot_price=out_transaction.spot_price,
                    crypto_out_no_fee=out_transaction.crypto_out_no_fee,
                    crypto_fee=out_transaction.crypto_fee,
                    crypto_out_with_fee=out_transaction.crypto_out_with_fee,
                    fiat_out_no_fee=out_transaction.fiat_out_no_fee,
                    fiat_fee=out_transaction.fiat_fee,
                    notes=(
                        f"{out_transaction.notes + '; ' if out_transaction.notes else ''} Sell side of conversion from "
                        f"{out_transaction.crypto_out_with_fee} {out_transaction.asset} -> "
                        f"{in_transaction.crypto_in} {in_transaction.asset} "
                        f"({in_transaction.asset} in-transaction unique id: {in_transaction.unique_id})"
                    ),
                )
            else:
                # InTransaction is fiat, so it is ignored in RP2: however any fiat_fee must be applied to the OutTransaction to avoid losing it
                if not in_transaction.fiat_fee or not out_transaction.fiat_out_no_fee:
                    raise RP2RuntimeError(f"Internal error: swap transactions have incomplete fiat data: {in_transaction} // {out_transaction}")
                fiat_out_no_fee = RP2Decimal(out_transaction.fiat_out_no_fee)
                fiat_fee: RP2Decimal = RP2Decimal(in_transaction.fiat_fee) if in_transaction.fiat_fee else ZERO
                transaction_list[swap_pair.out_transaction.out_transaction_index] = OutTransaction(
                    plugin=self.__COINBASE,
                    unique_id=out_transaction.unique_id,
                    raw_data=out_transaction.raw_data,
                    timestamp=out_transaction.timestamp,
                    asset=out_transaction.asset,
                    exchange=out_transaction.exchange,
                    holder=out_transaction.holder,
                    transaction_type=out_transaction.transaction_type,
                    spot_price=out_transaction.spot_price,
                    crypto_out_no_fee=out_transaction.crypto_out_no_fee,
                    crypto_fee=out_transaction.crypto_fee,
                    crypto_out_with_fee=out_transaction.crypto_out_with_fee,
                    fiat_out_no_fee=str(fiat_out_no_fee - fiat_fee),
                    fiat_fee=str(fiat_fee),
                    notes=(
                        f"{out_transaction.notes + '; ' if out_transaction.notes else ''} Sell side of conversion from "
                        f"{out_transaction.crypto_out_with_fee} {out_transaction.asset} -> "
                        f"{in_transaction.crypto_in} {in_transaction.asset} "
                        f"({in_transaction.asset} in-transaction unique id: {in_transaction.unique_id})"
                    ),
                )

    def _is_credit_card_spend(self, transaction: Any) -> bool:
        return (  # type: ignore
            transaction[_TYPE] is None
            and _TO in transaction
            and _EMAIL in transaction[_TO]
            and transaction[_TO][_EMAIL] == "treasury+coinbase-card@coinbase.com"
        ) or (transaction[_TYPE] == _CARDSPEND)

    def _process_account(self, account: Dict[str, Any]) -> Optional[_ProcessAccountResult]:
        currency: str = account[_CURRENCY][_CODE]
        account_id: str = account[_ID]
        in_transaction_list: List[InTransaction] = []
        out_transaction_list: List[OutTransaction] = []
        intra_transaction_list: List[IntraTransaction] = []
        in_transaction_2_trade_id: Dict[InTransaction, str] = {}
        trade_id_2_out_transaction: Dict[str, OutTransaction] = {}
        out_transaction_2_trade_id: Dict[OutTransaction, str] = {}
        trade_id_2_in_transaction: Dict[str, InTransaction] = {}

        self.__logger.debug("Account: %s", json.dumps(account))

        if account[_CREATED_AT] == account[_UPDATED_AT] and RP2Decimal(account[_BALANCE][_AMOUNT]) == ZERO:
            # skip account without activity to avoid unnecessary API calls
            return None

        transactions = []
        pickle_filename = f"transactions_{account_id}.pkl"
        if self.__pickle_api_cache_enabled and os.path.isfile(pickle_filename):
            with open(pickle_filename, "rb") as file:
                transactions = pickle.load(file)
        else:
            for transaction in self.__get_transactions(account_id):
                transactions.append(transaction)
            if self.__pickle_api_cache_enabled:
                with open(pickle_filename, "wb") as file:
                    pickle.dump(transactions, file)
        
        for transaction in transactions:
            raw_data: str = json.dumps(transaction)
            self.__logger.debug("Transaction: %s", raw_data)
            transaction_type: str = transaction[_TYPE]
            if transaction_type in {_PRIME_WITHDRAWAL, _PRO_DEPOSIT, _PRO_WITHDRAWAL, _EXCHANGE_DEPOSIT, _EXCHANGE_WITHDRAWAL, _SEND, _STAKING_TRANSFER}:
                self._process_transfer(transaction, currency, in_transaction_list, out_transaction_list, intra_transaction_list)
            elif transaction_type in {_BUY, _SELL, _TRADE}:
                self._process_fill(
                    transaction=transaction,
                    currency=currency,
                    in_transaction_list=in_transaction_list,
                    out_transaction_list=out_transaction_list,
                    in_transaction_2_trade_id=in_transaction_2_trade_id,
                    trade_id_2_out_transaction=trade_id_2_out_transaction,
                    out_transaction_2_trade_id=out_transaction_2_trade_id,
                    trade_id_2_in_transaction=trade_id_2_in_transaction,
                )
            elif transaction_type in {_ADVANCED_TRADE_FILL}:
                self._process_advanced_trade_fill(
                    transaction=transaction,
                    currency=currency,
                    in_transaction_list=in_transaction_list,
                    out_transaction_list=out_transaction_list,
                )
            elif transaction_type in {_INTEREST}:
                self._process_gain(transaction, currency, Keyword.INTEREST, in_transaction_list)
            elif transaction_type in {_STAKING_REWARD}:
                self._process_gain(transaction, currency, Keyword.STAKING, in_transaction_list)
            elif transaction_type in {_INFLATION_REWARD}:
                self._process_gain(transaction, currency, Keyword.INCOME, in_transaction_list)
            elif transaction_type in {_FIAT_DEPOSIT, _CARDBUYBACK}:
                # _CARDBUYBACK is a credit card refund
                self._process_fiat_deposit(transaction, currency, in_transaction_list)
            elif transaction_type in {_FIAT_WITHDRAWAL}:
                self._process_fiat_withdrawal(transaction, currency, out_transaction_list)
            elif self._is_credit_card_spend(transaction):
                self._process_fiat_withdrawal(transaction, currency, out_transaction_list, "Coinbase card spend")
            else:
                self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        return _ProcessAccountResult(
            in_transactions=in_transaction_list,
            out_transactions=out_transaction_list,
            intra_transactions=intra_transaction_list,
            in_transaction_2_trade_id=in_transaction_2_trade_id,
            trade_id_2_out_transaction=trade_id_2_out_transaction,
            out_transaction_2_trade_id=out_transaction_2_trade_id,
            trade_id_2_in_transaction=trade_id_2_in_transaction,
        )
        
    def _process_stablecoin_send(self, transaction: Any, raw_data: str, currency: str, out_transaction_list: List[OutTransaction]) -> None:
        amount: RP2Decimal = RP2Decimal(transaction[_AMOUNT][_AMOUNT])
        native_amount: RP2Decimal = RP2Decimal(transaction[_NATIVE_AMOUNT][_AMOUNT])
        transaction_type: str = transaction[_TYPE]
        spot_price: RP2Decimal = RP2Decimal(native_amount / amount)
        
        # Parse transaction fees
        network_json: Any = transaction[_NETWORK] if _NETWORK in transaction else {}
        transaction_fee_json: Any = network_json[_TRANSACTION_FEE] if _TRANSACTION_FEE in network_json else {}
        transaction_fee_currency: str = transaction_fee_json[_CURRENCY] if _CURRENCY in transaction_fee_json else Keyword.UNKNOWN.value
        transaction_fee_fallback: str = Keyword.UNKNOWN.value if transaction_fee_currency == Keyword.UNKNOWN.value else None
        transaction_fee_crypto: str = str(RP2Decimal(transaction_fee_json[_AMOUNT]))\
            if _AMOUNT in transaction_fee_json and transaction_fee_currency in _STABLECOIN_INTERMEDIARIES else transaction_fee_fallback
        transaction_fee_fiat: str = str(RP2Decimal(transaction_fee_json[_AMOUNT]))\
            if _AMOUNT in transaction_fee_json and transaction_fee_currency not in _STABLECOIN_INTERMEDIARIES else transaction_fee_fallback        
        transaction_fee_crypto = str(RP2Decimal(transaction_fee_fiat) / spot_price) \
            if transaction_fee_crypto == transaction_fee_fallback and transaction_fee_fiat != transaction_fee_fallback and spot_price != ZERO\
            else transaction_fee_crypto
        transaction_fee_fiat = str(RP2Decimal(transaction_fee_crypto) * spot_price) \
            if transaction_fee_fiat == transaction_fee_fallback and transaction_fee_crypto != transaction_fee_fallback and spot_price != ZERO\
            else transaction_fee_fiat
        
        # Calculate amounts
        crypto_out_no_fee: str = None
        crypto_out_with_fee: str = None
        fiat_out_no_fee: str = None
        fiat_out_with_fee: str = None
        if transaction_fee_crypto != transaction_fee_fallback:
            crypto_out_no_fee = str(RP2Decimal(abs(amount)) - RP2Decimal(transaction_fee_crypto))
            crypto_out_with_fee = str(abs(amount))
        if transaction_fee_fiat != transaction_fee_fallback:
            fiat_out_no_fee = str(RP2Decimal(abs(native_amount)) - RP2Decimal(transaction_fee_fiat))
            fiat_out_with_fee = str(abs(native_amount))
        # Parse for notes
        crypto_address: str = transaction[_TO][_ADDRESS] if _TO in transaction and _ADDRESS in transaction[_TO] else Keyword.UNKNOWN.value
        network_name: str = transaction['network']['network_name'] if 'network' in transaction and 'network_name' in transaction['network'] else Keyword.UNKNOWN.value
        
        out_transaction_list.append(
            OutTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=raw_data,
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type=Keyword.GIFT.value,
                spot_price=str(native_amount / amount),
                crypto_out_no_fee=crypto_out_no_fee,
                crypto_fee=transaction_fee_crypto,
                crypto_out_with_fee=crypto_out_with_fee,
                fiat_out_no_fee=fiat_out_no_fee,
                fiat_fee=transaction_fee_fiat,
                notes=f"Sent stablecoins to '{crypto_address}' on chain '{network_name}'",
            )
        )

    def _process_asset_delisted_sale(
        self,
        transaction: Any,
        currency: str,
        in_transaction_list: List[InTransaction]
    ) -> None:
        # Incoming money from Coinbase crypto delisting/forced sell.
        fiat_fee = Keyword.UNKNOWN.value
        fallback_amount = Keyword.UNKNOWN.value
        crypto_amount = RP2Decimal(transaction[_AMOUNT][_AMOUNT]) if _AMOUNT in transaction and _AMOUNT in transaction[_AMOUNT] else fallback_amount
        native_amount = RP2Decimal(transaction[_NATIVE_AMOUNT][_AMOUNT]) if _NATIVE_AMOUNT in transaction and _AMOUNT in transaction[_NATIVE_AMOUNT] else fallback_amount
        spot_price = RP2Decimal(transaction[_NATIVE_AMOUNT][_AMOUNT]) / RP2Decimal(transaction[_AMOUNT][_AMOUNT]) \
            if type(crypto_amount) != type(fallback_amount) and type(native_amount) != type(fallback_amount) \
            else Keyword.UNKNOWN.value
        fiat_fee = RP2Decimal("0") # not provided by Coinbase, conservatively assume 0; use transaction hints otherwise
        self.__logger.debug("Asset delisting: %s", json.dumps(transaction))
        notes=f"Reconcile asset delisting {self.__transaction_description_or_default(transaction)} via Manual input.  Sold asset for '{native_amount}', including fees.  This portion reflects the receival of {crypto_amount} '{currency}'."

        in_transaction_list.append(
            InTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=json.dumps(transaction),
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type=Keyword.BUY.value,
                spot_price=str(spot_price),
                crypto_in=str(crypto_amount),
                crypto_fee=None,
                fiat_in_no_fee=str(native_amount),
                fiat_in_with_fee=str(native_amount),
                fiat_fee=str(fiat_fee),
                notes=notes,
            )
        )
    
    def _process_transfer(
        self,
        transaction: Any,
        currency: str,
        in_transaction_list: List[InTransaction],
        out_transaction_list: List[OutTransaction],
        intra_transaction_list: List[IntraTransaction],
    ) -> None:
        # Ensure the amount is positive
        amount: RP2Decimal = RP2Decimal(transaction[_AMOUNT][_AMOUNT])
        native_amount: RP2Decimal = RP2Decimal(transaction[_NATIVE_AMOUNT][_AMOUNT])
        transaction_type: str = transaction[_TYPE]
        raw_data: str = json.dumps(transaction)

        if transaction_type in {_PRIME_WITHDRAWAL, _PRO_WITHDRAWAL, _EXCHANGE_WITHDRAWAL}:
            intra_transaction_list.append(
                IntraTransaction(
                    plugin=self.__COINBASE,
                    unique_id=transaction[_ID],
                    raw_data=raw_data,
                    timestamp=transaction[_CREATED_AT],
                    asset=currency,
                    from_exchange=self.__COINBASE_PRO,
                    from_holder=self.account_holder,
                    to_exchange=self.__COINBASE,
                    to_holder=self.account_holder,
                    spot_price=None,
                    crypto_sent=str(amount),
                    crypto_received=str(amount),
                )
            )
        elif transaction_type in [_PRO_DEPOSIT, _EXCHANGE_DEPOSIT]:
            intra_transaction_list.append(
                IntraTransaction(
                    plugin=self.__COINBASE,
                    unique_id=transaction[_ID],
                    raw_data=raw_data,
                    timestamp=transaction[_CREATED_AT],
                    asset=currency,
                    from_exchange=self.__COINBASE,
                    from_holder=self.account_holder,
                    to_exchange=self.__COINBASE_PRO,
                    to_holder=self.account_holder,
                    spot_price=None,
                    crypto_sent=str(-amount),
                    crypto_received=str(-amount),
                )
            )
        elif transaction_type == _SEND:
            transaction_network = transaction[_NETWORK]
            crypto_hash: str = transaction_network[_HASH] if _HASH in transaction_network else Keyword.UNKNOWN.value
            if amount < ZERO:
                if (  # pylint: disable=too-many-boolean-expressions
                    _TO in transaction
                    and _DETAILS in transaction
                    and transaction[_TO] is not None
                    and _RESOURCE in transaction[_TO]
                    and transaction[_TO][_RESOURCE] in {_USER, _EMAIL}
                    and transaction_network[_STATUS] == _OFF_BLOCKCHAIN
                    and _SUBTITLE in transaction[_DETAILS]
                ):
                    # Outgoing gift to another Coinbase user
                    out_transaction_list.append(
                        OutTransaction(
                            plugin=self.__COINBASE,
                            unique_id=transaction[_ID],
                            raw_data=raw_data,
                            timestamp=transaction[_CREATED_AT],
                            asset=currency,
                            exchange=self.__COINBASE,
                            holder=self.account_holder,
                            transaction_type="Gift",
                            spot_price=str(native_amount / amount),
                            crypto_out_no_fee=str(-amount),
                            crypto_fee="0",
                            crypto_out_with_fee=str(-amount),
                            fiat_out_no_fee=str(-native_amount),
                            fiat_fee="0",
                            notes=f"{self.__transaction_details_or_default(transaction, _SUBTITLE)}",
                        )
                    )
                elif _FROM in transaction and self.__transaction_details_or_default(transaction, _SUBTITLE).startswith("From Coinbase"):
                    # Coinbase Earn reversal transactions (due to credit card refunds typically): this is conservatively treated as a sale.
                    out_transaction_list.append(
                        OutTransaction(
                            plugin=self.__COINBASE,
                            unique_id=transaction[_ID],
                            raw_data=raw_data,
                            timestamp=transaction[_CREATED_AT],
                            asset=currency,
                            exchange=self.__COINBASE,
                            holder=self.account_holder,
                            transaction_type=Keyword.SELL.name,
                            spot_price=str(native_amount / amount),
                            crypto_out_no_fee=str(-amount),
                            crypto_fee="0",
                            crypto_out_with_fee=str(-amount),
                            fiat_out_no_fee=str(-native_amount),
                            fiat_fee="0",
                            notes="Coinbase EARN reversal",
                        )
                    )
                elif currency in _STABLECOIN_INTERMEDIARIES:
                    self._process_stablecoin_send(transaction, raw_data, currency, out_transaction_list)
                    return
                else:
                    intra_transaction_list.append(
                        IntraTransaction(
                            plugin=self.__COINBASE,
                            unique_id=crypto_hash,
                            raw_data=raw_data,
                            timestamp=transaction[_CREATED_AT],
                            asset=currency,
                            from_exchange=self.__COINBASE,
                            from_holder=self.account_holder,
                            to_exchange=Keyword.UNKNOWN.value,
                            to_holder=Keyword.UNKNOWN.value,
                            spot_price=str(native_amount / amount),
                            crypto_sent=str(-amount),
                            crypto_received=Keyword.UNKNOWN.value,
                        )
                    )
            else: # amount >= ZERO
                if (
                    _FROM in transaction
                    and transaction[_FROM][_RESOURCE] == _USER
                    and transaction_network[_STATUS] == _OFF_BLOCKCHAIN
                    
                ):
                    if (_DETAILS in transaction and _SUBTITLE in transaction[_DETAILS]):
                        if _EMAIL in transaction[_FROM]:
                            # Incoming money from another Coinbase user. Marking it as income conservatively, but it could be
                            # a gift or other type: if so the user needs to explicitly recast it with a transaction hint
                            self._process_gain(transaction, currency, Keyword.INCOME, in_transaction_list, f"From: {transaction[_FROM][_EMAIL]}")
                            return
                        elif (
                            self.__transaction_details_or_default(transaction, _SUBTITLE).startswith("From Coinbase")
                            or self.__transaction_details_or_default(transaction, _SUBTITLE).endswith("Coinbase Earn")
                            
                            or self.__transaction_description_or_default(transaction) == "Earn Task"
                        ):
                            self._process_gain(transaction, currency, Keyword.INCOME, in_transaction_list, "Coinbase EARN")
                            return
                    elif (self.__transaction_description_or_default(transaction).startswith("Asset_Delisting")):
                        self._process_asset_delisted_sale(transaction, currency, in_transaction_list)
                        return

                # Unhandled currency-receive case.  Emit partial transaction
                intra_transaction_list.append(
                    IntraTransaction(
                        plugin=self.__COINBASE,
                        unique_id=transaction[_ID],
                        raw_data=raw_data,
                        timestamp=transaction[_CREATED_AT],
                        asset=currency,
                        from_exchange=Keyword.UNKNOWN.value,
                        from_holder=Keyword.UNKNOWN.value,
                        to_exchange=self.__COINBASE,
                        to_holder=self.account_holder,
                        spot_price=str(native_amount / amount),
                        crypto_sent=Keyword.UNKNOWN.value,
                        crypto_received=str(amount),
                    )
                )

    def _process_fill(
        self,
        transaction: Any,
        currency: str,
        in_transaction_list: List[InTransaction],
        out_transaction_list: List[OutTransaction],
        in_transaction_2_trade_id: Dict[InTransaction, str],
        trade_id_2_out_transaction: Dict[str, OutTransaction],
        out_transaction_2_trade_id: Dict[OutTransaction, str],
        trade_id_2_in_transaction: Dict[str, InTransaction]
    ) -> None:
        transaction_type: str = transaction[_TYPE]
        native_amount: RP2Decimal = RP2Decimal(transaction[_NATIVE_AMOUNT][_AMOUNT])
        crypto_amount: RP2Decimal = RP2Decimal(transaction[_AMOUNT][_AMOUNT])
        fiat_fee: RP2Decimal = ZERO
        spot_price: RP2Decimal
        spot_price_string: str

        if not self.is_native_fiat(transaction[_NATIVE_AMOUNT][_CURRENCY]):
            raise RP2RuntimeError(f"Internal error: native amount is not denominated in {self.native_fiat} {json.dumps(transaction)}")

        raw_data: str = json.dumps(transaction)
        if not transaction_type in {_BUY, _SELL, _TRADE}:
            self.__logger.error("Unsupported transaction type for fill (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)
            return

        if native_amount >= ZERO:
            if transaction_type == _BUY:
                buy: Dict[str, Any] = transaction[transaction_type]
                raw_data = f"{raw_data}//{json.dumps(buy)}"
                fiat_fee = ZERO
                if _FEE in buy:
                    fiat_fee = RP2Decimal(buy[_FEE][_AMOUNT])
                if _UNIT_PRICE in buy:
                    spot_price = RP2Decimal(buy[_UNIT_PRICE][_AMOUNT])
                    spot_price_string = str(spot_price)
                elif transaction[_AMOUNT][_CURRENCY] == 'USDC': #todo: determine regional stable coins
                    spot_price = RP2Decimal(1)
                    spot_price_string = str(spot_price)
                else:
                    # swap in transaction
                    spot_price_string = Keyword.UNKNOWN.value
                    if native_amount >= self.__MINIMUM_FIAT_PRECISION:
                        spot_price_string = str((native_amount - fiat_fee) / crypto_amount)
                
                self.__logger.debug("Buy: %s", json.dumps(buy))
            else:
                # swap in transaction
                spot_price_string = Keyword.UNKNOWN.value
                if native_amount >= self.__MINIMUM_FIAT_PRECISION:
                    spot_price_string = str((native_amount - fiat_fee) / crypto_amount)

            fiat_in_no_fee: Optional[str] = None
            fiat_in_with_fee: Optional[str] = None
            if native_amount >= self.__MINIMUM_FIAT_PRECISION:
                fiat_in_no_fee = str(native_amount - fiat_fee)
                fiat_in_with_fee = str(native_amount)

            in_transaction: InTransaction = InTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=raw_data,
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type=Keyword.BUY.name,
                spot_price=spot_price_string,
                crypto_in=str(crypto_amount),
                crypto_fee=None,
                fiat_in_no_fee=fiat_in_no_fee,
                fiat_in_with_fee=fiat_in_with_fee,
                fiat_fee=str(fiat_fee),
                notes=None,
            )
            in_transaction_list.append(in_transaction)
            if transaction_type == _TRADE:
                in_transaction_2_trade_id[in_transaction] = transaction[_TRADE][_ID]
                trade_id_2_in_transaction[transaction[_TRADE][_ID]] = in_transaction

        elif native_amount < ZERO:
            if transaction_type == _SELL:
                sell = transaction[transaction_type]
                raw_data = f"{raw_data}//{json.dumps(sell)}"
                fiat_fee = RP2Decimal(sell[_FEE][_AMOUNT])
                spot_price = RP2Decimal(sell[_UNIT_PRICE][_AMOUNT])
                self.__logger.debug("Sell: %s", json.dumps(sell))
            else:
                # swap out transaction
                spot_price = native_amount / crypto_amount
            out_transaction: OutTransaction = OutTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=raw_data,
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type=Keyword.SELL.name,
                spot_price=str(spot_price),
                crypto_out_no_fee=str(-crypto_amount - fiat_fee / spot_price),
                crypto_fee=str(fiat_fee / spot_price),
                crypto_out_with_fee=str(-crypto_amount),
                fiat_out_no_fee=str(-native_amount),
                fiat_fee=str(fiat_fee),
                notes=None,
            )
            out_transaction_list.append(out_transaction)
            if transaction_type == _TRADE:
                out_transaction_2_trade_id[out_transaction] = transaction[_TRADE][_ID]
                trade_id_2_out_transaction[transaction[_TRADE][_ID]] = out_transaction

    # Handle transactions where purchase of crypto incurred purchase and sale of a stablecoin
    # Actual crypto purchase is a separate transaction.
    def _process_advanced_trade_stablecoin_multi_fill(
        self,
        transaction: Any,
        raw_data: str,
        transaction_type: str,
        native_currency: str,
        product_id: str,
        in_transaction_list: List[InTransaction],
        out_transaction_list: List[OutTransaction],
    ) -> None:

        stable_fee = 0 # verify no fiat-stable fees with more data to reduce cost-basis calculations
        crypto_spot = RP2Decimal(transaction[transaction_type][_FILL_PRICE])
        crypto_fee_in_fiat = RP2Decimal(transaction[transaction_type][_COMMISSION])
        crypto_fee_in_crypto = RP2Decimal(crypto_fee_in_fiat / crypto_spot) 
        non_fiat_currencies = product_id.split('-')
        if len(non_fiat_currencies) != 2:
            raise RP2RuntimeError(f"Internal error: expected two currencies in product id '{product_id}', found '{str(non_fiat_currencies)}'.")
        if transaction[_AMOUNT][_CURRENCY] not in _STABLECOIN_INTERMEDIARIES:
            raise RP2RuntimeError(f"Internal error: stablecoin intermediary transaction expecting stablecoin amount. '{transaction[_AMOUNT][_CURRENCY]}' currency provided instead.")
        crypto_currency = non_fiat_currencies[0]
        stable_currency = non_fiat_currencies[1]
        native_amount = RP2Decimal(transaction[_NATIVE_AMOUNT][_AMOUNT])
        stable_amount = RP2Decimal(transaction[_AMOUNT][_AMOUNT])
        stable_spot = RP2Decimal(abs(native_amount / stable_amount))
        crypto_amount = RP2Decimal(abs(native_amount / crypto_spot))
        
        in_transaction: InTransaction = None
        out_transaction: OutTransaction = None
        if stable_amount > ZERO: # Crypto sale to stablecoin
            in_transaction = InTransaction(
                    plugin=self.__COINBASE,
                    unique_id=transaction[_ID] + _ID_SUFFIX_IN,
                    raw_data=raw_data,
                    timestamp=transaction[_CREATED_AT],
                    asset=stable_currency,
                    exchange=self.__COINBASE,
                    holder=self.account_holder,
                    transaction_type=Keyword.BUY.name,
                    spot_price=str(stable_spot),
                    crypto_in=str(abs(stable_amount)),
                    crypto_fee=None,
                    fiat_in_no_fee=str(abs(native_amount)),
                    fiat_in_with_fee=str(abs(native_amount)),
                    fiat_fee=str(0),
                    notes=f"Cryptocurrency '{crypto_currency}' sale to purchase Stablecoin '{stable_currency}'.  This is stablecoin-purchase portion only.",
                )
            in_transaction_list.append(in_transaction)
        
        else: # Stablecoin sale to purchase crypto
            out_transaction = OutTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID] + _ID_SUFFIX_OUT,
                raw_data=raw_data,
                timestamp=transaction[_CREATED_AT],
                asset=stable_currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type=Keyword.SELL.name,
                spot_price=str(stable_spot),
                crypto_out_no_fee=str(abs(stable_amount)),
                crypto_fee=str(0),
                crypto_out_with_fee=str(abs(stable_amount)),
                fiat_out_no_fee=str(abs(native_amount)),
                fiat_fee=str(0),
                notes=f"Stablecoin '{stable_currency}' sale to purchase '{crypto_currency}'.  This is stable-sale portion only.",
            )
            out_transaction_list.append(out_transaction)

    def _process_advanced_trade_fill(
        self,
        transaction: Any,
        currency: str,
        in_transaction_list: List[InTransaction],
        out_transaction_list: List[OutTransaction],
    ) -> None:
        transaction_type: str = transaction[_TYPE]
        native_amount: RP2Decimal = RP2Decimal(transaction[_NATIVE_AMOUNT][_AMOUNT])
        crypto_amount: RP2Decimal = RP2Decimal(transaction[_AMOUNT][_AMOUNT])
        fiat_fee: RP2Decimal = ZERO
        spot_price: RP2Decimal

        native_currency = transaction[_NATIVE_AMOUNT][_CURRENCY]
        if not self.is_native_fiat(native_currency):
            raise RP2RuntimeError(f"Internal error: native amount is not denominated in {self.native_fiat} {json.dumps(transaction)}")
        
        product_id = transaction[transaction_type][_PRODUCT_ID]
        raw_data: str = json.dumps(transaction)
        
        if not transaction_type in {_ADVANCED_TRADE_FILL}:
            self.__logger.error("Unsupported transaction type for fill (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)
            return

        if native_amount >= ZERO:
            fiat_fee = RP2Decimal(transaction[transaction_type][_COMMISSION])
            spot_price = RP2Decimal(transaction[transaction_type][_FILL_PRICE])

            if currency in _STABLECOIN_INTERMEDIARIES and not \
                (product_id.startswith(native_currency) or product_id.endswith(native_currency)): 
                self._process_advanced_trade_stablecoin_multi_fill(transaction, raw_data, transaction_type, native_currency, product_id, in_transaction_list, out_transaction_list)
                return

            fiat_in_no_fee: Optional[str] = None
            fiat_in_with_fee: Optional[str] = None
            if native_amount >= self.__MINIMUM_FIAT_PRECISION:
                fiat_in_no_fee = str(native_amount)
                fiat_in_with_fee = str(native_amount + fiat_fee)

            in_transaction: InTransaction = InTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=raw_data,
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type=Keyword.BUY.name,
                spot_price=str(spot_price),
                crypto_in=str(crypto_amount),
                crypto_fee=None,
                fiat_in_no_fee=fiat_in_no_fee,
                fiat_in_with_fee=fiat_in_with_fee,
                fiat_fee=str(fiat_fee),
                notes=None,
            )
            in_transaction_list.append(in_transaction)

        elif native_amount < ZERO:
            if currency in _STABLECOIN_INTERMEDIARIES and not \
                (product_id.startswith(native_currency) or product_id.endswith(native_currency)): 
                self._process_advanced_trade_stablecoin_multi_fill(transaction, raw_data, transaction_type, native_currency, product_id, in_transaction_list, out_transaction_list)
                return
            
            fiat_fee = RP2Decimal(transaction[transaction_type][_COMMISSION])
            spot_price = RP2Decimal(transaction[transaction_type][_FILL_PRICE])
            crypto_out_no_fee = -crypto_amount - fiat_fee / spot_price
            out_transaction: OutTransaction = OutTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=raw_data,
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type=Keyword.SELL.name,
                spot_price=str(spot_price),
                crypto_out_no_fee=str(crypto_out_no_fee),
                crypto_fee=str(fiat_fee / spot_price),
                crypto_out_with_fee=str(-crypto_amount),
                # ignoring Coinbase's apparently inconsistent values for str(-native_amount)
                fiat_out_no_fee=str(crypto_out_no_fee * spot_price),
                fiat_fee=str(fiat_fee),
                notes=None,
            )
            out_transaction_list.append(out_transaction)

    def _process_gain(
        self, transaction: Any, currency: str, transaction_type: Keyword, in_transaction_list: List[InTransaction], notes: Optional[str] = None
    ) -> None:
        amount: RP2Decimal = RP2Decimal(transaction[_AMOUNT][_AMOUNT])
        native_amount: RP2Decimal = RP2Decimal(transaction[_NATIVE_AMOUNT][_AMOUNT])
        notes = f"{notes + '; ' if notes else ''}{self.__transaction_details_or_default(transaction, _TITLE)}"
        spot_price: str = Keyword.UNKNOWN.value
        fiat_in_no_fee: Optional[str] = None
        fiat_in_with_fee: Optional[str] = None
        if native_amount >= self.__MINIMUM_FIAT_PRECISION:
            spot_price = str(native_amount / amount)
            fiat_in_no_fee = str(native_amount)
            fiat_in_with_fee = str(native_amount)
        in_transaction_list.append(
            InTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=json.dumps(transaction),
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type=transaction_type.value.capitalize(),
                spot_price=spot_price,
                crypto_in=str(amount),
                crypto_fee=None,
                fiat_in_no_fee=fiat_in_no_fee,
                fiat_in_with_fee=fiat_in_with_fee,
                fiat_fee="0",
                notes=notes,
            )
        )

    def _process_fiat_deposit(self, transaction: Any, currency: str, in_transaction_list: List[InTransaction], notes: Optional[str] = None) -> None:
        amount: RP2Decimal = RP2Decimal(transaction[_AMOUNT][_AMOUNT])
        details_title = self.__transaction_details_or_default(transaction, _TITLE)
        details_subtitle = self.__transaction_details_or_default(transaction, _SUBTITLE)
        notes = f"{notes + '; ' if notes else ''}{details_title + '; ' if details_title else ''}{details_subtitle}"
        in_transaction_list.append(
            InTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=json.dumps(transaction),
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type=Keyword.BUY.value,
                spot_price="1",
                crypto_in=str(amount),
                crypto_fee="0",
                fiat_in_no_fee=None,
                fiat_in_with_fee=None,
                fiat_fee=None,
                notes=notes,
            )
        )

    def _process_fiat_withdrawal(self, transaction: Any, currency: str, out_transaction_list: List[OutTransaction], notes: Optional[str] = None) -> None:
        amount: RP2Decimal = RP2Decimal(transaction[_AMOUNT][_AMOUNT])
        details_title = self.__transaction_details_or_default(transaction, _TITLE)
        details_subtitle = self.__transaction_details_or_default(transaction, _SUBTITLE)
        notes = f"{notes + '; ' if notes else ''}{details_title + '; ' if details_title else ''}{details_subtitle}"
        out_transaction_list.append(
            OutTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=json.dumps(transaction),
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type=Keyword.SELL.value,
                spot_price="1",
                crypto_out_no_fee=str(-amount),
                crypto_fee="0",
                crypto_out_with_fee=None,
                fiat_out_no_fee=None,
                fiat_fee=None,
                notes=notes,
            )
        )

    def __get_accounts(self) -> Any:
        acc = self.__send_request_with_pagination("/v2/accounts")
        return acc

    def __get_transactions(self, account_id: str) -> Any:
        trn = self.__send_request_with_pagination(f"/v2/accounts/{account_id}/transactions")
        return trn

    def __get_buys(self, account_id: str) -> Any:
        buy = self.__send_request_with_pagination(f"/v2/accounts/{account_id}/transactions", params=dict(type="buy"))
        return buy

    def __get_sells(self, account_id: str) -> Any:
        sell = self.__send_request_with_pagination(f"/v2/accounts/{account_id}/transactions", params=dict(type="sell"))
        return sell

    def __send_request(self, method: str, endpoint: str, params: Any = None, data: Any = None) -> Any:  # pylint: disable=unused-private-member
        full_url: str = f"{self.__api_url}{endpoint}"
        
        NUM_RETRIES = 1
        current_num_retries = 0
        while current_num_retries <= NUM_RETRIES:
            try:
                response: dict = self.__auth._request(method, endpoint, params=params, data=data, auth=self.__auth, timeout=self.__TIMEOUT)
                return response
            except requests.exceptions.HTTPError as e:
                self._validate_response(e, method, endpoint)
                self.__auth._reauthenticate()
                current_num_retries += 1
                if current_num_retries > NUM_RETRIES:
                    raise e
        # Defensive programming: we shouldn't reach here.
        import sys
        line_num = sys._getframe().f_lineno
        raise RP2RuntimeError(f"Programming error at line {line_num}")

    # Documented at: https://docs.cloud.coinbase.com/exchange/docs/pagination
    def __send_request_with_pagination(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if params is None:
            params = {}
        
        while True:
            response: dict = {}
            NUM_RETRIES = 1
            current_num_retries = 0
            while current_num_retries <= NUM_RETRIES:
                try:
                    response = self.__auth._request("get", endpoint, params=params, data=None, timeout=self.__TIMEOUT)
                    yield from response["data"]
                    break
                except requests.exceptions.HTTPError as e:
                    self._validate_response(e, "get", endpoint)
                    self.__auth._reauthenticate()
                    current_num_retries += 1
                    if current_num_retries > NUM_RETRIES:
                        raise e

            if "pagination" not in response or "next_uri" not in response["pagination"] or not response["pagination"]["next_uri"]:
                return
            # endpoint = response['pagination']['next_uri']  <-- the RESTClient cannot authorize query parameters
            params['starting_after'] = response['pagination']['next_starting_after']
            if not params['starting_after']:
                return

    def _validate_response(self, exception: requests.exceptions.HTTPError, method: str, endpoint: str) -> None:
        message: str = ""
        self.__logger.info("Error %s", str(exception))
        if exception.response.status_code == 401:
            return # allow reauthentication and retry
        raise exception
