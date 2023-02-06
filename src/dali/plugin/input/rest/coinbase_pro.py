# Copyright 2022 eprbell
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

# CoinbasePro REST plugin links:
# REST API: https://docs.cloud.coinbase.com/exchange/reference
# Authentication: https://docs.cloud.coinbase.com/exchange/docs/authorization-and-authentication
# Endpoint: https://api.pro.coinbase.com

import base64
import hashlib
import hmac
import json
import logging
import time
from multiprocessing.pool import ThreadPool
from typing import Any, Dict, List, NamedTuple, Optional, cast

import requests
from requests import PreparedRequest
from requests.auth import AuthBase
from requests.models import Response
from requests.sessions import Session
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction, AssetAndUniqueId
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# Native format keywords
_AMOUNT: str = "amount"
_BUY: str = "buy"
_COINBASE_TRANSACTION_ID: str = "coinbase_transaction_id"
_CONVERSION: str = "conversion"
_CONVERSION_ID: str = "conversion_id"
_CREATED_AT: str = "created_at"
_CRYPTO_TRANSACTION_HASH: str = "crypto_transaction_hash"
_CURRENCY: str = "currency"
_DEPOSIT: str = "deposit"
_DETAILS: str = "details"
_FEE: str = "fee"
_FROM_ACCOUNT_ID: str = "from_account_id"
_ID: str = "id"
_MATCH: str = "match"
_ORDER_ID: str = "order_id"
_PRICE: str = "price"
_PRODUCT_ID: str = "product_id"
_SELL: str = "sell"
_SIDE: str = "side"
_SIZE: str = "size"
_TO_ACCOUNT_ID: str = "to_account_id"
_TRADE_ID: str = "trade_id"
_TRANSFER: str = "transfer"
_TRANSFER_ID: str = "transfer_id"
_TRANSFER_TYPE: str = "transfer_type"
_TYPE: str = "type"
_USD_VOLUME: str = "usd_volume"
_WITHDRAW: str = "withdraw"


class _ProcessAccountResult(NamedTuple):
    in_transactions: List[InTransaction]
    out_transactions: List[OutTransaction]
    intra_transactions: List[IntraTransaction]


class _FromToCurrencyPair(NamedTuple):
    from_currency: str
    to_currency: str


class _CoinbaseProAuth(AuthBase):
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str) -> None:
        self.__api_key: str = api_key
        self.__api_secret: str = api_secret
        self.__api_passphrase: str = api_passphrase

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        timestamp: str = str(time.time())
        message: str = f"{timestamp}{request.method}{request.path_url}{(request.body or '')}"  # type: ignore
        hmac_key: bytes = base64.b64decode(self.__api_secret)
        signature: hmac.HMAC = hmac.new(hmac_key, message.encode("ascii"), hashlib.sha256)
        signature_b64: str = base64.b64encode(signature.digest()).decode("utf-8")

        request.headers.update(
            {
                "Content-Type": "Application/JSON",
                "CB-ACCESS-SIGN": signature_b64,
                "CB-ACCESS-TIMESTAMP": timestamp,
                "CB-ACCESS-KEY": self.__api_key,
                "CB-ACCESS-PASSPHRASE": self.__api_passphrase,
            }
        )
        return request


class InputPlugin(AbstractInputPlugin):

    __API_URL: str = "https://api.pro.coinbase.com/"
    __DEFAULT_THREAD_COUNT: int = 2
    __MAX_THREAD_COUNT: int = 4
    __TIMEOUT: int = 30

    __COINBASE_PRO: str = "Coinbase Pro"

    # Sometimes Coinbase Pro reports the same transaction twice (e.g. when swapping from a coin to another):
    # in such cases we need to ensure we add the transaction only once, hence the cache
    __fill_cache: Dict[AssetAndUniqueId, AbstractTransaction] = {}

    def __init__(
        self,
        account_holder: str,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        native_fiat: Optional[str] = None,
        thread_count: Optional[int] = None,
    ) -> None:

        super().__init__(account_holder=account_holder, native_fiat=native_fiat)
        self.__api_url: str = InputPlugin.__API_URL
        self.__auth = _CoinbaseProAuth(api_key, api_secret, api_passphrase)
        self.__session: Session = requests.Session()
        self.__logger: logging.Logger = create_logger(f"{self.__COINBASE_PRO}/{self.account_holder}")
        self.__cache_key: str = f"coinbase_pro-{account_holder}"
        self.__thread_count = thread_count if thread_count else self.__DEFAULT_THREAD_COUNT
        if self.__thread_count > self.__MAX_THREAD_COUNT:
            raise RP2RuntimeError(f"Thread count is {self.__thread_count}: it exceeds the maximum value of {self.__MAX_THREAD_COUNT}")
        self.__account_id_2_account: Dict[str, Any] = {}

    def cache_key(self) -> Optional[str]:
        return self.__cache_key

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []
        process_account_result_list: List[Optional[_ProcessAccountResult]]
        accounts = self.__get_accounts()

        for account in accounts:
            self.__account_id_2_account[account[_ID]] = account

        with ThreadPool(self.__thread_count) as pool:
            process_account_result_list = pool.map(self._process_account, accounts)

        for process_account_result in process_account_result_list:
            if process_account_result is None:
                continue
            if process_account_result.in_transactions:
                result.extend(process_account_result.in_transactions)
            if process_account_result.out_transactions:
                result.extend(process_account_result.out_transactions)
            if process_account_result.intra_transactions:
                result.extend(process_account_result.intra_transactions)

        return result

    def _process_account(self, account: Dict[str, Any]) -> Optional[_ProcessAccountResult]:
        currency: str = account[_CURRENCY]
        account_id: str = account[_ID]
        product_id_2_trade_id_2_fill: Dict[str, Dict[str, Any]] = {}
        in_transaction_list: List[InTransaction] = []
        out_transaction_list: List[OutTransaction] = []
        intra_transaction_list: List[IntraTransaction] = []

        self.__logger.debug("Account: %s", json.dumps(account))
        for transaction in self.__get_transactions(account_id):
            transaction_type: str = transaction[_TYPE]
            raw_data: str = json.dumps(transaction)
            self.__logger.debug("Transaction: %s", raw_data)
            if transaction_type == _TRANSFER:
                self._process_transfer(transaction, currency, intra_transaction_list)
            elif transaction_type == _MATCH:
                product_id: str = transaction[_DETAILS][_PRODUCT_ID]
                if product_id not in product_id_2_trade_id_2_fill:
                    trade_id_2_fill: Dict[str, Any] = {}
                    for fill in self.__get_fills(product_id):
                        trade_id_2_fill[f"{fill[_ORDER_ID]}/{fill[_TRADE_ID]}"] = fill
                    product_id_2_trade_id_2_fill[product_id] = trade_id_2_fill
                self._process_fills(
                    transaction,
                    in_transaction_list,
                    out_transaction_list,
                    product_id_2_trade_id_2_fill[product_id][f"{transaction[_DETAILS][_ORDER_ID]}/{transaction[_DETAILS][_TRADE_ID]}"],
                )
            elif transaction_type == _FEE:
                # The fees are already deduced while processing other transactions
                self.__logger.debug("Redundant fee transaction (skipping): %s", raw_data)
            elif transaction_type == _CONVERSION:
                self._process_conversion(transaction, in_transaction_list, out_transaction_list)
            else:
                self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        return _ProcessAccountResult(
            in_transactions=in_transaction_list,
            out_transactions=out_transaction_list,
            intra_transactions=intra_transaction_list,
        )

    @staticmethod
    def _parse_product_id(product_id: str) -> _FromToCurrencyPair:
        split_product_id: List[str] = product_id.split("-")
        return _FromToCurrencyPair(from_currency=split_product_id[0], to_currency=split_product_id[1])

    def _process_transfer(self, transaction: Any, currency: str, intra_transaction_list: List[IntraTransaction]) -> None:
        # Ensure the amount is positive
        amount: RP2Decimal = RP2Decimal(transaction[_AMOUNT])
        if amount < ZERO:
            amount = -amount  # type: ignore
        transaction_details: Any = transaction[_DETAILS]
        transfer_id: str = transaction_details[_TRANSFER_ID]
        transfer: Any = self.__get_transfer(transfer_id)
        transfer_details: Any = transfer[_DETAILS]
        crypto_hash: str = Keyword.UNKNOWN.value
        raw_data: str = f"{json.dumps(transaction)}//{json.dumps(transfer)}"

        self.__logger.debug("Transfer: %s", json.dumps(transfer))
        if _CRYPTO_TRANSACTION_HASH not in transfer_details:
            self.__logger.debug("Transfer to/from Coinbase already captured by Coinbase plugin: ignoring.")
            return

        if _COINBASE_TRANSACTION_ID in transfer_details:
            self.__logger.debug("Transfer is a Coinbase transaction already captured by Coinbase plugin: ignoring.")
            return

        crypto_hash = transfer_details[_CRYPTO_TRANSACTION_HASH]

        if transaction_details[_TRANSFER_TYPE] == _DEPOSIT:
            intra_transaction_list.append(
                IntraTransaction(
                    plugin=self.__COINBASE_PRO,
                    unique_id=crypto_hash,
                    raw_data=raw_data,
                    timestamp=transaction[_CREATED_AT],
                    asset=currency,
                    from_exchange=Keyword.UNKNOWN.value,
                    from_holder=Keyword.UNKNOWN.value,
                    to_exchange=self.__COINBASE_PRO,
                    to_holder=self.account_holder,
                    spot_price=None,
                    crypto_sent=Keyword.UNKNOWN.value,
                    crypto_received=str(amount),
                    notes=None,
                )
            )
        elif transaction_details[_TRANSFER_TYPE] == _WITHDRAW:
            intra_transaction_list.append(
                IntraTransaction(
                    plugin=self.__COINBASE_PRO,
                    unique_id=crypto_hash,
                    raw_data=raw_data,
                    timestamp=transaction[_CREATED_AT],
                    asset=currency,
                    from_exchange=self.__COINBASE_PRO,
                    from_holder=self.account_holder,
                    to_exchange=Keyword.UNKNOWN.value,
                    to_holder=Keyword.UNKNOWN.value,
                    spot_price=None,
                    crypto_sent=str(amount),
                    crypto_received=Keyword.UNKNOWN.value,
                    notes=None,
                )
            )
        else:
            self.__logger.error("Unsupported transfer type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

    def _process_fills(self, transaction: Any, in_transaction_list: List[InTransaction], out_transaction_list: List[OutTransaction], fill: Any) -> None:
        product_id: str = transaction[_DETAILS][_PRODUCT_ID]
        self.__logger.debug("Product id: %s", product_id)

        from_currency: str
        to_currency: str
        usd_volume: RP2Decimal
        fill_side: str = fill[_SIDE]
        unique_id: str = f"{fill[_ORDER_ID]}/{fill[_TRADE_ID]}"
        from_currency_size: RP2Decimal
        from_currency_price: RP2Decimal
        to_currency_size: RP2Decimal
        crypto_fee: RP2Decimal
        to_currency_price: RP2Decimal
        spot_price: RP2Decimal = RP2Decimal(fill[_PRICE])
        crypto_amount: RP2Decimal = RP2Decimal(fill[_SIZE])
        fiat_fee: RP2Decimal = RP2Decimal(fill[_FEE])
        raw_data: str = f"{json.dumps(transaction)}//{json.dumps(fill)}"

        from_currency, to_currency = self._parse_product_id(fill[_PRODUCT_ID])
        is_from_currency_fiat: bool = self.is_native_fiat(from_currency)
        is_to_currency_fiat: bool = self.is_native_fiat(to_currency)
        self.__logger.debug("Fill: %s", json.dumps(fill))
        if (is_from_currency_fiat and not is_to_currency_fiat and fill_side == _SELL) or (  # pylint: disable=too-many-boolean-expressions
            not is_from_currency_fiat and is_to_currency_fiat and fill_side == _BUY
        ):
            self.__append_transaction(
                cast(List[AbstractTransaction], in_transaction_list),
                cast(
                    AbstractTransaction,
                    InTransaction(
                        plugin=self.__COINBASE_PRO,
                        unique_id=unique_id,
                        raw_data=raw_data,
                        timestamp=fill[_CREATED_AT],
                        asset=to_currency if is_from_currency_fiat else from_currency,
                        exchange=self.__COINBASE_PRO,
                        holder=self.account_holder,
                        transaction_type=Keyword.BUY.name,
                        spot_price=str(spot_price),
                        crypto_in=str(crypto_amount),
                        crypto_fee=None,
                        fiat_in_no_fee=None,
                        fiat_in_with_fee=None,
                        fiat_fee=str(fiat_fee),
                        notes=None,
                    ),
                ),
            )
        elif (is_from_currency_fiat and not is_to_currency_fiat and fill_side == _BUY) or (  # pylint: disable=too-many-boolean-expressions
            not is_from_currency_fiat and is_to_currency_fiat and fill_side == _SELL
        ):
            self.__append_transaction(
                cast(List[AbstractTransaction], out_transaction_list),
                cast(
                    AbstractTransaction,
                    OutTransaction(
                        plugin=self.__COINBASE_PRO,
                        unique_id=unique_id,
                        raw_data=raw_data,
                        timestamp=fill[_CREATED_AT],
                        asset=from_currency if is_to_currency_fiat else from_currency,
                        exchange=self.__COINBASE_PRO,
                        holder=self.account_holder,
                        transaction_type=Keyword.SELL.name,
                        spot_price=str(spot_price),
                        crypto_out_no_fee=str(crypto_amount - fiat_fee / spot_price),
                        crypto_fee=str(fiat_fee / spot_price),
                        crypto_out_with_fee=None,
                        fiat_out_no_fee=None,
                        fiat_fee=None,
                        notes=None,
                    ),
                ),
            )
        elif not is_from_currency_fiat and not is_to_currency_fiat:
            # Convert from a crypto to another
            usd_volume = RP2Decimal(fill[_USD_VOLUME])
            crypto_fee = RP2Decimal(fill[_FEE])
            if fill_side == _SELL:
                from_crypto_fee = ZERO
                to_crypto_fee = crypto_fee
                from_currency_size = RP2Decimal(fill[_SIZE])
                from_currency_price = usd_volume / from_currency_size
                to_currency_size = from_currency_size * RP2Decimal(fill[_PRICE])
                to_currency_price = usd_volume / (to_currency_size + to_crypto_fee)
            elif fill_side == _BUY:
                from_crypto_fee = crypto_fee
                to_crypto_fee = ZERO
                (from_currency, to_currency) = (to_currency, from_currency)
                to_currency_size = RP2Decimal(fill[_SIZE])
                to_currency_price = usd_volume / to_currency_size
                from_currency_size = to_currency_size * RP2Decimal(fill[_PRICE])
                from_currency_price = usd_volume / from_currency_size
            else:
                raise RP2RuntimeError(f"Internal error: unsupported fill side {transaction}\n{fill}")
            self.__append_transaction(
                cast(List[AbstractTransaction], out_transaction_list),
                OutTransaction(
                    plugin=self.__COINBASE_PRO,
                    unique_id=unique_id,
                    raw_data=raw_data,
                    timestamp=fill[_CREATED_AT],
                    asset=from_currency,
                    exchange=self.__COINBASE_PRO,
                    holder=self.account_holder,
                    transaction_type=Keyword.SELL.name,
                    spot_price=str(from_currency_price),
                    crypto_out_no_fee=str(from_currency_size),
                    crypto_fee=str(from_crypto_fee),
                    crypto_out_with_fee=None,
                    fiat_out_no_fee=None,
                    fiat_fee=None,
                    notes=f"Sell side of conversion from {fill_side} fill: {from_currency_size:.8f} {from_currency} -> {to_currency_size:.8f} {to_currency}",
                ),
            )

            self.__append_transaction(
                cast(List[AbstractTransaction], in_transaction_list),
                InTransaction(
                    plugin=self.__COINBASE_PRO,
                    unique_id=f"{unique_id}/buy",
                    raw_data=raw_data,
                    timestamp=fill[_CREATED_AT],
                    asset=to_currency,
                    exchange=self.__COINBASE_PRO,
                    holder=self.account_holder,
                    transaction_type=Keyword.BUY.name,
                    spot_price=str(to_currency_price),
                    crypto_in=str(to_currency_size),
                    crypto_fee=str(to_crypto_fee),
                    fiat_in_no_fee=None,
                    fiat_in_with_fee=None,
                    fiat_fee=None,
                    notes=f"Buy side of conversion from {fill_side} fill: {from_currency_size:.8f} {from_currency} -> {to_currency_size:.8f} {to_currency}",
                ),
            )

        else:
            self.__logger.error("Unsupported fill type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

    # This seems to occur when converting fiat to stablecoins and viceversa
    def _process_conversion(self, transaction: Any, in_transaction_list: List[InTransaction], out_transaction_list: List[OutTransaction]) -> None:
        conversion_id: str = transaction[_DETAILS][_CONVERSION_ID]
        conversion: Any = self.__get_conversion(conversion_id)
        from_currency: str = self.__account_id_2_account[conversion[_FROM_ACCOUNT_ID]][_CURRENCY]
        to_currency: str = self.__account_id_2_account[conversion[_TO_ACCOUNT_ID]][_CURRENCY]
        self.__logger.debug("Conversion: %s", json.dumps(conversion))

        unique_id: str = conversion_id
        raw_data: str = f"{json.dumps(transaction)}//{json.dumps(conversion)}"
        amount: str = conversion[_AMOUNT]

        if not self.is_native_fiat(from_currency) and not self.is_native_fiat(to_currency):
            raise RP2RuntimeError(f"Internal error: conversion without fiat currency ({from_currency} -> {to_currency}):{transaction}//{conversion}")

        self.__append_transaction(
            cast(List[AbstractTransaction], out_transaction_list),
            OutTransaction(
                plugin=self.__COINBASE_PRO,
                unique_id=unique_id,
                raw_data=raw_data,
                timestamp=transaction[_CREATED_AT],
                asset=from_currency,
                exchange=self.__COINBASE_PRO,
                holder=self.account_holder,
                transaction_type=Keyword.SELL.name,
                spot_price="1",
                crypto_out_no_fee=amount,
                crypto_fee="0",
                crypto_out_with_fee=None,
                fiat_out_no_fee=None,
                fiat_fee=None,
                notes=f"Sell side of conversion: {amount} {from_currency} -> {amount} {to_currency}",
            ),
        )

        self.__append_transaction(
            cast(List[AbstractTransaction], in_transaction_list),
            InTransaction(
                plugin=self.__COINBASE_PRO,
                unique_id=f"{unique_id}/buy",
                raw_data=raw_data,
                timestamp=transaction[_CREATED_AT],
                asset=to_currency,
                exchange=self.__COINBASE_PRO,
                holder=self.account_holder,
                transaction_type=Keyword.BUY.name,
                spot_price="1",
                crypto_in=amount,
                crypto_fee="0",
                fiat_in_no_fee=None,
                fiat_in_with_fee=None,
                fiat_fee=None,
                notes=f"Buy side of conversion: {amount} {from_currency} -> {amount} {to_currency}",
            ),
        )

    def __append_transaction(self, transaction_list: List[AbstractTransaction], transaction: AbstractTransaction) -> None:
        if AssetAndUniqueId(transaction.asset, transaction.unique_id) not in self.__fill_cache:
            transaction_list.append(transaction)
            self.__fill_cache[AssetAndUniqueId(transaction.asset, transaction.unique_id)] = transaction

    def __get_accounts(self) -> Any:
        return self.__send_request("get", "accounts")

    def __get_fills(self, product_id: str) -> Any:
        return self.__send_request_with_pagination("fills", {"product_id": f"{product_id}"})

    def __get_transfer(self, transfer_id: str) -> Any:
        return self.__send_request("get", f"transfers/{transfer_id}")

    def __get_transactions(self, account_id: str) -> Any:
        return self.__send_request_with_pagination(f"accounts/{account_id}/ledger")

    def __get_conversion(self, conversion_id: str) -> Any:
        return self.__send_request("get", f"conversions/{conversion_id}")

    def __send_request(self, method: str, endpoint: str, params: Any = None, data: Any = None) -> Any:
        full_url: str = f"{self.__api_url}{endpoint}"
        response: Response = self.__session.request(method, full_url, params=params, data=data, auth=self.__auth, timeout=self.__TIMEOUT)
        self._validate_response(response, method, endpoint)
        return response.json()

    # Documented at: https://docs.cloud.coinbase.com/exchange/docs/pagination
    def __send_request_with_pagination(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if params is None:
            params = {}
        full_url: str = f"{self.__api_url}{endpoint}"
        while True:
            response: Response = self.__session.get(full_url, params=params, auth=self.__auth, timeout=self.__TIMEOUT)
            self._validate_response(response, "get", endpoint)
            json_response: Any = response.json()
            yield from json_response
            if not response.headers.get("cb-after"):
                break
            params["after"] = response.headers["cb-after"]

    # Documented at: https://docs.cloud.coinbase.com/exchange/docs/requests
    def _validate_response(self, response: Response, method: str, endpoint: str) -> None:
        json_response: Any = response.json()
        message: str = ""
        if 200 <= response.status_code < 300:
            return
        if "message" in json_response:
            message = json_response["message"]
            self.__logger.error("Error %d: %s%s (%s): %s", response.status_code, self.__api_url, endpoint, method.upper(), json_response["message"])

        response.raise_for_status()

        # Defensive programming: we shouldn't reach here.
        self.__logger.debug("Reached past raise_for_status() call: %s", json_response["message"])
        raise RP2RuntimeError(message)
