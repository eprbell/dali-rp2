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

# Coinbase REST plugin links:
# REST API: https://developers.coinbase.com/api/v2
# Authentication: https://developers.coinbase.com/docs/wallet/api-key-authentication
# Endpoint: https://api.coinbase.com

import hashlib
import hmac
import json
import logging
import time
from typing import Any, Dict, List, Optional, NamedTuple
from multiprocessing.pool import ThreadPool

import requests
from requests import PreparedRequest
from requests.auth import AuthBase
from requests.models import Response
from requests.sessions import Session
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.dali_configuration import Keyword, is_fiat
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# Native format keywords
_AMOUNT: str = "amount"
_BALANCE: str = "balance"
_BUY: str = "buy"
_CODE: str = "code"
_CREATED_AT: str = "created_at"
_CURRENCY: str = "currency"
_DETAILS: str = "details"
_EMAIL: str = "email"
_EXCHANGE_DEPOSIT: str = "exchange_deposit"
_FEE: str = "fee"
_FROM: str = "from"
_HASH: str = "hash"
_ID: str = "id"
_INTEREST: str = "interest"
_INFLATION_REWARD: str = "inflation_reward"
_NATIVE_AMOUNT: str = "native_amount"
_NETWORK: str = "network"
_OFF_BLOCKCHAIN: str = "off_blockchain"
_PRIME_WITHDRAWAL: str = "prime_withdrawal"
_PRO_DEPOSIT: str = "pro_deposit"
_PRO_WITHDRAWAL: str = "pro_withdrawal"
_RESOURCE: str = "resource"
_SELL: str = "sell"
_SEND: str = "send"
_STATUS: str = "status"
_SUBTITLE: str = "subtitle"
_TITLE: str = "title"
_TO: str = "to"
_TRADE: str = "trade"
_TYPE: str = "type"
_UNIT_PRICE: str = "unit_price"
_UPDATED_AT: str = "updated_at"
_USER: str = "user"


class TransactionList(NamedTuple):
    in_transactions: List[InTransaction]
    out_transactions: List[OutTransaction]
    intra_transactions: List[IntraTransaction]


class CoinbaseAuth(AuthBase):

    __API_VERSION: str = "2017-11-27"

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.__api_key: str = api_key
        self.__api_secret: str = api_secret

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


class InputPlugin(AbstractInputPlugin):

    __API_URL: str = "https://api.coinbase.com"
    __THREAD_COUNT: int = 3
    __TIMEOUT: int = 30

    __COINBASE: str = "Coinbase"
    __COINBASE_PRO: str = "Coinbase Pro"

    def __init__(
        self,
        account_holder: str,
        api_key: str,
        api_secret: str,
    ) -> None:

        super().__init__(account_holder)
        self.__api_url: str = InputPlugin.__API_URL
        self.__auth: CoinbaseAuth = CoinbaseAuth(api_key, api_secret)
        self.__session: Session = requests.Session()
        self.__logger: logging.Logger = create_logger(f"{self.__COINBASE}/{self.account_holder}")
        self.__cache_key: str = f"{self.__COINBASE.lower()}-{account_holder}"

    def cache_key(self) -> Optional[str]:
        return self.__cache_key

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []
        accounts = self.__get_accounts()
        transaction_lists: List[Optional[TransactionList]]
        with ThreadPool(self.__THREAD_COUNT) as pool:
            transaction_lists = pool.map(self._process_account, accounts)

        for transaction_list in transaction_lists:
            if transaction_list is None:
                continue
            if transaction_list.in_transactions:
                result.extend(transaction_list.in_transactions)
            if transaction_list.out_transactions:
                result.extend(transaction_list.out_transactions)
            if transaction_list.intra_transactions:
                result.extend(transaction_list.intra_transactions)

        return result

    def _process_account(self, account: Dict[str, Any]) -> Optional[TransactionList]:
        currency: str = account[_CURRENCY][_CODE]
        account_id: str = account[_ID]
        in_transaction_list: List[InTransaction] = []
        out_transaction_list: List[OutTransaction] = []
        intra_transaction_list: List[IntraTransaction] = []

        self.__logger.debug("Account: %s", json.dumps(account))

        if account[_CREATED_AT] == account[_UPDATED_AT] and RP2Decimal(account[_BALANCE][_AMOUNT]) == ZERO:
            # skip account without activity to avoid unnecessary API calls
            return None

        id_2_buy: Dict[str, Any] = {}
        for buy in self.__get_buys(account_id):
            id_2_buy[buy[_ID]] = buy

        id_2_sell: Dict[str, Any] = {}
        for sell in self.__get_sells(account_id):
            id_2_sell[sell[_ID]] = sell

        for transaction in self.__get_transactions(account_id):
            if is_fiat(currency):
                self.__logger.debug("Skipping fiat transaction: %s", json.dumps(transaction))
                continue

            raw_data: str = json.dumps(transaction)
            self.__logger.debug("Transaction: %s", raw_data)
            transaction_type: str = transaction[_TYPE]
            if transaction_type in {_PRIME_WITHDRAWAL, _PRO_DEPOSIT, _PRO_WITHDRAWAL, _EXCHANGE_DEPOSIT, _SEND}:
                self._process_transfer(transaction, currency, in_transaction_list, out_transaction_list, intra_transaction_list)
            elif transaction_type in {_BUY, _SELL, _TRADE}:
                self._process_fill(transaction, currency, in_transaction_list, out_transaction_list, id_2_buy, id_2_sell)
            elif transaction_type in {_INTEREST}:
                self._process_interest(transaction, currency, in_transaction_list)
            elif transaction_type in {_INFLATION_REWARD}:
                self._process_income(transaction, currency, in_transaction_list, transaction[_DETAILS][_TITLE])
            else:
                self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

        return TransactionList(
            in_transactions=in_transaction_list,
            out_transactions=out_transaction_list,
            intra_transactions=intra_transaction_list,
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

        if transaction_type in {_PRIME_WITHDRAWAL, _PRO_WITHDRAWAL}:
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
        elif transaction_type == _PRO_DEPOSIT:
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
        elif transaction_type == _EXCHANGE_DEPOSIT:
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
                if (
                    transaction[_TO][_RESOURCE] == _USER
                    and transaction_network[_STATUS] == _OFF_BLOCKCHAIN
                    and _SUBTITLE in transaction[_DETAILS]
                    and _EMAIL in transaction[_TO]
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
                            notes=f"To: {transaction[_TO][_EMAIL]}",
                        )
                    )
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
            else:
                if transaction[_FROM][_RESOURCE] == _USER and transaction_network[_STATUS] == _OFF_BLOCKCHAIN and _SUBTITLE in transaction[_DETAILS]:
                    if _EMAIL in transaction[_FROM]:
                        # Incoming gift from another Coinbase user
                        in_transaction_list.append(
                            InTransaction(
                                plugin=self.__COINBASE,
                                unique_id=transaction[_ID],
                                raw_data=raw_data,
                                timestamp=transaction[_CREATED_AT],
                                asset=currency,
                                exchange=self.__COINBASE,
                                holder=self.account_holder,
                                transaction_type="Income",
                                spot_price=str(native_amount / amount),
                                crypto_in=transaction[_AMOUNT][_AMOUNT],
                                crypto_fee=None,
                                fiat_in_no_fee=str(native_amount),
                                fiat_in_with_fee=str(native_amount),
                                fiat_fee="0",
                                notes=f"From: {transaction[_FROM][_EMAIL]}",
                            )
                        )
                    elif transaction[_DETAILS][_SUBTITLE].startswith("From Coinbase"):
                        # Coinbase Earn transactions
                        self._process_income(transaction, currency, in_transaction_list, "Coinbase EARN")
                else:
                    intra_transaction_list.append(
                        IntraTransaction(
                            plugin=self.__COINBASE,
                            unique_id=crypto_hash,
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
        id_2_buy: Dict[str, Any],
        id_2_sell: Dict[str, Any],
    ) -> None:
        transaction_type: str = transaction[_TYPE]
        native_amount: RP2Decimal = RP2Decimal(transaction[_NATIVE_AMOUNT][_AMOUNT])
        crypto_amount: RP2Decimal = RP2Decimal(transaction[_AMOUNT][_AMOUNT])
        fiat_fee: RP2Decimal = ZERO
        spot_price: RP2Decimal

        if transaction[_NATIVE_AMOUNT][_CURRENCY] != "USD":
            # This is probably a coin swap: TBD (for now just return)
            self.__logger.warning("Swap transaction encountered (swaps not supported yet by Coinbase plugin): %s", json.dumps(transaction))
            return

        raw_data: str = json.dumps(transaction)
        if transaction_type == _BUY or (transaction_type == _TRADE and native_amount >= ZERO):
            if transaction_type == _BUY:
                buy: Dict[str, Any] = id_2_buy[transaction[transaction_type][_ID]]
                raw_data = f"{raw_data}//{json.dumps(buy)}"
                fiat_fee = RP2Decimal(buy[_FEE][_AMOUNT])
                self.__logger.debug("Buy: %s", json.dumps(buy))
                spot_price = RP2Decimal(buy[_UNIT_PRICE][_AMOUNT])
            else:
                # swap in transaction
                spot_price = (native_amount - fiat_fee) / crypto_amount
            in_transaction_list.append(
                InTransaction(
                    plugin=self.__COINBASE,
                    unique_id=transaction[_ID],
                    raw_data=raw_data,
                    timestamp=transaction[_CREATED_AT],
                    asset=currency,
                    exchange=self.__COINBASE,
                    holder=self.account_holder,
                    transaction_type="Buy",
                    spot_price=str(spot_price),
                    crypto_in=str(crypto_amount),
                    crypto_fee=None,
                    fiat_in_no_fee=str(native_amount - fiat_fee),
                    fiat_in_with_fee=str(native_amount),
                    fiat_fee=str(fiat_fee),
                    notes=None,  # Add notes
                )
            )
        elif transaction_type == _SELL or (transaction_type == _TRADE and native_amount < ZERO):
            if transaction_type == _SELL:
                sell = id_2_sell[transaction[transaction_type][_ID]]
                fiat_fee = RP2Decimal(sell[_FEE][_AMOUNT])
                self.__logger.debug("Sell: %s", json.dumps(sell))
                spot_price = RP2Decimal(sell[_UNIT_PRICE][_AMOUNT])
                raw_data = f"{raw_data}//{json.dumps(sell)}"
            else:
                # swap out transaction
                spot_price = native_amount / crypto_amount

            out_transaction_list.append(
                OutTransaction(
                    plugin=self.__COINBASE,
                    unique_id=transaction[_ID],
                    raw_data=raw_data,
                    timestamp=transaction[_CREATED_AT],
                    asset=currency,
                    exchange=self.__COINBASE,
                    holder=self.account_holder,
                    transaction_type="Sell",
                    spot_price=str(spot_price),
                    crypto_out_no_fee=str(-crypto_amount - fiat_fee / spot_price),
                    crypto_fee=str(fiat_fee / spot_price),
                    crypto_out_with_fee=str(-crypto_amount),
                    fiat_out_no_fee=str(-native_amount),
                    fiat_fee=str(fiat_fee),
                    notes=None,
                )
            )
        else:
            self.__logger.error("Unsupported transaction type (skipping): %s. Please open an issue at %s", raw_data, self.ISSUES_URL)

    def _process_interest(self, transaction: Any, currency: str, in_transaction_list: List[InTransaction]) -> None:

        in_transaction_list.append(
            InTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=json.dumps(transaction),
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type="Interest",
                spot_price=Keyword.UNKNOWN.value,
                crypto_in=transaction[_AMOUNT][_AMOUNT],
                crypto_fee=None,
                fiat_in_no_fee=None,
                fiat_in_with_fee=None,
                fiat_fee="0",
                notes=None,
            )
        )

    def _process_income(self, transaction: Any, currency: str, in_transaction_list: List[InTransaction], notes: str) -> None:
        amount: RP2Decimal = RP2Decimal(transaction[_AMOUNT][_AMOUNT])
        native_amount: RP2Decimal = RP2Decimal(transaction[_NATIVE_AMOUNT][_AMOUNT])
        in_transaction_list.append(
            InTransaction(
                plugin=self.__COINBASE,
                unique_id=transaction[_ID],
                raw_data=json.dumps(transaction),
                timestamp=transaction[_CREATED_AT],
                asset=currency,
                exchange=self.__COINBASE,
                holder=self.account_holder,
                transaction_type="Income",
                spot_price=str(native_amount / amount),
                crypto_in=str(amount),
                crypto_fee=None,
                fiat_in_no_fee=str(native_amount),
                fiat_in_with_fee=str(native_amount),
                fiat_fee="0",
                notes=notes,
            )
        )

    def __get_accounts(self) -> Any:
        return self.__send_request_with_pagination("/v2/accounts")

    def __get_transactions(self, account_id: str) -> Any:
        return self.__send_request_with_pagination(f"/v2/accounts/{account_id}/transactions")

    def __get_buys(self, account_id: str) -> Any:
        return self.__send_request_with_pagination(f"/v2/accounts/{account_id}/buys")

    def __get_sells(self, account_id: str) -> Any:
        return self.__send_request_with_pagination(f"/v2/accounts/{account_id}/sells")

    def __send_request(self, method: str, endpoint: str, params: Any = None, data: Any = None) -> Any:  # pylint: disable=unused-private-member
        full_url: str = f"{self.__api_url}{endpoint}"
        response: Response = self.__session.request(method, full_url, params=params, data=data, auth=self.__auth, timeout=self.__TIMEOUT)
        self._validate_response(response, method, endpoint)
        return response.json()

    # Documented at: https://docs.cloud.coinbase.com/exchange/docs/pagination
    def __send_request_with_pagination(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if params is None:
            params = {}
        current_url: str = f"{self.__api_url}{endpoint}"
        while True:
            response: Response = self.__session.get(current_url, params=params, auth=self.__auth, timeout=self.__TIMEOUT)
            self._validate_response(response, "get", endpoint)
            json_response: Any = response.json()
            for result in json_response["data"]:
                yield result
            if "pagination" not in json_response or "next_uri" not in json_response["pagination"] or not json_response["pagination"]["next_uri"]:
                break
            current_url = f"{self.__api_url}{json_response['pagination']['next_uri']}"

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
        raise Exception(message)
