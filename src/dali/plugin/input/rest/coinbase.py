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
from typing import Any, Dict, List, Optional

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
_NATIVE_AMOUNT: str = "native_amount"
_NETWORK: str = "network"
_OFF_BLOCKCHAIN: str = "off_blockchain"
_PRO_DEPOSIT: str = "pro_deposit"
_PRO_WITHDRAWAL: str = "pro_withdrawal"
_RESOURCE: str = "resource"
_SELL: str = "sell"
_SEND: str = "send"
_STATUS: str = "status"
_SUBTITLE: str = "subtitle"
_TO: str = "to"
_TRADE: str = "trade"
_TYPE: str = "type"
_UNIT_PRICE: str = "unit_price"
_USER: str = "user"


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

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        for account in self.__get_accounts():
            currency: str = account[_CURRENCY][_CODE]
            account_id: str = account[_ID]
            in_transaction_list: List[InTransaction] = []
            out_transaction_list: List[OutTransaction] = []
            intra_transaction_list: List[IntraTransaction] = []

            self.__logger.debug("Account: %s", json.dumps(account))

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
                self.__logger.debug("Transaction: %s", json.dumps(transaction))
                transaction_type: str = transaction[_TYPE]
                if transaction_type in {_PRO_DEPOSIT, _PRO_WITHDRAWAL, _EXCHANGE_DEPOSIT, _SEND}:
                    self._process_transfer(transaction, currency, in_transaction_list, out_transaction_list, intra_transaction_list)
                elif transaction_type in {_BUY, _SELL, _TRADE}:
                    self._process_fill(transaction, currency, in_transaction_list, out_transaction_list, id_2_buy, id_2_sell)
                elif transaction_type in {_INTEREST}:
                    self._process_interest(transaction, currency, in_transaction_list)
                else:
                    self.__logger.debug("Unsupported transaction type (skipping): %s", transaction_type)

            if in_transaction_list:
                result.extend(in_transaction_list)
            if out_transaction_list:
                result.extend(out_transaction_list)
            if intra_transaction_list:
                result.extend(intra_transaction_list)

        return result

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

        if transaction_type == _PRO_WITHDRAWAL:
            intra_transaction_list.append(
                IntraTransaction(
                    self.__COINBASE,
                    transaction[_ID],
                    json.dumps(transaction),
                    transaction[_CREATED_AT],
                    currency,
                    self.__COINBASE_PRO,
                    self.account_holder,
                    self.__COINBASE,
                    self.account_holder,
                    None,  # Spot price
                    str(amount),
                    str(amount),
                )
            )
        elif transaction_type == _PRO_DEPOSIT:
            intra_transaction_list.append(
                IntraTransaction(
                    self.__COINBASE,
                    transaction[_ID],
                    json.dumps(transaction),
                    transaction[_CREATED_AT],
                    currency,
                    self.__COINBASE,
                    self.account_holder,
                    self.__COINBASE_PRO,
                    self.account_holder,
                    None,  # Spot price
                    str(-amount),
                    str(-amount),
                )
            )
        elif transaction_type == _EXCHANGE_DEPOSIT:
            intra_transaction_list.append(
                IntraTransaction(
                    self.__COINBASE,
                    transaction[_ID],
                    json.dumps(transaction),
                    transaction[_CREATED_AT],
                    currency,
                    self.__COINBASE,
                    self.account_holder,
                    self.__COINBASE_PRO,
                    self.account_holder,
                    None,  # Spot price
                    str(-amount),
                    str(-amount),
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
                            self.__COINBASE,
                            transaction[_ID],
                            json.dumps(transaction),
                            transaction[_CREATED_AT],
                            currency,
                            self.__COINBASE,
                            self.account_holder,
                            "Gift",
                            str(native_amount / amount),
                            str(-amount),
                            "0",
                            str(-amount),
                            str(-native_amount),
                            "0",
                            f"To: {transaction[_TO][_EMAIL]}",
                        )
                    )
                else:
                    intra_transaction_list.append(
                        IntraTransaction(
                            self.__COINBASE,
                            crypto_hash,
                            json.dumps(transaction),
                            transaction[_CREATED_AT],
                            currency,
                            self.__COINBASE,
                            self.account_holder,
                            Keyword.UNKNOWN.value,
                            Keyword.UNKNOWN.value,
                            str(native_amount / amount),
                            str(-amount),
                            Keyword.UNKNOWN.value,
                        )
                    )
            else:
                if transaction[_FROM][_RESOURCE] == _USER and transaction_network[_STATUS] == _OFF_BLOCKCHAIN and _SUBTITLE in transaction[_DETAILS]:
                    if _EMAIL in transaction[_FROM]:
                        # Incoming gift from another Coinbase user
                        in_transaction_list.append(
                            InTransaction(
                                self.__COINBASE,
                                transaction[_ID],
                                json.dumps(transaction),
                                transaction[_CREATED_AT],
                                currency,
                                self.__COINBASE,
                                self.account_holder,
                                "Income",
                                str(native_amount / amount),
                                transaction[_AMOUNT][_AMOUNT],
                                "0",
                                str(native_amount),
                                "0",
                                f"From: {transaction[_FROM][_EMAIL]}",
                            )
                        )
                    elif transaction[_DETAILS][_SUBTITLE].startswith("From Coinbase"):
                        # Coinbase Earn transactions
                        in_transaction_list.append(
                            InTransaction(
                                self.__COINBASE,
                                transaction[_ID],
                                json.dumps(transaction),
                                transaction[_CREATED_AT],
                                currency,
                                self.__COINBASE,
                                self.account_holder,
                                "Income",
                                str(native_amount / amount),
                                transaction[_AMOUNT][_AMOUNT],
                                "0",
                                str(native_amount),
                                str(native_amount),
                                "Coinbase EARN",
                            )
                        )
                else:
                    intra_transaction_list.append(
                        IntraTransaction(
                            self.__COINBASE,
                            crypto_hash,
                            json.dumps(transaction),
                            transaction[_CREATED_AT],
                            currency,
                            Keyword.UNKNOWN.value,
                            Keyword.UNKNOWN.value,
                            self.__COINBASE,
                            self.account_holder,
                            str(native_amount / amount),
                            Keyword.UNKNOWN.value,
                            str(amount),
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

        if transaction_type in {_BUY, _TRADE}:
            spot_price = (native_amount - fiat_fee) / crypto_amount
            if transaction_type == _BUY:
                buy: Dict[str, Any] = id_2_buy[transaction[transaction_type][_ID]]
                fiat_fee = RP2Decimal(buy[_FEE][_AMOUNT])
                self.__logger.debug("Buy: %s", json.dumps(buy))
                spot_price = RP2Decimal(buy[_UNIT_PRICE][_AMOUNT])
            in_transaction_list.append(
                InTransaction(
                    self.__COINBASE,
                    transaction[_ID],
                    json.dumps(transaction),
                    transaction[_CREATED_AT],
                    currency,
                    self.__COINBASE,
                    self.account_holder,
                    "Buy",
                    str(spot_price),
                    str(crypto_amount),
                    str(fiat_fee),
                    str(native_amount - fiat_fee),
                    str(native_amount),
                    None,  # Add notes
                )
            )
        elif transaction_type == _SELL:
            sell = id_2_sell[transaction[transaction_type][_ID]]
            fiat_fee = RP2Decimal(sell[_FEE][_AMOUNT])
            spot_price = RP2Decimal(sell[_UNIT_PRICE][_AMOUNT])
            self.__logger.debug("Sell: %s", json.dumps(sell))
            out_transaction_list.append(
                OutTransaction(
                    self.__COINBASE,
                    transaction[_ID],
                    json.dumps(transaction),
                    transaction[_CREATED_AT],
                    currency,
                    self.__COINBASE,
                    self.account_holder,
                    "Sell",
                    str(spot_price),
                    str(-crypto_amount - fiat_fee / spot_price),
                    str(fiat_fee / spot_price),
                    str(-crypto_amount),
                    str(-native_amount),
                    str(fiat_fee),
                    None,  # Add notes
                )
            )
        else:
            self.__logger.debug("Unsupported transaction type (skipping): %s", json.dumps(transaction_type))

    def _process_interest(self, transaction: Any, currency: str, in_transaction_list: List[InTransaction]) -> None:

        in_transaction_list.append(
            InTransaction(
                self.__COINBASE,
                transaction[_ID],
                json.dumps(transaction),
                transaction[_CREATED_AT],
                currency,
                self.__COINBASE,
                self.account_holder,
                "Interest",
                Keyword.UNKNOWN.value,
                transaction[_AMOUNT][_AMOUNT],
                "0",
                None,
                None,
                None,
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
            if not "pagination" in json_response or not "next_uri" in json_response["pagination"] or not json_response["pagination"]["next_uri"]:
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
