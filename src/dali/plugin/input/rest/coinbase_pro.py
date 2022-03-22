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
from typing import Any, Dict, List, NamedTuple, Optional, cast

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
from dali.transaction_resolver import AssetAndUniqueId

# Native format keywords
_AMOUNT: str = "amount"
_BUY: str = "buy"
_COINBASE_TRANSACTION_ID: str = "coinbase_transaction_id"
_CREATED_AT: str = "created_at"
_CRYPTO_TRANSACTION_HASH: str = "crypto_transaction_hash"
_CURRENCY: str = "currency"
_DEPOSIT: str = "deposit"
_DETAILS: str = "details"
_FEE: str = "fee"
_ID: str = "id"
_MATCH: str = "match"
_ORDER_ID: str = "order_id"
_PRICE: str = "price"
_PRODUCT_ID: str = "product_id"
_SELL: str = "sell"
_SIDE: str = "side"
_SIZE: str = "size"
_TRADE_ID: str = "trade_id"
_TRANSFER: str = "transfer"
_TRANSFER_ID: str = "transfer_id"
_TRANSFER_TYPE: str = "transfer_type"
_TYPE: str = "type"
_USD_VOLUME: str = "usd_volume"
_WITHDRAW: str = "withdraw"


class CoinbaseProAuth(AuthBase):
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


class FromToCurrencyPair(NamedTuple):
    from_currency: str
    to_currency: str


class InputPlugin(AbstractInputPlugin):

    __API_URL: str = "https://api.pro.coinbase.com/"
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
    ) -> None:

        super().__init__(account_holder)
        self.__api_url: str = InputPlugin.__API_URL
        self.__auth = CoinbaseProAuth(api_key, api_secret, api_passphrase)
        self.__session: Session = requests.Session()
        self.__logger: logging.Logger = create_logger(f"{self.__COINBASE_PRO}/{self.account_holder}")

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        product_id_2_trade_id_2_fill: Dict[str, Dict[str, Any]] = {}

        for account in self.__get_accounts():
            currency: str = account[_CURRENCY]
            account_id: str = account[_ID]
            in_transaction_list: List[InTransaction] = []
            out_transaction_list: List[OutTransaction] = []
            intra_transaction_list: List[IntraTransaction] = []

            self.__logger.debug("Account: %s", json.dumps(account))
            for transaction in self.__get_transactions(account_id):
                transaction_type: str = transaction[_TYPE]
                self.__logger.debug("Transaction: %s", json.dumps(transaction))
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

            if in_transaction_list:
                result.extend(in_transaction_list)
            if out_transaction_list:
                result.extend(out_transaction_list)
            if intra_transaction_list:
                result.extend(intra_transaction_list)

        return result

    @staticmethod
    def _parse_product_id(product_id: str) -> FromToCurrencyPair:
        split_product_id: List[str] = product_id.split("-")
        return FromToCurrencyPair(from_currency=split_product_id[0], to_currency=split_product_id[1])

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
                    raw_data=json.dumps(transaction),
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
                    raw_data=json.dumps(transaction),
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
            self.__logger.debug("Unsupported transaction type (skipping): %s", transaction_details[_TRANSFER_TYPE])

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

        from_currency, to_currency = self._parse_product_id(fill[_PRODUCT_ID])
        is_from_currency_fiat: bool = is_fiat(from_currency)
        is_to_currency_fiat: bool = is_fiat(to_currency)
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
                        raw_data=json.dumps(transaction),
                        timestamp=fill[_CREATED_AT],
                        asset=to_currency if is_from_currency_fiat else from_currency,
                        exchange=self.__COINBASE_PRO,
                        holder=self.account_holder,
                        transaction_type="Buy",
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
                        raw_data=json.dumps(transaction),
                        timestamp=fill[_CREATED_AT],
                        asset=from_currency if is_to_currency_fiat else from_currency,
                        exchange=self.__COINBASE_PRO,
                        holder=self.account_holder,
                        transaction_type="Sell",
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
                from_currency_size = RP2Decimal(fill[_SIZE])
                from_currency_price = usd_volume / from_currency_size
                to_currency_size = from_currency_size * RP2Decimal(fill[_PRICE])
                to_currency_price = usd_volume / to_currency_size
            elif fill_side == _BUY:
                (from_currency, to_currency) = (to_currency, from_currency)
                to_currency_size = RP2Decimal(fill[_SIZE])
                to_currency_price = usd_volume / to_currency_size
                from_currency_size = to_currency_size * RP2Decimal(fill[_PRICE])
                from_currency_price = usd_volume / from_currency_size
            else:
                raise Exception(f"Internal error: unsupported fill side {transaction}\n{fill}")
            self.__append_transaction(
                cast(List[AbstractTransaction], out_transaction_list),
                OutTransaction(
                    plugin=self.__COINBASE_PRO,
                    unique_id=unique_id,
                    raw_data=json.dumps(transaction),
                    timestamp=fill[_CREATED_AT],
                    asset=from_currency,
                    exchange=self.__COINBASE_PRO,
                    holder=self.account_holder,
                    transaction_type="Sell",
                    spot_price=str(from_currency_price),
                    crypto_out_no_fee=str(from_currency_size),
                    crypto_fee="0",
                    crypto_out_with_fee=None,
                    fiat_out_no_fee=None,
                    fiat_fee=None,
                    notes=f"Sell side of conversion: {from_currency_size:.8f} {from_currency} -> {to_currency_size:.8f} {to_currency}",
                ),
            )
            self.__append_transaction(
                cast(List[AbstractTransaction], in_transaction_list),
                InTransaction(
                    plugin=self.__COINBASE_PRO,
                    unique_id=f"{unique_id}/buy",
                    raw_data=json.dumps(transaction),
                    timestamp=fill[_CREATED_AT],
                    asset=to_currency,
                    exchange=self.__COINBASE_PRO,
                    holder=self.account_holder,
                    transaction_type="Buy",
                    spot_price=str(to_currency_price),
                    crypto_in=str(to_currency_size),
                    crypto_fee=str(crypto_fee),
                    fiat_in_no_fee=None,
                    fiat_in_with_fee=None,
                    fiat_fee=None,
                    notes=f"Buy side of conversion: {from_currency_size:.8f} {from_currency} -> {to_currency_size:.8f} {to_currency}",
                ),
            )

        else:
            self.__logger.debug("Unsupported transaction (skipping): %s", json.dumps(transaction))

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
            for result in json_response:
                yield result
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
        raise Exception(message)
