# Copyright 2022 macanudo527
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# CCXT documentation:
# https://docs.ccxt.com/en/latest/index.html

# pylint: disable=fixme

import json
import logging

# import re
from datetime import datetime, timezone
from multiprocessing.pool import ThreadPool

# from time import sleep
from typing import Any, Dict, List, NamedTuple, Optional, Union

from ccxt import Exchange
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# Native format keywords
_AMOUNT: str = "amount"
_BUY: str = "buy"
_COST: str = "cost"
_CURRENCY: str = "currency"
_DATE_TIME: str = "datetime"
_DEPOSIT: str = "deposit"
_FEE: str = "fee"
_ID: str = "id"
_INDICATED_AMOUNT: str = "indicatedAmount"
_IS_FIATPAYMENT: str = "is_fiat_payment"
_ORDER: str = "order"
_PRICE: str = "price"
_SELL: str = "sell"
_SIDE: str = "side"
_STATUS: str = "status"
_SYMBOL: str = "symbol"
_TIMESTAMP: str = "timestamp"
_TYPE: str = "type"
_TX_ID: str = "txid"  # CCXT doesn't capitalize I
_WITHDRAWAL: str = "withdrawal"

# Time period constants
_NINETY_DAYS_IN_MS: int = 7776000000
_THIRTY_DAYS_IN_MS: int = 2592000000
_ONE_DAY_IN_MS: int = 86400000
_MS_IN_SECOND: int = 1000

_DEFAULT_WINDOW: int = _THIRTY_DAYS_IN_MS


class _PaginationDetails(NamedTuple):
    symbol: Optional[str]
    since: Optional[int]
    limit: Optional[int]
    params: Optional[Dict[str, Union[int, str, None]]]


class _ProcessAccountResult(NamedTuple):
    in_transactions: List[InTransaction]
    out_transactions: List[OutTransaction]
    intra_transactions: List[IntraTransaction]


class _Trade(NamedTuple):
    base_asset: str
    quote_asset: str
    base_info: str
    quote_info: str


class AbstractPaginationDetailSet:
    def __iter__(self) -> "AbstractPaginationDetailsIterator":
        raise NotImplementedError("Abstract method")


class DateBasedPaginationDetailSet(AbstractPaginationDetailSet):
    def __init__(
        self,
        exchange_start_time: int,
        limit: Optional[int] = None,
        markets: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[int, str, None]]] = None,
        window: Optional[int] = None,
    ) -> None:

        super().__init__()
        self.__exchange_start_time: int = exchange_start_time
        self.__limit: Optional[int] = limit
        self.__markets: Optional[List[str]] = markets
        self.__params: Optional[Dict[str, Union[int, str, None]]] = params
        self.__window: Optional[int] = window

    def __iter__(self) -> "DateBasedPaginationDetailsIterator":
        return DateBasedPaginationDetailsIterator(
            self.__exchange_start_time,
            self.__limit,
            self.__markets,
            self.__params,
            self.__window,
        )

    @property
    def window(self) -> int:
        if self.__window:
            return self.__window
        return _DEFAULT_WINDOW


class CustomDateBasedPaginationDetailSet(DateBasedPaginationDetailSet):
    def __init__(
        self,
        exchange_start_time: int,
        start_time_key: str,
        end_time_key: str,
        window: int,
        limit: Optional[int] = None,
        markets: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[int, str, None]]] = None,
    ) -> None:

        super().__init__(exchange_start_time, limit, markets, params, window)
        self.__start_time_key: str = start_time_key
        self.__end_time_key: str = end_time_key

    def __iter__(self) -> "CustomDateBasedPaginationDetailsIterator":
        return CustomDateBasedPaginationDetailsIterator(
            self.__exchange_start_time,
            self.__start_time_key,
            self.__end_time_key,
            self.window,
            self.__limit,
            self.__markets,
            self.__params,
        )


class AbstractPaginationDetailsIterator:
    def __init__(self, limit: Optional[int], markets: Optional[List[str]] = None, params: Optional[Dict[str, Union[int, str, None]]] = None) -> None:
        self.__limit: Optional[int] = limit
        self.__markets: Optional[List[str]] = markets
        self.__market_count: int = 0
        self.__params: Optional[Dict[str, Union[int, str, None]]] = params

    def get_market(self) -> Optional[str]:
        if self.__markets:
            return self.__markets[self.__market_count]
        return None

    def has_more_markets(self) -> bool:
        if self.__markets:
            return self.__market_count + 1 < len(self.__markets)
        return False

    def next_market(self) -> None:
        self.__market_count += 1

    def update_fetched_elements(self, current_results: Any) -> None:
        raise NotImplementedError("Abstract method")

    @property
    def limit(self) -> Optional[int]:
        return self.__limit

    @property
    def since(self) -> Optional[int]:
        return None

    @property
    def params(self) -> Optional[Dict[str, Union[int, str, None]]]:
        return self.__params

    def __next__(self) -> _PaginationDetails:
        raise NotImplementedError("Abstract method")


class DateBasedPaginationDetailsIterator(AbstractPaginationDetailsIterator):
    def __init__(
        self,
        exchange_start_time: int,
        limit: Optional[int] = None,
        markets: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[int, str, None]]] = None,
        window: Optional[int] = None,
    ) -> None:

        super().__init__(limit, markets, params)
        self.__end_of_data = False
        self.__since: Optional[int] = exchange_start_time
        self.__exchange_start_time: int = exchange_start_time
        self.__now: int = int(datetime.now().timestamp()) * _MS_IN_SECOND
        self.__window: Optional[int] = window

    def update_fetched_elements(self, current_results: Any) -> None:

        end_of_market: bool = False

        # Update Since if needed otherwise end_of_market
        if self.__since is not None:
            if len(current_results):
                # All times are inclusive
                self.__since = current_results[len(current_results) - 1][_TIMESTAMP] + 1
            elif self.__window:
                self.__since += self.__window

            if self.__since > self.__now:  # type: ignore
                end_of_market = True
        else:
            end_of_market = True

        if end_of_market and self.has_more_markets():
            # we have reached the end of one market, now let's move on to the next
            self.__since = self.__exchange_start_time
            self.next_market()
        else:
            self.__end_of_data = True

    def __next__(self) -> _PaginationDetails:
        while not self.__end_of_data:
            return _PaginationDetails(
                symbol=self.get_market(),
                since=self.__since,
                limit=self.limit,
                params=self.params,
            )
        raise StopIteration(self)

    @property
    def window(self) -> int:
        if self.__window:
            return self.__window
        return _DEFAULT_WINDOW


class CustomDateBasedPaginationDetailsIterator(DateBasedPaginationDetailsIterator):
    def __init__(
        self,
        exchange_start_time: int,
        start_time_key: str,
        end_time_key: str,
        window: int,
        limit: Optional[int] = None,
        markets: Optional[List[str]] = None,
        params: Optional[Dict[str, Union[int, str, None]]] = None,
    ) -> None:

        super().__init__(exchange_start_time, limit, markets, params, window)
        self.__start_time_key: str = start_time_key
        self.__end_time_key: str = end_time_key

    def __next__(self) -> _PaginationDetails:

        while not self.__end_of_data:
            base_details: _PaginationDetails = super().__next__()
            end_of_window: int = self.since + self.window
            if base_details.params:
                base_details.params[self.__start_time_key] = self.since
                base_details.params[self.__end_time_key] = end_of_window
            else:
                base_details._replace(params={self.__start_time_key: self.since, self.__end_time_key: end_of_window})
            return base_details
        raise StopIteration(self)

    @property
    def exchange_start_time(self) -> int:
        return self.__exchange_start_time

    @property
    def since(self) -> int:
        if self.__since:
            return self.__since
        return self.exchange_start_time

    @since.setter
    def since(self, value: int) -> None:
        if value < self.__exchange_start_time:
            raise ValueError("Since time is before exchange start time")
        self.__since = value


# class IdBasedPaginationDetails(AbstractPaginationDetails):
#     ...

# class PageNumberBasedPaginationDetails(AbstractPaginationDetails):
#     ...


class AbstractCcxtInputPlugin(AbstractInputPlugin):

    __DEFAULT_THREAD_COUNT: int = 3

    def __init__(
        self,
        account_holder: str,
        exchange_start_time: datetime,
        native_fiat: Optional[str],
        thread_count: Optional[int],
    ) -> None:

        super().__init__(account_holder, native_fiat)
        self.__logger: logging.Logger = create_logger(f"{self.exchange_name()}/{self.account_holder}")
        self.__cache_key: str = f"{str(self.exchange_name).lower()}-{account_holder}"
        self.__client: Exchange = self.initialize_client()
        if native_fiat:
            self.__native_fiat: str = native_fiat
        else:
            self.__native_fiat = "USD"
        if thread_count:
            self.__thread_count: int = thread_count
        else:
            self.__thread_count = self.__DEFAULT_THREAD_COUNT
        self.__markets: List[str] = []
        self.__start_time: datetime = exchange_start_time
        self.__start_time_ms: int = int(self.__start_time.timestamp()) * _MS_IN_SECOND

    def plugin_name(self) -> str:
        raise NotImplementedError("Abstract method")

    def cache_key(self) -> Optional[str]:
        return self.__cache_key

    def initialize_client(self) -> Exchange:
        raise NotImplementedError("Abstract method")

    def exchange_name(self) -> str:
        raise NotImplementedError("Abstract method")

    def get_process_deposits_pagination_detail_set(self) -> AbstractPaginationDetailSet:
        raise NotImplementedError("Abstract method")

    def get_process_withdrawals_pagination_detail_set(self) -> AbstractPaginationDetailSet:
        raise NotImplementedError("Abstract method")

    # Some exchanges require you to loop through all markets for trades
    def get_process_trades_pagination_detail_set(self) -> AbstractPaginationDetailSet:
        raise NotImplementedError("Abstract method")

    @property
    def client(self) -> Exchange:
        return self.__client

    @property
    def logger(self) -> logging.Logger:
        return self.__logger

    @property
    def markets(self) -> List[str]:

        if self.__markets:
            return self.__markets

        ccxt_markets: Any = self.__client.fetch_markets()
        for market in ccxt_markets:
            self.__logger.debug("Market: %s", json.dumps(market))
            self.__markets.append(market[_ID])
        return self.__markets

    @property
    def native_fiat(self) -> str:
        return self.__native_fiat

    @property
    def start_time_ms(self) -> int:
        return self.__start_time_ms

    @property
    def thread_count(self) -> int:
        return self.__thread_count

    @staticmethod
    def _rp2_timestamp_from_ms_epoch(epoch_timestamp: str) -> str:
        rp2_time = datetime.fromtimestamp((int(epoch_timestamp) / _MS_IN_SECOND), timezone.utc)

        return rp2_time.strftime("%Y-%m-%d %H:%M:%S%z")

    @staticmethod
    def _to_trade(market_pair: str, base_amount: str, quote_amount: str) -> _Trade:
        assets = market_pair.split("/")
        return _Trade(
            base_asset=assets[0],
            quote_asset=assets[1],
            base_info=f"{base_amount} {assets[0]}",
            quote_info=f"{quote_amount} {assets[1]}",
        )

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []
        in_transactions: List[InTransaction] = []
        out_transactions: List[OutTransaction] = []
        intra_transactions: List[IntraTransaction] = []

        self._process_deposits(intra_transactions)
        self._process_gains(in_transactions, out_transactions)
        self._process_trades(in_transactions, out_transactions)
        self._process_withdrawals(intra_transactions)
        self._process_implicit_api(in_transactions, out_transactions, intra_transactions)

        result.extend(in_transactions)
        result.extend(out_transactions)
        result.extend(intra_transactions)

        return result

    ### Multiple Transaction Processing or Pagination Methods

    def _process_deposits(
        self,
        intra_transactions: List[IntraTransaction],
    ) -> None:

        pagination_detail_set: AbstractPaginationDetailSet = self.get_process_deposits_pagination_detail_set()
        pagination_detail_iterator: AbstractPaginationDetailsIterator = iter(pagination_detail_set)
        try:
            while True:
                pagination_details: _PaginationDetails = next(pagination_detail_iterator)
                deposits = self.__client.fetch_deposits(
                    code=pagination_details.symbol,
                    since=pagination_details.since,
                    limit=pagination_details.limit,
                    params=pagination_details.params,
                )
                # CCXT returns a standardized response from fetch_deposits. 'info' is the exchange-specific information
                # in this case from Binance.com

                # {
                #   'info': {
                #       'amount': '0.00999800',
                #       'coin': 'PAXG',
                #       'network': 'ETH',
                #       'status': '1',
                #       'address': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c',
                #       'addressTag': '',
                #       'txId': '0xaad4654a3234aa6118af9b4b335f5ae81c360b2394721c019b5d1e75328b09f3',
                #       'insertTime': '1599621997000',
                #       'transferType': '0',
                #       'confirmTimes': '12/12',
                #       'unlockConfirm': '12/12',
                #       'walletType': '0'
                #   },
                #   'id': None,
                #   'txid': '0xaad4654a3234aa6118af9b4b335f5ae81c360b2394721c019b5d1e75328b09f3',
                #   'timestamp': 1599621997000,
                #   'datetime': '2020-09-09T03:26:37.000Z',
                #   'network': 'ETH',
                #   'address': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c',
                #   'addressTo': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c',
                #   'addressFrom': None,
                #   'tag': None,
                #   'tagTo': None,
                #   'tagFrom': None,
                #   'type': 'deposit',
                #   'amount': 0.00999800,
                #   'currency': 'PAXG',
                #   'status': 'ok',
                #   'updated': None,
                #   'internal': False,
                #   'fee': None
                # }

                pagination_detail_iterator.update_fetched_elements(deposits)

                with ThreadPool(self.__thread_count) as pool:
                    processing_result_list: List[Optional[_ProcessAccountResult]] = pool.map(self._process_transfer, deposits)

                for processing_result in processing_result_list:
                    if processing_result is None:
                        continue
                    if processing_result.intra_transactions:
                        intra_transactions.extend(processing_result.intra_transactions)

        except StopIteration:
            # End of pagination details
            pass

    def _process_gains(
        self,
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
    ) -> None:
        raise NotImplementedError("Abstract method")

    def _process_implicit_api(
        self,
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
        intra_transactions: List[IntraTransaction],
    ) -> None:
        raise NotImplementedError("Abstract method")

    def _process_trades(
        self,
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
    ) -> None:

        pagination_detail_set: AbstractPaginationDetailSet = self.get_process_trades_pagination_detail_set()
        pagination_detail_iterator: AbstractPaginationDetailsIterator = iter(pagination_detail_set)
        try:
            while True:
                pagination_details: _PaginationDetails = next(pagination_detail_iterator)
                trades: Optional[List[Dict[str, Union[str, float]]]] = self.__client.fetch_my_trades(
                    symbol=pagination_details.symbol,
                    since=pagination_details.since,
                    limit=pagination_details.limit,
                    params=pagination_details.params,
                )
                #   {
                #       'info':         { ... },                    // the original decoded JSON as is
                #       'id':           '12345-67890:09876/54321',  // string trade id
                #       'timestamp':    1502962946216,              // Unix timestamp in milliseconds
                #       'datetime':     '2017-08-17 12:42:48.000',  // ISO8601 datetime with milliseconds
                #       'symbol':       'ETH/BTC',                  // symbol
                #       'order':        '12345-67890:09876/54321',  // string order id or undefined/None/null
                #       'type':         'limit',                    // order type, 'market', 'limit' or undefined/None/null
                #       'side':         'buy',                      // direction of the trade, 'buy' or 'sell'
                #       'takerOrMaker': 'taker',                    // string, 'taker' or 'maker'
                #       'price':        0.06917684,                 // float price in quote currency
                #       'amount':       1.5,                        // amount of base currency
                #       'cost':         0.10376526,                 // total cost, `price * amount`,
                #       'fee':          {                           // provided by exchange or calculated by ccxt
                #           'cost':  0.0015,                        // float
                #           'currency': 'ETH',                      // usually base currency for buys, quote currency for sells
                #           'rate': 0.002,                          // the fee rate (if available)
                #       },
                #   }

                # * The work on ``'fee'`` info is still in progress, fee info may be missing partially or entirely, depending on the exchange capabilities.
                # * The ``fee`` currency may be different from both traded currencies (for example, an ETH/BTC order with fees in USD).
                # * The ``cost`` of the trade means ``amount * price``. It is the total *quote* volume of the trade (whereas `amount` is the *base* volume).
                # * The cost field itself is there mostly for convenience and can be deduced from other fields.
                # * The ``cost`` of the trade is a *"gross"* value. That is the value pre-fee, and the fee has to be applied afterwards.

                pagination_detail_iterator.update_fetched_elements(trades)

                with ThreadPool(self.__thread_count) as pool:
                    processing_result_list: List[Optional[_ProcessAccountResult]] = pool.map(self._process_buy_and_sell, trades)  # type: ignore

                for processing_result in processing_result_list:
                    if processing_result is None:
                        continue
                    if processing_result.in_transactions:
                        in_transactions.extend(processing_result.in_transactions)
                    if processing_result.out_transactions:
                        out_transactions.extend(processing_result.out_transactions)

        except StopIteration:
            # End of pagination details
            pass

    def _process_withdrawals(
        self,
        intra_transactions: List[IntraTransaction],
    ) -> None:

        pagination_detail_set: AbstractPaginationDetailSet = self.get_process_withdrawals_pagination_detail_set()
        pagination_detail_iterator: AbstractPaginationDetailsIterator = iter(pagination_detail_set)
        try:
            while True:
                pagination_details: _PaginationDetails = next(pagination_detail_iterator)
                withdrawals: Optional[List[Dict[str, Union[str, float]]]] = self.__client.fetch_withdrawals(
                    code=pagination_details.symbol,
                    since=pagination_details.since,
                    limit=pagination_details.limit,
                    params=pagination_details.params,
                )
                # {
                #   'info': {
                #       'amount': '0.00999800',
                #       'coin': 'PAXG',
                #       'network': 'ETH',
                #       'status': '1',
                #       'address': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c',
                #       'addressTag': '',
                #       'txId': '0xaad4654a3234aa6118af9b4b335f5ae81c360b2394721c019b5d1e75328b09f3',
                #       'insertTime': '1599621997000',
                #       'transferType': '0',
                #       'confirmTimes': '12/12',
                #       'unlockConfirm': '12/12',
                #       'walletType': '0'
                #   },
                #   'id': None,
                #   'txid': '0xaad4654a3234aa6118af9b4b335f5ae81c360b2394721c019b5d1e75328b09f3',
                #   'timestamp': 1599621997000,
                #   'datetime': '2020-09-09T03:26:37.000Z',
                #   'network': 'ETH',
                #   'address': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c',
                #   'addressTo': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c',
                #   'addressFrom': None,
                #   'tag': None,
                #   'tagTo': None,
                #   'tagFrom': None,
                #   'type': 'withdrawal',
                #   'amount': 0.00999800,
                #   'currency': 'PAXG',
                #   'status': 'ok',
                #   'updated': None,
                #   'internal': False,
                #   'fee': None
                # }
                pagination_detail_iterator.update_fetched_elements(withdrawals)

                with ThreadPool(self.__thread_count) as pool:
                    processing_result_list: List[Optional[_ProcessAccountResult]] = pool.map(self._process_transfer, withdrawals)  # type: ignore

                for processing_result in processing_result_list:
                    if processing_result is None:
                        continue
                    if processing_result.intra_transactions:
                        intra_transactions.extend(processing_result.intra_transactions)

        except StopIteration:
            # End of pagination details
            pass

    ### Single Transaction Processing

    def _process_buy_and_sell(self, transaction: Any) -> _ProcessAccountResult:
        results: _ProcessAccountResult = self._process_buy(transaction)
        results.out_transactions.extend(self._process_sell(transaction).out_transactions)

        return results

    def _process_buy(self, transaction: Any, notes: Optional[str] = None) -> _ProcessAccountResult:
        self.__logger.debug("Buy: %s", json.dumps(transaction))
        in_transaction_list: List[InTransaction] = []
        out_transaction_list: List[OutTransaction] = []
        crypto_in: RP2Decimal
        crypto_fee: RP2Decimal

        trade: _Trade = self._to_trade(transaction[_SYMBOL], str(transaction[_AMOUNT]), str(transaction[_COST]))
        if transaction[_SIDE] == _BUY:
            out_asset = trade.quote_asset
            in_asset = trade.base_asset
            crypto_in = RP2Decimal(str(transaction[_AMOUNT]))
            conversion_info = f"{trade.quote_info} -> {trade.base_info}"
        elif transaction[_SIDE] == _SELL:
            out_asset = trade.base_asset
            in_asset = trade.quote_asset
            crypto_in = RP2Decimal(str(transaction[_COST]))
            conversion_info = f"{trade.base_info} -> {trade.quote_info}"
        else:
            raise Exception(f"Internal error: unrecognized transaction side: {transaction[_SIDE]}")

        if transaction[_FEE][_CURRENCY] == in_asset:
            crypto_fee = RP2Decimal(str(transaction[_FEE][_COST]))
        else:
            crypto_fee = ZERO
            transaction_fee = transaction[_FEE][_COST]

            # Users can use other crypto assets to pay for trades
            if transaction[_FEE][_CURRENCY] != out_asset and float(transaction_fee) > 0:
                out_transaction_list.append(
                    OutTransaction(
                        plugin=self.plugin_name(),
                        unique_id=transaction[_ID],
                        raw_data=json.dumps(transaction),
                        timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_TIMESTAMP]),
                        asset=transaction[_FEE][_CURRENCY],
                        exchange=self.exchange_name(),
                        holder=self.account_holder,
                        transaction_type=Keyword.FEE.value,
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_out_no_fee="0",
                        crypto_fee=str(transaction_fee),
                        crypto_out_with_fee=str(transaction_fee),
                        fiat_out_no_fee=None,
                        fiat_fee=None,
                        notes=(f"{notes + '; ' if notes else ''} Fee for conversion from " f"{conversion_info}"),
                    )
                )

        # Is this a plain buy or a conversion?
        if trade.quote_asset == self.__native_fiat:
            fiat_in_with_fee = RP2Decimal(str(transaction[_COST]))
            fiat_fee = RP2Decimal(crypto_fee)
            spot_price = RP2Decimal(str(transaction[_PRICE]))
            if transaction[_SIDE] == _BUY:
                transaction_notes = f"Fiat buy of {trade.base_asset} with {trade.quote_asset}"
                fiat_in_no_fee = fiat_in_with_fee - (fiat_fee * spot_price)
            elif transaction[_SIDE] == _SELL:
                transaction_notes = f"Fiat sell of {trade.base_asset} into {trade.quote_asset}"
                fiat_in_no_fee = fiat_in_with_fee - fiat_fee

            in_transaction_list.append(
                InTransaction(
                    plugin=self.plugin_name(),
                    unique_id=transaction[_ID],
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_TIMESTAMP]),
                    asset=in_asset,
                    exchange=self.exchange_name(),
                    holder=self.account_holder,
                    transaction_type=Keyword.BUY.value,
                    spot_price=str(spot_price),
                    crypto_in=str(crypto_in),
                    crypto_fee=str(crypto_fee),
                    fiat_in_no_fee=str(fiat_in_no_fee),
                    fiat_in_with_fee=str(fiat_in_with_fee),
                    fiat_fee=None,
                    fiat_ticker=trade.quote_asset,
                    notes=(f"{notes + '; ' if notes else ''} {transaction_notes}"),
                )
            )

        else:
            transaction_notes = f"Buy side of conversion from " f"{conversion_info}" f"({out_asset} out-transaction unique id: {transaction[_ID]}"

            in_transaction_list.append(
                InTransaction(
                    plugin=self.plugin_name(),
                    unique_id=transaction[_ID],
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_TIMESTAMP]),
                    asset=in_asset,
                    exchange=self.exchange_name(),
                    holder=self.account_holder,
                    transaction_type=Keyword.BUY.value,
                    spot_price=Keyword.UNKNOWN.value,
                    crypto_in=str(crypto_in),
                    crypto_fee=str(crypto_fee),
                    fiat_in_no_fee=None,
                    fiat_in_with_fee=None,
                    fiat_fee=None,
                    notes=(f"{notes + '; ' if notes else ''} {transaction_notes}"),
                )
            )

        return _ProcessAccountResult(in_transactions=in_transaction_list, out_transactions=out_transaction_list, intra_transactions=[])

    def _process_sell(self, transaction: Any, notes: Optional[str] = None) -> _ProcessAccountResult:
        self.__logger.debug("Sell: %s", json.dumps(transaction))
        out_transaction_list: List[OutTransaction] = []
        trade: _Trade = self._to_trade(transaction[_SYMBOL], str(transaction[_AMOUNT]), str(transaction[_COST]))

        # For some reason CCXT outputs amounts in float
        if transaction[_SIDE] == _BUY:
            out_asset = trade.quote_asset
            in_asset = trade.base_asset
            crypto_out_no_fee: RP2Decimal = RP2Decimal(str(transaction[_COST]))
            conversion_info = f"{trade.quote_info} -> {trade.base_info}"
        elif transaction[_SIDE] == _SELL:
            out_asset = trade.base_asset
            in_asset = trade.quote_asset
            crypto_out_no_fee = RP2Decimal(str(transaction[_AMOUNT]))
            conversion_info = f"{trade.base_info} -> {trade.quote_info}"
        else:
            raise Exception(f"Internal error: unrecognized transaction side: {transaction[_SIDE]}")

        if transaction[_FEE][_CURRENCY] == out_asset:
            crypto_fee: RP2Decimal = RP2Decimal(str(transaction[_FEE][_COST]))
        else:
            crypto_fee = ZERO
        crypto_out_with_fee: RP2Decimal = crypto_out_no_fee + crypto_fee

        # Is this a plain buy or a conversion?
        if trade.quote_asset == self.__native_fiat:
            fiat_out_no_fee: RP2Decimal = RP2Decimal(str(transaction[_COST]))
            fiat_fee: RP2Decimal = RP2Decimal(crypto_fee)
            spot_price: RP2Decimal = RP2Decimal(str(transaction[_PRICE]))

            out_transaction_list.append(
                OutTransaction(
                    plugin=self.plugin_name(),
                    unique_id=transaction[_ID],
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_TIMESTAMP]),
                    asset=out_asset,
                    exchange=self.exchange_name(),
                    holder=self.account_holder,
                    transaction_type=Keyword.SELL.value,
                    spot_price=str(spot_price),
                    crypto_out_no_fee=str(crypto_out_no_fee),
                    crypto_fee=str(crypto_fee),
                    crypto_out_with_fee=str(crypto_out_with_fee),
                    fiat_out_no_fee=str(fiat_out_no_fee),
                    fiat_fee=str(fiat_fee),
                    fiat_ticker=trade.quote_asset,
                    notes=(f"{notes + ';' if notes else ''} Fiat sell of {trade.base_asset} with {trade.quote_asset}."),
                )
            )

        else:
            # CCXT does not report the value of the transaction in fiat
            out_transaction_list.append(
                OutTransaction(
                    plugin=self.plugin_name(),
                    unique_id=transaction[_ID],
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_TIMESTAMP]),
                    asset=out_asset,
                    exchange=self.exchange_name(),
                    holder=self.account_holder,
                    transaction_type=Keyword.SELL.value,
                    spot_price=Keyword.UNKNOWN.value,
                    crypto_out_no_fee=str(crypto_out_no_fee),
                    crypto_fee=str(crypto_fee),
                    crypto_out_with_fee=str(crypto_out_with_fee),
                    fiat_out_no_fee=None,
                    fiat_fee=None,
                    notes=(
                        f"{notes + '; ' if notes else ''} Sell side of conversion from "
                        f"{conversion_info}"
                        f"({in_asset} in-transaction unique id: {transaction[_ID]}"
                    ),
                )
            )

        return _ProcessAccountResult(out_transactions=out_transaction_list, in_transactions=[], intra_transactions=[])

    def _process_transfer(self, transaction: Any) -> _ProcessAccountResult:
        self.__logger.debug("Transfer: %s", json.dumps(transaction))
        if transaction[_STATUS] == "failed":
            self.__logger.info("Skipping failed transfer %s", json.dumps(transaction))
        else:
            intra_transaction_list: List[IntraTransaction] = []

            # This is a CCXT list must convert to string from float
            amount: RP2Decimal = RP2Decimal(str(transaction[_AMOUNT]))

            if transaction[_TYPE] == _DEPOSIT:
                intra_transaction_list.append(
                    IntraTransaction(
                        plugin=self.plugin_name(),
                        unique_id=transaction[_TX_ID],
                        raw_data=json.dumps(transaction),
                        timestamp=transaction[_DATE_TIME],
                        asset=transaction[_CURRENCY],
                        from_exchange=Keyword.UNKNOWN.value,
                        from_holder=Keyword.UNKNOWN.value,
                        to_exchange=self.exchange_name(),
                        to_holder=self.account_holder,
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_sent=Keyword.UNKNOWN.value,
                        crypto_received=str(amount),
                    )
                )
            elif transaction[_TYPE] == _WITHDRAWAL:
                intra_transaction_list.append(
                    IntraTransaction(
                        plugin=self.plugin_name(),
                        unique_id=transaction[_TX_ID],
                        raw_data=json.dumps(transaction),
                        timestamp=transaction[_DATE_TIME],
                        asset=transaction[_CURRENCY],
                        from_exchange=self.exchange_name(),
                        from_holder=self.account_holder,
                        to_exchange=Keyword.UNKNOWN.value,
                        to_holder=Keyword.UNKNOWN.value,
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_sent=str(amount),
                        crypto_received=Keyword.UNKNOWN.value,
                    )
                )
            else:
                self.__logger.error("Unrecognized Crypto transfer: %s", json.dumps(transaction))

        return _ProcessAccountResult(out_transactions=[], in_transactions=[], intra_transactions=intra_transaction_list)
