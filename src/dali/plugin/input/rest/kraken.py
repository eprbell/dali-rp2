# Copyright 2023 ndopencode
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

# Kraken REST plugin links:
# REST API: https://docs.kraken.com/rest/
# Authentication: https://docs.kraken.com/rest/#section/Authentication
# Endpoint: https://api.kraken.com

# CCXT documentation:
# https://docs.ccxt.com/en/latest/index.html

import logging
from datetime import datetime
from typing import Dict, List, Optional, Union, Set

from ccxt import Exchange, kraken
from rp2.logger import create_logger
from rp2.abstract_country import AbstractCountry
from rp2.rp2_decimal import RP2Decimal, ZERO
from rp2.rp2_error import RP2RuntimeError

from dali.abstract_ccxt_input_plugin import (
    AbstractCcxtInputPlugin,
)
from dali.ccxt_pagination import (
    AbstractPaginationDetailSet,
)
from dali.abstract_transaction import AbstractTransaction
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction
from dali.configuration import Keyword, _FIAT_SET
from dali.cache import load_from_cache, save_to_cache

# keywords
_AMOUNT: str = 'amount'
_ASSET: str = 'asset'
_BASE: str = 'base'
_BASE_ID: str = 'baseId'
_COST: str = 'cost'
_COUNT: str = 'count'
_CREDIT: str = 'credit'
_DEPOSIT: str = 'deposit'
_FEE: str = 'fee'
_IN: str = "in"
_INTRA: str = 'intra'
_LEDGER: str = 'ledger'
_MARGIN: str = 'margin'
_OFFSET: str = 'ofs'
_OUT: str = "out"
_PAIR: str = 'pair'
_PRICE: str = 'price'
_QUOTE: str = 'quote'
_REFID: str = 'refid'
_RESULT: str = 'result'
_ROLLOVER: str = 'rollover'
_SALE: str = 'sale'
_SETTLED: str = 'settled'
_STAKING: str = 'staking'
_TIMESTAMP: str = 'time'
_TRADE: str = 'trade'
_TRADES: str = 'trades'
_TRANSFER: str = 'transfer'
_TYPE: str = 'type'
_WITHDRAWAL: str = 'withdrawal'

# Record Limits
_TRADE_RECORD_LIMIT: int = 50

_KRAKEN_FIAT_SET: Set[str] = {'AUD', 'CAD', 'EUR', 'GBP', 'JPY', 'USD',
                             'USDC', 'USDT', 'ZAUD', 'ZCAD', 'ZEUR', 'ZGBP', 'ZJPY', 'ZUSD'}

_KRAKEN_FIAT_LIST = list(set(list(_KRAKEN_FIAT_SET) + list(_FIAT_SET)))


class InputPlugin(AbstractCcxtInputPlugin):

    __EXCHANGE_NAME: str = "kraken"
    __PLUGIN_NAME: str = "kraken_REST"
    __DEFAULT_THREAD_COUNT: int = 1
    __CACHE_FILE: str = 'kraken.pickle'

    def __init__(
        self,
        account_holder: str,
        api_key: str,
        api_secret: str,
        native_fiat: str,
        thread_count: Optional[int] = __DEFAULT_THREAD_COUNT,
        use_cache: Optional[bool] = True,
    ) -> None:
        self.__api_key = api_key
        self.__api_secret = api_secret

        # We will have a default start time of July 27th, 2011 since Kraken Exchange officially launched on July 28th.
        super().__init__(account_holder, datetime(2011, 7, 27, 0, 0, 0, 0), native_fiat, thread_count)
        self.__logger: logging.Logger = create_logger(f"{self.__EXCHANGE_NAME}")
        self._initialize_client()
        self._client.load_markets()
        self._client.markets_by_id.update({'BSVUSD': {'id': 'BSVUSD', _BASE_ID: 'BSV', _BASE: 'BSV', _QUOTE: 'USD'}})
        self.base_id_to_base: Dict[str, str] = {value[_BASE_ID]: value[_BASE] for key, value in self._client.markets_by_id.items()}
        self.use_cache: bool = use_cache

    def exchange_name(self) -> str:
        return self.__EXCHANGE_NAME

    def plugin_name(self) -> str:
        return self.__PLUGIN_NAME

    def _initialize_client(self) -> kraken:
        return kraken(
            {
                "apiKey": self.__api_key,
                "enableRateLimit": True,
                "secret": self.__api_secret,
            }
        )

    @property
    def _client(self) -> kraken:
        super_client: Exchange = super()._client
        if not isinstance(super_client, kraken):
            raise RP2RuntimeError("Exchange is not instance of class kraken.")
        return super_client

    def _get_process_deposits_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        pass

    def _get_process_withdrawals_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        pass

    def _get_process_trades_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        pass

    def _process_gains(
        self,
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
    ) -> None:
        pass

    def _gather_api_data(self):
        loaded_cache = load_from_cache(self.__CACHE_FILE)
        if self.use_cache and loaded_cache:
            return loaded_cache

        # get initial trade history to get count
        index: int = 0
        count: int = int(self._client.private_post_tradeshistory(params={_OFFSET: index})[_RESULT][_COUNT])
        trade_history: Dict[str, Dict[str, Union[str, int, None, List[str]]]] = {}
        while index < count:
            trade_history.update(self._process_trade_history(index))
            index += _TRADE_RECORD_LIMIT

        # reset index and count for next API call
        index = 0
        count = int(self._client.private_post_ledgers(params={_OFFSET: index})[_RESULT][_COUNT])
        ledger: Dict[str, Dict[str, Union[str, int, None, List[str]]]] = {}
        while index < count:
            ledger.update(self._process_ledger(index))
            index += _TRADE_RECORD_LIMIT

        result = (trade_history, ledger)

        if self.use_cache:
            save_to_cache(self.__CACHE_FILE, result)

        return result

    def load(self, country: AbstractCountry=None) -> List[AbstractTransaction]:
        (trade_history, ledger) = self._gather_api_data()
        return self._compute_transaction_set(trade_history, ledger)

    def _compute_transaction_set(self, trade_history, ledger) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        unhandled_types: Dict[str, str] = {}
        for key in ledger:
            record: Dict[str, str] = ledger[key]
            self.__logger.debug("Ledger record: %s", record)

            timestamp_value: str = self._rp2_timestamp_from_seconds_epoch(record[_TIMESTAMP])

            is_fiat_asset: bool = record[_ASSET] in _KRAKEN_FIAT_LIST

            amount: RP2Decimal = abs(RP2Decimal(record[_AMOUNT]))
            asset_base: str = self.base_id_to_base[record[_ASSET]]
            raw_data = str(record)

            if record[_TYPE] == _WITHDRAWAL or record[_TYPE] == _DEPOSIT:
                is_deposit: bool = record[_TYPE] == _DEPOSIT
                is_withdrawal: bool = record[_TYPE] == _WITHDRAWAL
                spot_price: str = Keyword.UNKNOWN.value

                result.append(
                    IntraTransaction(
                        plugin=self.__PLUGIN_NAME,
                        unique_id=Keyword.UNKNOWN.value,
                        raw_data=raw_data,
                        timestamp=timestamp_value,
                        asset=asset_base,
                        from_exchange=self.__EXCHANGE_NAME if is_withdrawal else Keyword.UNKNOWN.value,
                        from_holder=self.account_holder if is_withdrawal else Keyword.UNKNOWN.value,
                        to_exchange=self.__EXCHANGE_NAME if is_deposit else Keyword.UNKNOWN.value,
                        to_holder=self.account_holder if is_deposit else Keyword.UNKNOWN.value,
                        spot_price=spot_price,
                        crypto_sent=Keyword.UNKNOWN.value if is_deposit else str(amount),
                        crypto_received=str(amount) if is_deposit else Keyword.UNKNOWN.value,
                        notes=key,
                    )
                )
                continue

            crypto_fee: str = '0' if is_fiat_asset else record[_FEE]
            fiat_fee: str = record[_FEE] if is_fiat_asset else None

            if record[_TYPE] == _TRADE and not is_fiat_asset:
                self.__logger.debug("Trade history record: %s", trade_history[record[_REFID]])

                asset_quote: str = self._client.markets_by_id[trade_history[record[_REFID]][_PAIR]][_QUOTE]
                is_quote_asset_fiat: bool = asset_quote in _KRAKEN_FIAT_LIST

                spot_price: str = trade_history[record[_REFID]][_PRICE] if is_quote_asset_fiat else Keyword.UNKNOWN.value
                transaction_type: str = Keyword.BUY.value if RP2Decimal(record[_AMOUNT]) > ZERO else Keyword.SELL.value

                unique_id: str = Keyword.UNKNOWN.value if spot_price is Keyword.UNKNOWN.value else key

                if RP2Decimal(record[_AMOUNT]) > ZERO:
                    crypto_in = str(amount)
                    fiat_in_no_fee: str = str(RP2Decimal(trade_history[record[_REFID]][_COST]) - RP2Decimal(
                        trade_history[record[_REFID]][_FEE]))
                    fiat_in_with_fee: str = trade_history[record[_REFID]][_COST]
                    result.append(
                        InTransaction(
                            plugin=self.__PLUGIN_NAME,
                            unique_id=unique_id,
                            raw_data=raw_data,
                            timestamp=timestamp_value,
                            asset=asset_base,
                            exchange=self.__EXCHANGE_NAME,
                            holder=self.account_holder,
                            transaction_type=transaction_type,
                            spot_price=spot_price,
                            crypto_in=crypto_in,
                            crypto_fee=crypto_fee,
                            fiat_in_no_fee=fiat_in_no_fee,
                            fiat_in_with_fee=fiat_in_with_fee,
                            fiat_fee=fiat_fee,
                            notes=key,
                        )
                    )
                else:
                    crypto_out_no_fee: str = str(amount)
                    crypto_out_with_fee: str = str(amount + RP2Decimal(record[_FEE]))
                    fiat_out_no_fee: str = str(RP2Decimal(trade_history[record[_REFID]][_COST]) - RP2Decimal(
                        trade_history[record[_REFID]][_FEE]))

                    result.append(
                        OutTransaction(
                            plugin=self.__PLUGIN_NAME,
                            unique_id=unique_id,
                            raw_data=raw_data,
                            timestamp=timestamp_value,
                            asset=asset_base,
                            exchange=self.__EXCHANGE_NAME,
                            holder=self.account_holder,
                            transaction_type=transaction_type,
                            spot_price=spot_price,
                            crypto_out_no_fee=crypto_out_no_fee,
                            crypto_fee=crypto_fee,
                            crypto_out_with_fee=crypto_out_with_fee,
                            fiat_out_no_fee=fiat_out_no_fee,
                            fiat_fee=fiat_fee,
                            notes=key,
                        )
                    )
            elif record[_TYPE] == _MARGIN or record[_TYPE] == _ROLLOVER:
                self.__logger.debug("Trade history record: %s", trade_history[record[_REFID]])

                spot_price: str = Keyword.UNKNOWN.value
                crypto_out_no_fee: str = str(amount)
                crypto_out_with_fee: str = str(amount + RP2Decimal(record[_FEE]))
                fiat_out_no_fee: str = str(RP2Decimal(trade_history[record[_REFID]][_COST]) - RP2Decimal(
                    trade_history[record[_REFID]][_FEE]))

                result.append(
                    OutTransaction(
                        plugin=self.__PLUGIN_NAME,
                        unique_id=Keyword.UNKNOWN.value,
                        raw_data=raw_data,
                        timestamp=timestamp_value,
                        asset=asset_base,
                        exchange=self.__EXCHANGE_NAME,
                        holder=self.account_holder,
                        transaction_type=Keyword.SELL.value,
                        spot_price=spot_price,
                        crypto_out_no_fee=crypto_out_no_fee,
                        crypto_fee=crypto_fee,
                        crypto_out_with_fee=crypto_out_with_fee,
                        fiat_out_no_fee=fiat_out_no_fee,
                        fiat_fee=fiat_fee,
                        notes=key,
                    )
                )
            elif record[_TYPE] == _TRANSFER:
                spot_price: str = Keyword.UNKNOWN.value
                crypto_in: str = str(amount)
                fiat_in_no_fee: str = '0'
                fiat_in_with_fee: str = '0'

                result.append(
                    InTransaction(
                        plugin=self.__PLUGIN_NAME,
                        unique_id=Keyword.UNKNOWN.value,
                        raw_data=raw_data,
                        timestamp=timestamp_value,
                        asset=asset_base,
                        exchange=self.__EXCHANGE_NAME,
                        holder=self.account_holder,
                        transaction_type=Keyword.BUY.value,
                        spot_price=spot_price,
                        crypto_in=crypto_in,
                        crypto_fee=crypto_fee,
                        fiat_in_no_fee=fiat_in_no_fee,
                        fiat_in_with_fee=fiat_in_with_fee,
                        fiat_fee=fiat_fee,
                        notes=key,
                    )
                )
            elif record[_TYPE] == _TRADE and is_fiat_asset:
                # FIAT ledger entries with trade type ignored currently
                pass
            elif record[_TYPE] == _SETTLED:
                # ignorable in terms of in/out/intra
                pass
            elif record[_TYPE] == _CREDIT or record[_TYPE] == _STAKING or record[_TYPE] == _SALE:
                self.__logger.error(f"Unsupported transaction type: {record[_TYPE]} (skipping): %s. Please open an issue at %s",
                                    raw_data, self.ISSUES_URL)
            else:
                self.__logger.error(f"Unsupported transaction type: {record[_TYPE]} (skipping): %s. Please open an issue at %s",
                                    raw_data, self.ISSUES_URL)
                unhandled_types.update({record[_TYPE]: key})

            self.__logger.debug("unknown types of the ledger=%s", str(unhandled_types))

        return result

    def _process_implicit_api(self, in_transactions: List[InTransaction], out_transactions: List[OutTransaction],
                              intra_transactions: List[IntraTransaction]) -> None:
        pass

    def _process_trade_history(self, index: int = 0) -> Dict[str, Dict[str, Union[str, int, None, List[str]]]]:
        result: Dict[str, Dict[str, Union[str, int, None, List[str]]]] = {}
        params: Dict[str, Union[str, int]] = {_OFFSET: index}
        response = self._safe_api_call(
                    self._client.private_post_tradeshistory,
                    # self._client.fetch_my_trades, # UNIFIED CCXT API
                    {
                        'params': params,
                    },
        )
        # {
        #     "error": [
        #         "EGeneral:Invalid arguments"
        #     ]
        #     "result": {
        #         "count": 1,
        #         "trades": {
        #             "txid1": {
        #                 "ordertxid": "string",
        #                 "postxid": "string",
        #                 "pair": "string",
        #                 "time": 0,
        #                 "type": "string",
        #                 "ordertype": "string",
        #                 "price": "string",
        #                 "cost": "string",
        #                 "fee": "string",
        #                 "vol": "string",
        #                 "margin": "string",
        #                 "leverage": "string",
        #                 "misc": "string",
        #                 "trade_id": 0,
        #                 "posstatus": "string",
        #                 "cprice": null,
        #                 "ccost": null,
        #                 "cfee": null,
        #                 "cvol": null,
        #                 "cmargin": null,
        #                 "net": null,
        #                 "trades": [
        #                     "string"
        #                 ]
        #             },
        #         }
        #     },
        # }

        trade_history: Dict[str, Dict[str, Union[str, int, None, List[str]]]] = response[_RESULT][_TRADES]

        for key, value in trade_history.items():
            result.update({key: value})
        return result

    def _process_ledger(self, index: int = 0) -> Dict[str, Dict[str, Union[str, int, None, List[str]]]]:
        result: Dict[str, Dict[str, Union[str, int, None, List[str]]]] = {}
        params: Dict[str, Union[str, int]]  = {_OFFSET: index}
        response = self._safe_api_call(
                    self._client.private_post_ledgers,
                    # self._client.fetch_ledger, # UNIFIED CCXT API
                    # self._client.fetchLedger,  # UNIFIED CCXT API
                    {
                        'params': params,
                    },
        )
        # {
        #     "error": [
        #         "EGeneral:Invalid arguments"
        #     ]
        #     "result": {
        #         "count": 1
        #         "ledger": {
        #             "ledger_id1": {
        #                 "refid": "string",
        #                 "time": 0,
        #                 "type": "trade",
        #                 "subtype": "string",
        #                 "aclass": "string",
        #                 "asset": "string",
        #                 "amount": "string",
        #                 "fee": "string",
        #                 "balance": "string"
        #             },
        #         },
        #     },
        # }

        ledger: Dict[str, Dict[str, Union[str, int, None, List[str]]]] = response[_RESULT][_LEDGER]

        for key, value in ledger.items():
            result.update({key: value})
        return result
