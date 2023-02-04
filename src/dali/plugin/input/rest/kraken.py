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

# Binance.com REST plugin links:
# REST API: https://docs.kraken.com/rest/
# Authentication: https://docs.kraken.com/rest/#section/Authentication
# Endpoint: https://api.kraken.com

# CCXT documentation:
# https://docs.ccxt.com/en/latest/index.html

import pytz
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union

from ccxt import Exchange, kraken

from dali.abstract_ccxt_input_plugin import (
    AbstractCcxtInputPlugin,
    _MS_IN_SECOND,
)
from dali.ccxt_pagination import (
    AbstractPaginationDetailSet,
)
from rp2.logger import create_logger
from dali.abstract_transaction import AbstractTransaction
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction
from dali.configuration import Keyword, _FIAT_SET
from dali.cache import load_from_cache, save_to_cache


# keywords
_OUT: str = "out"
_IN: str = "in"
_INTRA: str = 'intra'
_RESULT: str = 'result'
_COUNT: str = 'count'
_LEDGER: str = 'ledger'
_TRADES: str = 'trades'
_OFFSET: str = 'ofs'
_TYPE: str = 'type'
_REFID: str = 'refid'
_TIMESTAMP: str = 'time'
_FEE: str = 'fee'
_ASSET: str = 'asset'
_BASE: str = 'base'
_BASE_ID: str = 'baseId'
_AMOUNT: str = 'amount'
_COST: str = 'cost'
_PRICE: str = 'price'
_WITHDRAWAL: str = 'withdrawal'
_DEPOSIT: str = 'deposit'
_MARGIN: str = 'margin'
_TRADE: str = 'trade'
_ROLLOVER: str = 'rollover'
_TRANSFER: str = 'transfer'
_SETTLED: str = 'settled'
_CREDIT: str = 'credit'
_STAKING: str = 'staking'
_SALE: str = 'sale'

# Record Limits
_TRADE_RECORD_LIMIT: int = 50


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
        username: Optional[str] = None,
        thread_count: Optional[int] = __DEFAULT_THREAD_COUNT,
        use_cache: Optional[bool] = True,
    ) -> None:
        self.__api_key = api_key
        self.__api_secret = api_secret

        # We will have a default start time of July 27th, 2011 since Kraken Exchange officially launched on July 28th.
        super().__init__(account_holder, datetime(2011, 7, 27, 0, 0, 0, 0), native_fiat, thread_count)
        self.__username = username
        self.__logger: logging.Logger = create_logger(f"{self.__EXCHANGE_NAME}")
        self.__timezone = pytz.timezone('UTC')
        self._initialize_client()
        self._client.load_markets()
        self.baseId_to_base = {value[_BASE_ID]: value[_BASE] for key, value in self._client.markets_by_id.items()}
        self.baseId_to_base.update({'BSV': 'BSV'})
        self.use_cache = use_cache

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
            raise Exception("Exchange is not instance of class kraken.")
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

        index = 0
        count: int = int(self._client.private_post_ledgers(params={_OFFSET: index})[_RESULT][_COUNT])
        ledger: Dict[str, Dict[str, Union[str, int, None, List[str]]]] = {}
        while index < count:
            ledger.update(self._process_ledger(index))
            index += _TRADE_RECORD_LIMIT

        result = (trade_history, ledger)

        if self.use_cache:
            save_to_cache(self.__CACHE_FILE, result)

        return result

    def load(self) -> List[AbstractTransaction]:
        (trade_history, ledger) = self._gather_api_data()

        result = {'in': [], 'out': [], 'intra': []}

        unhandled_types = dict()
        for key, value in ledger.items():
            if value[_TYPE] == _WITHDRAWAL or value[_TYPE] == _DEPOSIT:
                result[_INTRA].append(key)
            elif value[_TYPE] == _TRADE and 'USD' not in value[_ASSET]:
                if float(value[_AMOUNT]) > 0: result[_IN].append(key)
                else: result[_OUT].append(key)
            elif value[_TYPE] == _MARGIN:
                result[_OUT].append(key)
            elif value[_TYPE] == _ROLLOVER:
                result[_OUT].append(key)
            elif value[_TYPE] == _TRANSFER:
                result[_IN].append(key)
            elif value[_TYPE] == _SETTLED:
                # ignorable in terms of in/out/intra
                pass
            elif value[_TYPE] == _CREDIT:
                # 'credit not implemented'
                pass
            elif value[_TYPE] == _STAKING:
                # 'credit not implemented'
                pass
            elif value[_TYPE] == _SALE:
                # 'credit not implemented'
                pass
            else:
                unhandled_types.update({value[_TYPE]: key})
        self.__logger.debug(f"unhandled types of the ledger={unhandled_types}")

        return self._compute_tx_set(trade_history, ledger, result)

    def _compute_tx_set(self, trade_history, ledger, processed_transactions) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []

        transactions = processed_transactions[_IN] + processed_transactions[_OUT] + processed_transactions[_INTRA]
        for key in transactions:
            record = ledger[key]
            timestamp_value: str = self._rp2_timestamp_from_ms_epoch(float(record[_TIMESTAMP])*_MS_IN_SECOND)

            is_fiat_asset = record[_ASSET] in _FIAT_SET or 'USD' in record[_ASSET]

            amount = abs(float(record[_AMOUNT]))
            asset = self.baseId_to_base[record[_ASSET]]
            kwargs = {
                'unique_id': Keyword.UNKNOWN.value,
                'plugin': self.__PLUGIN_NAME,
                'raw_data': str(record),
                'timestamp': str(timestamp_value),
                'spot_price': '0',
                'asset': asset,
                'notes': key,
            }

            if record[_TYPE] == _WITHDRAWAL or record[_TYPE] == _DEPOSIT:
                is_deposit = record[_TYPE] == _DEPOSIT
                is_withdrawal = record[_TYPE] == _WITHDRAWAL

                # Intra
                kwargs.update(
                    {
                        'unique_id': Keyword.UNKNOWN.value,
                        'from_exchange': self.__EXCHANGE_NAME if is_withdrawal else Keyword.UNKNOWN.value,
                        'from_holder': self.account_holder if is_withdrawal else Keyword.UNKNOWN.value,
                        'to_exchange': self.__EXCHANGE_NAME if is_deposit else Keyword.UNKNOWN.value,
                        'to_holder': self.account_holder if is_deposit else Keyword.UNKNOWN.value,
                        'crypto_sent': Keyword.UNKNOWN.value if is_deposit else str(amount),
                        'crypto_received': str(amount) if is_deposit else Keyword.UNKNOWN.value,
                    }
                )
                result.append(IntraTransaction(**kwargs))
                continue

            kwargs.update({
                'exchange': self.__EXCHANGE_NAME,
                'holder': self.account_holder,
                'transaction_type': Keyword.BUY.value if float(record[_AMOUNT]) > 0 else Keyword.SELL.value,
                'crypto_fee': '0' if is_fiat_asset else record[_FEE],
                'fiat_fee': record[_FEE] if is_fiat_asset else None,
            })

            if record[_TYPE] == _TRADE and 'USD' not in record[_ASSET]:
                kwargs.update({
                    'spot_price': trade_history[record[_REFID]][_PRICE],
                })
                if float(record[_AMOUNT]) > 0:
                    kwargs.update({
                        'crypto_in': str(amount),
                        'fiat_in_no_fee': str(float(trade_history[record[_REFID]][_COST]) - float(
                            trade_history[record[_REFID]][_FEE])),
                        'fiat_in_with_fee': trade_history[record[_REFID]][_COST],
                    })
                    result.append(InTransaction(**kwargs))
                else:
                    kwargs.update({
                        'crypto_out_no_fee': str(amount),
                        'crypto_out_with_fee': str(amount + float(record[_FEE])),
                        'fiat_out_no_fee': str(float(trade_history[record[_REFID]][_COST]) - float(
                            trade_history[record[_REFID]][_FEE])),
                        'is_spot_price_from_web': False,
                    })
                    result.append(OutTransaction(**kwargs))
            elif record[_TYPE] == _MARGIN or record[_TYPE] == _ROLLOVER:
                kwargs.update({
                    'transaction_type': Keyword.SELL.value,
                    'crypto_out_no_fee': str(amount),
                    'crypto_out_with_fee': str(amount + float(record[_FEE])),
                    'fiat_out_no_fee': str(float(trade_history[record[_REFID]][_COST]) - float(
                        trade_history[record[_REFID]][_FEE])),
                    'is_spot_price_from_web': False,
                })
                result.append(OutTransaction(**kwargs))
            elif record[_TYPE] == _TRANSFER:
                kwargs.update({
                    'transaction_type': Keyword.BUY.value,
                    'crypto_in': str(amount),
                    'fiat_in_no_fee': '0',
                    'fiat_in_with_fee': '0',
                })
                result.append(InTransaction(**kwargs))
            elif record[_TYPE] == _SETTLED:
                # ignorable in terms of in/out/intra
                pass
            elif record[_TYPE] == _CREDIT:
                # 'credit not implemented'
                pass
            elif record[_TYPE] == _STAKING:
                # 'credit not implemented'
                pass
            elif record[_TYPE] == _SALE:
                # 'credit not implemented'`
                pass
            else:
                raise BaseException(f"Unimplemented=record_type{record[_TYPE]}")
        return result

    def _process_trade_history(self, index: int = 0):
        result = dict()
        params = {_OFFSET: index}
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

        trade_history = response[_RESULT][_TRADES]

        for key,value in trade_history.items():
            result.update({key: value})
        return result

    def _process_ledger(self, index: int = 0):
        result = dict()
        params = {_OFFSET: index}
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

        ledger = response[_RESULT][_LEDGER]

        for key,value in ledger.items():
            result.update({key: value})
        return result
