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

# Binance.com REST plugin links:
# REST API: https://binance-docs.github.io/apidocs/
# Authentication: https://binance-docs.github.io/apidocs/spot/en/#introduction
# Endpoint: https://api.binance.com

# CCXT documentation:
# https://docs.ccxt.com/en/latest/index.html

# pylint: disable=fixme

import json
import logging
from datetime import datetime
from typing import List, NamedTuple, Optional

from ccxt import Exchange, binance

from dali.abstract_ccxt_input_plugin import (
    AbstractCcxtInputPlugin,
    AbstractPaginationDetailSet,
    DateBasedPaginationDetailSet,
)
from dali.abstract_transaction import AbstractTransaction
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# Native format keywords
_ACCOUNT_PROFITS: str = "accountProfits"
_ALGO: str = "algo"
_ALGO_NAME: str = "algoName"
_AMOUNT: str = "amount"
_ASSET: str = "asset"
_BEGIN_TIME: str = "beginTime"
_BUY: str = "buy"  # CCXT only variable
_COIN: str = "coin"
_COIN_NAME: str = "coinName"
_COST: str = "cost"  # CCXT only variable
_CREATE_TIME: str = "createTime"
_CRYPTOCURRENCY: str = "cryptoCurrency"
_CURRENCY: str = "currency"  # CCXT only variable
_DAILY: str = "DAILY"
_DATA: str = "data"
_DATE_TIME: str = "datetime"  # CCXT only variable
_DELIVER_DATE: str = "deliverDate"
_DEPOSIT: str = "deposit"  # CCXT only variable
_DIV_TIME: str = "divTime"
_END_TIME: str = "endTime"
_EN_INFO: str = "enInfo"
_FEE: str = "fee"
_FIAT_CURRENCY: str = "fiatCurrency"
_ID: str = "id"  # CCXT only variable
_INDICATED_AMOUNT: str = "indicatedAmount"
_INFO: str = "info"
_INSERT_TIME: str = "insertTime"
_INTEREST_PARAMETER: str = "INTEREST"
_INTEREST_FIELD: str = "interest"
_IS_DUST: str = "isDust"
_IS_FIAT_PAYMENT: str = "isFiatPayment"
_LEGAL_MONEY: str = "legalMoney"
_LENDING_TYPE: str = "lendingType"
_LIMIT: str = "limit"
_LOCK_PERIOD: str = "lockPeriod"
_OBTAIN_AMOUNT: str = "obtainAmount"
_ORDER: str = "order"  # CCXT only variable
_ORDER_NO: str = "orderNo"
_PAGE_INDEX: str = "pageIndex"
_PAGE_SIZE: str = "pageSize"
_POSITION_ID: str = "positionId"
_PRICE: str = "price"
_PRODUCT: str = "product"
_PROFIT_AMOUNT: str = "profitAmount"
_REDEMPTION: str = "REDEMPTION"
_ROWS: str = "rows"
_SELL: str = "sell"  # CCXT only variable
_SIDE: str = "side"  # CCXT only variable
_SIZE: str = "size"
_STAKING: str = "STAKING"
_START_TIME: str = "startTime"
_STATUS: str = "status"
_SOURCE_AMOUNT: str = "sourceAmount"
_SUBSCRIPTION: str = "SUBSCRIPTION"
_SYMBOL: str = "symbol"
_TIME: str = "time"
_TIMESTAMP: str = "timestamp"  # CCXT only variable
_TRAN_ID: str = "tranId"
_TRANSACTION_TYPE: str = "transactionType"
_TOTAL: str = "total"
_TOTAL_FEE: str = "totalFee"
_TOTAL_NUM: str = "totalNum"
_TYPE: str = "type"
_TX_ID: str = "txid"  # CCXT doesn't capitalize I
_TXN_TYPE: str = "txnType"
_UPDATE_TIME: str = "updateTime"
_USERNAME: str = "userName"
_WITHDRAWAL: str = "withdrawal"  # CCXT only variable

# Time period constants
_NINETY_DAYS_IN_MS: int = 7776000000
_THIRTY_DAYS_IN_MS: int = 2592000000
_ONE_DAY_IN_MS: int = 86400000
_MS_IN_SECOND: int = 1000

# Record limits
_DEPOSIT_RECORD_LIMIT: int = 1000
_DIVIDEND_RECORD_LIMIT: int = 500
_DUST_TRADE_RECORD_LIMIT: int = 100
_INTEREST_SIZE_LIMIT: int = 100
_MINING_PAGE_LIMIT: int = 200
_TRADE_RECORD_LIMIT: int = 1000
_WITHDRAWAL_RECORD_LIMIT: int = 1000

# Types of Binance Dividends
_BNB_VAULT = "BNB Vault"
_ETH_STAKING = "ETH 2.0 Staking"
_FLEXIBLE_SAVINGS = "Flexible Savings"
_LAUNCH_POOL = "Launchpool"
_LOCKED_SAVINGS = "Locked Savings"
_LOCKED_STAKING = "Locked Staking"
_SOLO_AIRDROP = "SOLO airdrop"
_GENERAL_STAKING = "STAKING"

_AIRDROP_LIST = [_SOLO_AIRDROP]
_INTEREST_LIST = [_FLEXIBLE_SAVINGS, _LOCKED_SAVINGS]
_STAKING_LIST = [_ETH_STAKING, _LOCKED_STAKING, _BNB_VAULT, _LAUNCH_POOL, _GENERAL_STAKING]


class _ProcessAccountResult(NamedTuple):
    in_transactions: List[InTransaction]
    out_transactions: List[OutTransaction]
    intra_transactions: List[IntraTransaction]


class _Trade(NamedTuple):
    base_asset: str
    quote_asset: str
    base_info: str
    quote_info: str


class InputPlugin(AbstractCcxtInputPlugin):

    __EXCHANGE_NAME: str = "Binance.com"
    __PLUGIN_NAME: str = "Binance.com_REST"

    def __init__(
        self,
        account_holder: str,
        api_key: str,
        api_secret: str,
        native_fiat: str,
        username: Optional[str] = None,
    ) -> None:

        self.__api_key = api_key
        self.__api_secret = api_secret
        super().__init__(account_holder, datetime(2017, 7, 13, 0, 0, 0, 0), native_fiat)
        self.__username = username

        # We have to know what markets and algos are on Binance so that we can pull orders using the market
        self.__algos: List[str] = []

    def exchange_name(self) -> str:
        return self.__EXCHANGE_NAME

    def cache_key(self) -> Optional[str]:
        return self.__cache_key

    def plugin_name(self) -> str:
        return self.__PLUGIN_NAME

    def logger(self) -> logging.Logger:
        return self.__logger

    def initialize_client(self) -> Exchange:
        return binance(
            {
                "apiKey": self.__api_key,
                "enableRateLimit": True,
                "secret": self.__api_secret,
            }
        )

    def get_process_deposits_pagination_detail_set(self) -> AbstractPaginationDetailSet:
        #        raise NotImplementedError("Abstract method")
        pass

    def get_process_withdrawals_pagination_detail_set(self) -> AbstractPaginationDetailSet:
        #        raise NotImplementedError("Abstract method")
        pass

    def get_process_trades_pagination_detail_set(self) -> AbstractPaginationDetailSet:
        return DateBasedPaginationDetailSet(
            limit=_TRADE_RECORD_LIMIT,
            exchange_start_time=self.start_time_ms,
            markets=self.markets,
        )

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []
        in_transactions: List[InTransaction] = []
        out_transactions: List[OutTransaction] = []
        intra_transactions: List[IntraTransaction] = []

        if self.__username:
            binance_algos = self.client.sapiGetMiningPubAlgoList()  # type: ignore
            for algo in binance_algos[_DATA]:
                self.__logger.debug("Algo: %s", json.dumps(algo))
                self.__algos.append(algo[_ALGO_NAME])

        self._process_trades(in_transactions, out_transactions)

        result.extend(in_transactions)
        result.extend(out_transactions)
        result.extend(intra_transactions)

        return result
