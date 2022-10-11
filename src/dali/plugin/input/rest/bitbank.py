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

# Bitbank REST plugin links:
# REST API: https://docs.bitbank.cc/
# Authentication: https://github.com/bitbankinc/bitbank-api-docs/blob/master/rest-api.md#authorization
# Endpoint: https://bitbank.cc/

# CCXT documentation:
# https://docs.ccxt.com/en/latest/index.html

from datetime import datetime
from typing import List, Optional

from ccxt import bitbank

from dali.abstract_ccxt_input_plugin import AbstractCcxtInputPlugin
from dali.ccxt_pagination import (
    AbstractPaginationDetailSet,
    DateBasedPaginationDetailSet,
)
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# Time period constants
_MS_IN_SECOND: int = 1000

# Record limits
_TRADE_RECORD_LIMIT: int = 1000


class InputPlugin(AbstractCcxtInputPlugin):

    __EXCHANGE_NAME: str = "Bitbank.cc"
    __PLUGIN_NAME: str = "Bitbank.cc_REST"
    __DEFAULT_THREAD_COUNT: int = 1

    def __init__(
        self,
        account_holder: str,
        api_key: str,
        api_secret: str,
        native_fiat: str,
        thread_count: Optional[int] = __DEFAULT_THREAD_COUNT,
    ) -> None:

        self.__api_key = api_key
        self.__api_secret = api_secret
        # We will have a default start time of March 1st, 2017 since Bitbank Exchange officially launched on March 1st Japan Time.
        super().__init__(account_holder, datetime(2017, 3, 1, 0, 0, 0, 0), native_fiat, thread_count)

    def exchange_name(self) -> str:
        return self.__EXCHANGE_NAME

    def plugin_name(self) -> str:
        return self.__PLUGIN_NAME

    def _initialize_client(self) -> bitbank:
        return bitbank(
            {
                "apiKey": self.__api_key,
                "enableRateLimit": True,
                "secret": self.__api_secret,
            }
        )

    def _get_process_deposits_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        return None

    def _get_process_withdrawals_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        return None

    def _get_process_trades_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        return DateBasedPaginationDetailSet(
            limit=_TRADE_RECORD_LIMIT,
            exchange_start_time=self._start_time_ms,
            markets=self._get_markets(),
        )

    def _process_gains(
        self,
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
    ) -> None:
        pass

    def _process_implicit_api(
        self,
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
        intra_transactions: List[IntraTransaction],
    ) -> None:
        pass
