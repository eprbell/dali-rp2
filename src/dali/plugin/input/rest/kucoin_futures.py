# Copyright 2022 topcoderasdf
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

import json
import time
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from ccxt import Exchange, RateLimitExceeded, kucoinfutures
from dateutil import tz

from dali.abstract_ccxt_input_plugin import AbstractCcxtInputPlugin
from dali.ccxt_pagination import AbstractPaginationDetailSet
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

_AMOUNT: str = "amount"
_CURRENCY: str = "currency"
_DATA: str = "data"
_DATA_LIST: str = "dataList"
_DEPOSIT: str = "Deposit"
_END_AT: str = "endAt"
_HAS_MORE: str = "hasMore"
_OFFSET: str = "offset"
_REALIZED_PNL: str = "RealizedPNL"
_START_AT: str = "startAt"
_TIME: str = "time"
_TRANSFER_IN: str = "TransferIn"
_TRANSFER_OUT: str = "TransferOut"
_TS_INCREMENT: int = 86400000
_TYPE: str = "type"
_WITHDRAWAL: str = "Withdrawal"

class InputPlugin(AbstractCcxtInputPlugin):

    __EXCHANGE_NAME: str = "kucoin_futures"
    __PLUGIN_NAME: str = "kucoin_futures"
    __DEFAULT_THREAD_COUNT: int = 1

    def __init__(
        self, account_holder: str, api_key: str, api_secret: str, api_passphrase: str, native_fiat: Optional[str] = None, thread_count: Optional[int] = None
    ) -> None:
        self.__api_key = api_key
        self.__api_secret = api_secret
        self.__api_passphrase = api_passphrase

        super().__init__(account_holder, datetime(2019, 2, 17, 0, 0, 0, 0), native_fiat, thread_count)

    def _initialize_client(self) -> kucoinfutures:
        return kucoinfutures(
            {
                "apiKey": self.__api_key,
                "secret": self.__api_secret,
                "password": self.__api_passphrase,
                "enableRateLimit": True,
            }
        )

    def exchange_name(self) -> str:
        return self.__EXCHANGE_NAME

    def plugin_name(self) -> str:
        return self.__PLUGIN_NAME

    @property
    def _client(self) -> kucoinfutures:
        super_client: Exchange = super()._client
        if not isinstance(super_client, kucoinfutures):
            raise TypeError("super_client is not of type kucoinfutures")

        return super_client

    # using kucoin transaction history api instead. (implicit api)
    def _get_process_deposits_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        return None

    def _get_process_withdrawals_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        return None

    def _get_process_trades_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        return None

    def _process_gains(
        self,
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
    ) -> None:
        pass

    def _fetch_data(
        self,
        start_at: int,
        end_at: int,
    ) -> List[Dict[str, Any]]:

        offset: int = 1
        retries: int = 0
        has_more: bool = False

        ledger_transactions: Dict[str, Any] = {}
        items: List[Dict[str, Any]] = []

        while (start_at <= end_at) and retries < 4:
            try:
                ledger_transactions = self._client.futuresPrivateGetTransactionHistory({_START_AT: start_at, _END_AT: start_at + _TS_INCREMENT})
                data_list = ledger_transactions[_DATA][_DATA_LIST]
                has_more = ledger_transactions[_DATA][_HAS_MORE]

                for item in data_list:
                    items.append(item)
                    offset = item[_OFFSET]

                # https://github.com/ccxt/ccxt/issues/10273
                # enableRateLimit is not working for kucoin
                time.sleep(0.15)
                retries = 0

                while has_more and (retries < 4):
                    try:
                        ledger_transactions = self._client.futuresPrivateGetTransactionHistory(
                            {_START_AT: start_at, _END_AT: start_at + _TS_INCREMENT, _OFFSET: offset}
                        )
                        data_list = ledger_transactions[_DATA][_DATA_LIST]
                        has_more = ledger_transactions[_DATA][_HAS_MORE]

                        for item in data_list:
                            items.append(item)
                            offset = item[_OFFSET]

                        time.sleep(0.15)

                    except RateLimitExceeded:
                        self._client.sleep(13000)
                        retries += 1

                if retries >= 4:
                    raise Exception("Failed to fetch ledger transactions after 4 retries")

                start_at += _TS_INCREMENT
                offset = 1
                has_more = False
                retries = 0

            except RateLimitExceeded:
                self._client.sleep(13000)
                retries += 1

            except Exception as exception:
                raise exception

        if retries >= 4:
            raise Exception("Failed to fetch ledger transactions after 4 retries")

        return items

    def _process_data(
        self,
        items: List[Dict[str, Any]],
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
        # intra_transactions: List[IntraTransaction],
    ) -> None:
        for item in items:
            timestamp_value: int = item[_TIME]
            timestamp: str = datetime.fromtimestamp(float(timestamp_value) / 1000.0, tz.tzutc()).strftime("%Y-%m-%d %H:%M:%S.%f%z")
            transaction_type: str = item[_TYPE]
            amount: str = str(item[_AMOUNT])
            currency: str = item[_CURRENCY]
            offset: int = item[_OFFSET]

            # internal tranfer between kucoin and kucoin futures. Ignore
            if transaction_type in {_TRANSFER_IN, _TRANSFER_OUT}:
                continue

            # To do: Currently Kucoin futures allows BTC and USDT deposits / withdrawals through blockchain wallets
            if transaction_type in {_DEPOSIT, _WITHDRAWAL}:
                pass

            elif transaction_type == _REALIZED_PNL:
                amount_value = Decimal(amount)

                # no internal txn id provided by kucoin futures
                # unique id manually generated
                unique_id = f"realized_pnl:{self.exchange_name}:{self.account_holder}:{timestamp}:{offset}"

                if amount_value > 0:
                    # Gain
                    # Example json
                    # {
                    #   "time": 1644444400000,
                    #   "type": "RealisedPNL",
                    #   "amount": 66.66666666,
                    #   "fee": 0.0,
                    #   "accountEquity": 12345.67890123,
                    #   "status": "Completed",
                    #   "remark": "ETHUSDTM",
                    #   "offset": 123456,
                    #   "currency": "USDT"
                    # }

                    # To do: need to replace tranaction type from GIFT to GAIN
                    # GAIN currently not implemented in rp2
                    in_transactions.append(
                        InTransaction(
                            plugin=self.plugin_name(),
                            unique_id=unique_id,
                            raw_data=json.dumps(item),
                            timestamp=timestamp,
                            asset=currency,
                            exchange=self.exchange_name(),
                            holder=self.account_holder,
                            transaction_type=Keyword.GIFT.name,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_in=str(amount_value),
                            crypto_fee=None,
                            fiat_in_no_fee=None,
                            fiat_in_with_fee=None,
                            fiat_fee=None,
                            notes=None,
                        )
                    )

                else:
                    # Loss
                    # Example json
                    # {
                    #   "time": 1644444400000,
                    #   "type": "RealisedPNL",
                    #   "amount": -66.66666666,
                    #   "fee": 0.0,
                    #   "accountEquity": 12345.67890123,
                    #   "status": "Completed",
                    #   "remark": "ETHUSDTM",
                    #   "offset": 123456,
                    #   "currency": "USDT"
                    # }

                    # To do: need to replace tranaction type from FEE to LOSS
                    # LOSS currently not implemented in rp2
                    out_transactions.append(
                        OutTransaction(
                            plugin=self.plugin_name(),
                            unique_id=unique_id,
                            raw_data=json.dumps(item),
                            timestamp=timestamp,
                            asset=currency,
                            exchange=self.exchange_name(),
                            holder=self.account_holder,
                            transaction_type=Keyword.FEE.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_out_no_fee="0",
                            crypto_fee=str(-amount_value),
                            crypto_out_with_fee=str(-amount_value),
                            fiat_out_no_fee=None,
                            fiat_fee=None,
                            notes=None,
                        )
                    )

    def _process_implicit_api(
        self,
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
        intra_transactions: List[IntraTransaction],
    ) -> None:
        in_transactions.clear()
        out_transactions.clear()
        intra_transactions.clear()

        end_at: int = int(time.time() * 1000)
        start_at: int = self._start_time_ms

        items: List[Dict[str, Any]] = self._fetch_data(start_at, end_at)
        self._process_data(items, in_transactions, out_transactions)
