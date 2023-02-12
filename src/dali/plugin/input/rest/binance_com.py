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

# pylint: disable=too-many-lines

import json
import re
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import Any, Dict, List, Optional, Union

from ccxt import Exchange, binance
from rp2.rp2_decimal import ZERO, RP2Decimal
from rp2.rp2_error import RP2RuntimeError

from dali.abstract_ccxt_input_plugin import (
    AbstractCcxtInputPlugin,
    ProcessOperationResult,
    Trade,
)
from dali.ccxt_pagination import (
    AbstractPaginationDetailSet,
    DateBasedPaginationDetailSet,
)
from dali.configuration import Keyword
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
_FLEXIBLE = "Flexible"
_FLEXIBLE_SAVINGS = "Flexible Savings"
_LAUNCH_POOL = "Launchpool"
_LOCKED = "Locked"
_LOCKED_SAVINGS = "Locked Savings"
_LOCKED_STAKING = "Locked Staking"
_SOLO_AIRDROP = "SOLO airdrop"
_GENERAL_STAKING = "STAKING"

_AIRDROP_LIST = [_SOLO_AIRDROP]
_INTEREST_LIST = [_FLEXIBLE, _FLEXIBLE_SAVINGS, _LOCKED, _LOCKED_SAVINGS]
_STAKING_LIST = [_ETH_STAKING, _LOCKED_STAKING, _BNB_VAULT, _LAUNCH_POOL, _GENERAL_STAKING]


class InputPlugin(AbstractCcxtInputPlugin):

    __EXCHANGE_NAME: str = "Binance.com"
    __PLUGIN_NAME: str = "Binance.com_REST"
    __DEFAULT_THREAD_COUNT: int = 1

    def __init__(
        self,
        account_holder: str,
        api_key: str,
        api_secret: str,
        native_fiat: str,
        username: Optional[str] = None,
        thread_count: Optional[int] = __DEFAULT_THREAD_COUNT,
    ) -> None:

        self.__api_key = api_key
        self.__api_secret = api_secret
        # We will have a default start time of July 13th, 2017 since Binance Exchange officially launched on July 14th Beijing Time.
        super().__init__(account_holder, datetime(2017, 7, 13, 0, 0, 0, 0), native_fiat, thread_count)
        self.__username = username

        # We have to know what markets and algos are on Binance so that we can pull orders using the market
        self.__algos: List[str] = []

    def exchange_name(self) -> str:
        return self.__EXCHANGE_NAME

    def plugin_name(self) -> str:
        return self.__PLUGIN_NAME

    def _initialize_client(self) -> binance:
        return binance(
            {
                "apiKey": self.__api_key,
                "enableRateLimit": True,
                "secret": self.__api_secret,
            }
        )

    @property
    def _client(self) -> binance:
        super_client: Exchange = super()._client
        if not isinstance(super_client, binance):
            raise RP2RuntimeError("Exchange is not instance of class binance.")
        return super_client

    def _get_algos(self) -> List[str]:
        if self.__algos:
            return self.__algos
        if self.__username:
            binance_algos = self._client.sapiGetMiningPubAlgoList()
            for algo in binance_algos[_DATA]:
                self._logger.debug("Algo: %s", json.dumps(algo))
                self.__algos.append(algo[_ALGO_NAME])
            return self.__algos
        return []

    def _get_process_deposits_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        return DateBasedPaginationDetailSet(
            limit=_DEPOSIT_RECORD_LIMIT,
            exchange_start_time=self._start_time_ms,
            window=_NINETY_DAYS_IN_MS,
        )

    def _get_process_withdrawals_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        return DateBasedPaginationDetailSet(
            limit=_WITHDRAWAL_RECORD_LIMIT,
            exchange_start_time=self._start_time_ms,
            window=_NINETY_DAYS_IN_MS,
        )

    def _get_process_trades_pagination_detail_set(self) -> Optional[AbstractPaginationDetailSet]:
        return DateBasedPaginationDetailSet(
            limit=_TRADE_RECORD_LIMIT,
            exchange_start_time=self._start_time_ms,
            markets=self._get_markets(),
        )

    ### Multiple transaction processing

    def _process_gains(
        self,
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
    ) -> None:

        ### Regular Dividends from Staking (including Eth staking) and Savings (Lending) after around May 8th, 2021 01:00 UTC.

        # We need milliseconds for Binance
        current_start = self._start_time_ms
        now_time = int(datetime.now().timestamp()) * _MS_IN_SECOND
        processing_result_list: List[Optional[ProcessOperationResult]] = []

        # The exact moment when Binance switched to unified dividends is unknown/unpublished.
        # This allows us an educated guess.
        earliest_record_epoch: int = 0

        # We will pull in 30 day periods. This allows for 16 assets with daily dividends.
        current_end = current_start + _THIRTY_DAYS_IN_MS

        while current_start < now_time:
            self._logger.debug("Pulling dividends/subscriptions/redemptions from %s to %s", current_start, current_end)

            # CCXT doesn't have a standard way to pull income, we must use the underlying API endpoint
            dividends = self._client.sapiGetAssetAssetDividend(params=({_START_TIME: current_start, _END_TIME: current_end, _LIMIT: _DIVIDEND_RECORD_LIMIT}))
            # {
            #     "rows":[
            #         {
            #             "id":1637366104,
            #             "amount":"10.00000000",
            #             "asset":"BHFT",
            #             "divTime":1563189166000,
            #             "enInfo":"BHFT distribution",
            #             "tranId":2968885920
            #         },
            #         {
            #             "id":1631750237,
            #             "amount":"10.00000000",
            #             "asset":"BHFT",
            #             "divTime":1563189165000,
            #             "enInfo":"BHFT distribution",
            #             "tranId":2968885920
            #         }
            #     ],
            #     "total":2
            # }

            self._logger.debug("Pulled a total of %s records for %s to %s", dividends[_TOTAL], current_start, current_end)

            # If user received more than 500 dividends in a 30 day period we need to shrink the window.
            if int(dividends[_TOTAL]) <= _DIVIDEND_RECORD_LIMIT:
                current_start = current_end + 1
                current_end = current_start + _THIRTY_DAYS_IN_MS
                with ThreadPool(self._thread_count) as pool:
                    processing_result_list = pool.map(self._process_dividend, dividends[_ROWS])

                for processing_result in processing_result_list:
                    if processing_result is None:
                        continue
                    if processing_result.in_transactions:
                        in_transactions.extend(processing_result.in_transactions)
            else:
                # Using implicit API so we need to follow Binance order, which sends latest record first ([0])
                # CCXT standard API sorts by timestamp, so latest record is last ([499])
                number_of_excess_records = int(dividends[_TOTAL]) - _DIVIDEND_RECORD_LIMIT
                current_end = int(dividends[_ROWS][number_of_excess_records][_DIV_TIME])  # times are inclusive
                self._logger.debug("Readjusting time window end to %s from %s", current_end, current_start + _THIRTY_DAYS_IN_MS)
                # current_end = current_start + _THIRTY_DAYS_IN_MS

            if not earliest_record_epoch and int(dividends[_TOTAL]) > 0:
                earliest_record_epoch = int(dividends[_ROWS][-1][_DIV_TIME]) - 1

            # We need to track subscription and redemption amounts since Binance will take a fee equal to the amount of interest
            # earned during the lock period if the user prematurely redeems their funds.

        # Old system Locked Savings

        old_savings: bool = False

        # Reset window
        current_start = self._start_time_ms
        current_end = current_start + _THIRTY_DAYS_IN_MS

        # The cummulative interest from a positionID
        total_current_interest: Dict[int, RP2Decimal] = {}

        # The cummulative interest payments made to each positionID
        total_current_payments: Dict[int, int] = {}

        # Subscriptions organized [asset][amount] = timestamp in milliseconds
        current_subscriptions: Dict[str, Dict[str, Dict[str, str]]] = {}

        # We will step backward in time from the switch over
        while current_start < now_time:

            self._logger.debug("Pulling locked staking from older api system from %s to %s", current_start, current_end)

            locked_staking = self._client.sapi_get_staking_stakingrecord(
                params=({_START_TIME: current_start, _END_TIME: current_end, _PRODUCT: _STAKING, _TXN_TYPE: _INTEREST_PARAMETER, _SIZE: _INTEREST_SIZE_LIMIT})
            )
            # [
            #   {
            #       'positionId': '7146912',
            #       'time': '1624233772000',
            #       'asset': 'BTC',
            #       'amount': '0.017666',
            #       'status': 'SUCCESS'
            #   },
            #   {
            #       'positionId': '7147052',
            #       'time': '1624147893000',
            #       'asset': 'BTC',
            #       'amount': '0.0176665',
            #       'status': 'SUCCESS'
            #   }
            # ]
            # NOTE: All values are str
            processing_result_list = []
            for stake_dividend in locked_staking:
                if int(stake_dividend[_TIME]) < earliest_record_epoch:
                    (self._logger).debug("Locked Staking (OLD): %s", json.dumps(stake_dividend))
                    stake_dividend[_EN_INFO] = "Locked Staking/Savings (OLD)"
                    stake_dividend[_ID] = Keyword.UNKNOWN.value
                    stake_dividend[_DIV_TIME] = stake_dividend[_TIME]
                    processing_result_list.append(self._process_gain(stake_dividend, Keyword.STAKING))
                    old_savings = True

                # Early redemption penalty tracking. Needs to be recorded even for new system.
                position_id: int = stake_dividend[_POSITION_ID]
                total_current_interest[position_id] = total_current_interest.get(position_id, ZERO) + RP2Decimal(str(stake_dividend[_AMOUNT]))
                total_current_payments[position_id] = total_current_payments.get(position_id, 0) + 1

            for processing_result in processing_result_list:
                if processing_result is None:
                    continue
                if processing_result.in_transactions:
                    in_transactions.extend(processing_result.in_transactions)

            locked_subscriptions = self._client.sapi_get_staking_stakingrecord(
                params=({_START_TIME: current_start, _END_TIME: current_end, _PRODUCT: _STAKING, _TXN_TYPE: _SUBSCRIPTION, _SIZE: _INTEREST_SIZE_LIMIT})
            )
            # [
            #     {
            #         "time": "1624147893000",
            #         "asset": "BTC",
            #         "amount": "1",
            #         "lockPeriod": "10",
            #         "type": "NORMAL",
            #         "status": "SUCCESS",
            #     },
            #     {
            #         "time": "1624147893000",
            #         "asset": "BTC",
            #         "amount": "1",
            #         "lockPeriod": "10",
            #         "type": "NORMAL",
            #         "status": "SUCCESS",
            #     }
            # ]
            # NOTE: all values are str

            for subscription in locked_subscriptions:

                # If the dict already exists add another key, if not add new dict
                if current_subscriptions.get(subscription[_ASSET]):
                    current_subscriptions[subscription[_ASSET]][f"{RP2Decimal(subscription[_AMOUNT]):.13f}"] = subscription
                else:
                    current_subscriptions[subscription[_ASSET]] = {f"{RP2Decimal(subscription[_AMOUNT]):.13f}": subscription}

            locked_redemptions = self._client.sapi_get_staking_stakingrecord(
                params=({_START_TIME: current_start, _END_TIME: current_end, _PRODUCT: _STAKING, _TXN_TYPE: _REDEMPTION, _SIZE: _INTEREST_SIZE_LIMIT})
            )
            # [
            #         {
            #             "positionId": "12345",
            #             "time": "1624147893000"
            #             "asset": "BTC",
            #             "amount": "1",
            #             "deliverDate": "1624147895000"
            #             "status": "PAID",
            #         },
            #         {
            #             "positionId": "12346",
            #             "time": "1624147993000"
            #             "asset": "BTC",
            #             "amount": "0.95",
            #             "deliverDate": "1624148093000"
            #             "status": "PAID",
            #         }
            # ]
            # NOTE: all values are str

            for redemption in locked_redemptions:

                redemption_amount: str = f"{RP2Decimal(redemption[_AMOUNT]):.13f}"

                # Check if there is a subscription with this asset and if the redemption amount is equal to the subscription amount
                if redemption[_ASSET] in current_subscriptions and redemption_amount not in current_subscriptions[redemption[_ASSET]]:

                    # If they do not equal we need to calculate what the amended principal should be based on total interest paid to that productId
                    total_interest_earned: RP2Decimal = total_current_interest[redemption[_POSITION_ID]]
                    original_principal: str = f"{(RP2Decimal(redemption[_AMOUNT]) + RP2Decimal(str(total_interest_earned))):.13f}"
                    earliest_redemption_timestamp: int = 0

                    if str(original_principal) in current_subscriptions[redemption[_ASSET]]:

                        subscription_time: int = int(current_subscriptions[redemption[_ASSET]][str(original_principal)][_TIME])
                        lockperiod_in_ms: int = int(current_subscriptions[redemption[_ASSET]][str(original_principal)][_LOCK_PERIOD]) * _ONE_DAY_IN_MS
                        earliest_redemption_timestamp = subscription_time + lockperiod_in_ms

                    else:
                        raise RP2RuntimeError(
                            f"Internal Error: Principal ({original_principal}) minus paid interest ({RP2Decimal(str(total_interest_earned))}) does not equal"
                            f" returned principal ({RP2Decimal(redemption[_AMOUNT])}) on locked savings position ID - {redemption[_POSITION_ID]}."
                        )

                    # There is some lag time between application for the subscription and when the subscription actually starts ~ 2 days
                    if (int(redemption[_TIME]) - int(earliest_redemption_timestamp)) < 2 * _ONE_DAY_IN_MS:
                        out_transactions.append(
                            OutTransaction(
                                plugin=self.__PLUGIN_NAME,
                                unique_id=Keyword.UNKNOWN.value,
                                raw_data=json.dumps(redemption),
                                timestamp=self._rp2_timestamp_from_ms_epoch(redemption[_DELIVER_DATE]),
                                asset=redemption[_ASSET],
                                exchange=self.__EXCHANGE_NAME,
                                holder=self.account_holder,
                                transaction_type=Keyword.FEE.value,
                                spot_price=Keyword.UNKNOWN.value,
                                crypto_out_no_fee="0",
                                crypto_fee=str(total_interest_earned),
                                crypto_out_with_fee=str(total_interest_earned),
                                fiat_out_no_fee=None,
                                fiat_fee=None,
                                notes=(f"Penalty Fee for {redemption[_POSITION_ID]}"),
                            )
                        )

                    else:
                        raise RP2RuntimeError(
                            f"Internal Error: The redemption time ({self._rp2_timestamp_from_ms_epoch(redemption[_TIME])}) is not in the redemption window "
                            f"({self._rp2_timestamp_from_ms_epoch(str(earliest_redemption_timestamp))} + 2 days)."
                        )

                elif redemption[_ASSET] in current_subscriptions and redemption_amount in current_subscriptions[redemption[_ASSET]]:

                    self._logger.debug("Locked Savings positionId %s redeemed successfully.", redemption[_POSITION_ID])

                else:
                    raise RP2RuntimeError(f"Internal Error: Orphaned Redemption. Please open an issue at {self.ISSUES_URL}.")

            # if we returned the limit, we need to roll the window forward to the last time
            if len(locked_redemptions) < _INTEREST_SIZE_LIMIT:
                current_start = current_end + 1
                current_end = current_start + _THIRTY_DAYS_IN_MS
            else:
                current_start = now_time - 1  # int(locked_redemptions[0][_TIME]) + 1
                current_end = now_time  # current_start + _THIRTY_DAYS_IN_MS

        # Old system Flexible Savings

        # Reset window
        current_start = self._start_time_ms
        current_end = current_start + _THIRTY_DAYS_IN_MS

        # We will step backward in time from the switch over
        while current_start < earliest_record_epoch:

            self._logger.debug("Pulling flexible saving from older api system from %s to %s", current_start, current_end)

            flexible_saving = self._client.sapi_get_lending_union_interesthistory(
                params=({_START_TIME: current_start, _END_TIME: current_end, _LENDING_TYPE: _DAILY, _SIZE: _INTEREST_SIZE_LIMIT})
            )
            # [
            #     {
            #         "asset": "BUSD",
            #         "interest": "0.00006408",
            #         "lendingType": "DAILY",
            #         "productName": "BUSD",
            #         "time": 1577233578000
            #     },
            #     {
            #         "asset": "USDT",
            #         "interest": "0.00687654",
            #         "lendingType": "DAILY",
            #         "productName": "USDT",
            #         "time": 1577233562000
            #     }
            # ]
            processing_result_list = []
            for saving in flexible_saving:
                self._logger.debug("Flexible Saving: %s", json.dumps(saving))
                saving[_EN_INFO] = "Flexible Savings (OLD)"
                saving[_ID] = Keyword.UNKNOWN.value
                saving[_DIV_TIME] = saving[_TIME]
                saving[_AMOUNT] = saving[_INTEREST_FIELD]
                processing_result_list.append(self._process_gain(saving, Keyword.INTEREST))
                old_savings = True

            for processing_result in processing_result_list:
                if processing_result is None:
                    continue
                if processing_result.in_transactions:
                    in_transactions.extend(processing_result.in_transactions)

            # if we returned the limit, we need to roll the window forward to the last time
            if len(flexible_saving) < _INTEREST_SIZE_LIMIT:
                current_start = current_end + 1
                current_end = current_start + _THIRTY_DAYS_IN_MS
            else:
                current_start = int(flexible_saving[0][_TIME]) + 1
                current_end = current_start + _THIRTY_DAYS_IN_MS

            current_end = min(current_end, earliest_record_epoch)

        if old_savings:
            # Since we are making a guess at the cut off, there might be errors.
            self._logger.warning(
                "Pre-May 8th, 2021 savings detected. Please be aware that there may be duplicate or missing savings records around May 8th, 2021."
            )

        ### Mining Income

        # username is only required when pulling mining data
        for algo in self._get_algos():
            # Binance uses pages for mining payments
            current_page = 1
            while True:
                results = self._client.sapiGetMiningPaymentList(
                    params=({_ALGO: algo, _USERNAME: self.__username, _PAGE_INDEX: current_page, _PAGE_SIZE: _MINING_PAGE_LIMIT})
                )
                # {
                #   "code": 0,
                #   "msg": "",
                #   "data": {
                #     "accountProfits": [
                #       {
                #         "time": 1586188800000,            // Mining date
                #         "type": "31", // 0:Mining Wallet,5:Mining Address,7:Pool Savings,
                #           8:Transferred,31:Income Transfer ,32:Hashrate Resale-Mining Wallet 33:Hashrate Resale-Pool Savings
                #         "hashTransfer": null,            // Transferred Hashrate
                #         "transferAmount": null,          // Transferred Income
                #         "dayHashRate": 129129903378244,  // Daily Hashrate
                #         "profitAmount": 8.6083060304,   //Earnings Amount
                #         "coinName":"BTC",              // Coin Type
                #         "status": "2"    //Status：0:Unpaid， 1:Paying  2：Paid
                #       },
                #       {
                #         "time": 1607529600000,
                #         "coinName": "BTC",
                #         "type": "0", // String
                #         "dayHashRate": 9942053925926,
                #         "profitAmount": 0.85426469,
                #         "hashTransfer": 200000000000,
                #         "transferAmount": 0.02180958,
                #         "status": "2"
                #       },
                #       {
                #         "time": 1607443200000,
                #         "coinName": "BTC",
                #         "type": "31",
                #         "dayHashRate": 200000000000,
                #         "profitAmount": 0.02905916,
                #         "hashTransfer": null,
                #         "transferAmount": null,
                #         "status": "2"
                #       }
                #     ],
                #     "totalNum": 3,          // Total Rows
                #     "pageSize": 20          // Rows per page
                #   }
                # }

                if results[_DATA][_TOTAL_NUM] != "0":
                    profits: List[Dict[str, Union[int, str]]] = results[_DATA][_ACCOUNT_PROFITS]
                    processing_result_list = []
                    for result in profits:
                        self._logger.debug("Mining profit: %s", json.dumps(result))

                        # Currently the plugin only supports standard mining deposits
                        # Payment must also be made (status=2) in order to be counted
                        if result[_TYPE] == "0" and result[_STATUS] == "2":
                            processing_result_list.append(self._process_gain(result, Keyword.MINING))
                        else:
                            self._logger.error(
                                "WARNING: Unsupported Mining Transaction Type: %s.\nFull Details: %s\nPlease open an issue at %s.",
                                result[_TYPE],
                                json.dumps(result),
                                self.ISSUES_URL,
                            )

                    for processing_result in processing_result_list:
                        if processing_result is None:
                            continue
                        if processing_result.in_transactions:
                            in_transactions.extend(processing_result.in_transactions)

                    if len(profits) == _MINING_PAGE_LIMIT:
                        current_page += 1
                    else:
                        break
                else:
                    break

    def _process_implicit_api(  # pylint: disable=unused-argument
        self,
        in_transactions: List[InTransaction],
        out_transactions: List[OutTransaction],
        intra_transactions: List[IntraTransaction],
    ) -> None:

        # We need milliseconds for Binance
        now_time = int(datetime.now().timestamp()) * _MS_IN_SECOND

        processing_result_list: List[Optional[ProcessOperationResult]] = []

        # Crypto Bought with fiat. Technically this is a deposit of fiat that is used for a market order that fills immediately.
        # No limit on the date range
        # fiat payments takes the 'beginTime' param in contrast to other functions that take 'startTime'
        fiat_payments = self._client.sapiGetFiatPayments(params=({_TRANSACTION_TYPE: 0, _BEGIN_TIME: self._start_time_ms, _END_TIME: now_time}))
        # {
        #   "code": "000000",
        #   "message": "success",
        #   "data": [
        #   {
        #      "orderNo": "353fca443f06466db0c4dc89f94f027a",
        #      "sourceAmount": "20.0",  // Fiat trade amount
        #      "fiatCurrency": "EUR",   // Fiat token
        #      "obtainAmount": "4.462", // Crypto trade amount
        #      "cryptoCurrency": "LUNA",  // Crypto token
        #      "totalFee": "0.2",    // Trade fee
        #      "price": "4.437472",
        #      "status": "Failed",  // Processing, Completed, Failed, Refunded
        #      "createTime": 1624529919000,
        #      "updateTime": 1624529919000
        #   }
        #   ],
        #   "total": 1,
        #   "success": true
        # }
        if _DATA in fiat_payments:
            with ThreadPool(self._thread_count) as pool:
                processing_result_list = pool.map(self._process_fiat_payment, fiat_payments[_DATA])

            for processing_result in processing_result_list:
                if processing_result is None:
                    continue
                if processing_result.in_transactions:
                    in_transactions.extend(processing_result.in_transactions)
                if processing_result.out_transactions:
                    out_transactions.extend(processing_result.out_transactions)

        # Process actual fiat deposits (no limit on the date range)
        # Fiat deposits can also be pulled via CCXT fetch_deposits by cycling through legal_money
        # Using the underlying api endpoint is faster for Binance.
        # Note that this is the same endpoint as withdrawls, but with _TRANSACTION_TYPE set to 0 (for deposits)
        fiat_deposits = self._client.sapiGetFiatOrders(params=({_TRANSACTION_TYPE: 0, _START_TIME: self._start_time_ms, _END_TIME: now_time}))
        #    {
        #      "code": "000000",
        #      "message": "success",
        #      "data": [
        #        {
        #          "orderNo": "25ced37075c1470ba8939d0df2316e23",
        #          "fiatCurrency": "EUR",
        #          "indicatedAmount": "15.00",
        #          "amount": "15.00",
        #          "totalFee": "0.00",
        #          "method": "card",
        #          "status": "Failed",
        #          "createTime": 1627501026000,
        #          "updateTime": 1627501027000
        #        }
        #      ],
        #      "total": 1,
        #      "success": True
        #    }
        if _DATA in fiat_deposits:
            with ThreadPool(self._thread_count) as pool:
                processing_result_list = pool.map(self._process_fiat_deposit_order, fiat_deposits[_DATA])

            for processing_result in processing_result_list:
                if processing_result is None:
                    continue
                if processing_result.in_transactions:
                    in_transactions.extend(processing_result.in_transactions)

        # Process actual fiat withdrawls (no limit on the date range)
        # Fiat deposits can also be pulled via CCXT fetch_withdrawls by cycling through legal_money
        # Using the underlying api endpoint is faster for Binance.
        # Note that this is the same endpoint as deposits, but with _TRANSACTION_TYPE set to 1 (for withdrawls)
        fiat_withdrawals = self._client.sapiGetFiatOrders(params=({_TRANSACTION_TYPE: 1, _START_TIME: self._start_time_ms, _END_TIME: now_time}))
        #    {
        #      "code": "000000",
        #      "message": "success",
        #      "data": [
        #        {
        #          "orderNo": "25ced37075c1470ba8939d0df2316e23",
        #          "fiatCurrency": "EUR",
        #          "indicatedAmount": "15.00",
        #          "amount": "15.00",
        #          "totalFee": "0.00",
        #          "method": "card",
        #          "status": "Failed",
        #          "createTime": 1627501026000,
        #          "updateTime": 1627501027000
        #        }
        #      ],
        #      "total": 1,
        #      "success": True
        #    }
        if _DATA in fiat_withdrawals:
            with ThreadPool(self._thread_count) as pool:
                processing_result_list = pool.map(self._process_fiat_withdrawal_order, fiat_withdrawals[_DATA])

            for processing_result in processing_result_list:
                if processing_result is None:
                    continue
                if processing_result.out_transactions:
                    out_transactions.extend(processing_result.out_transactions)

        ### Dust Trades

        # We need milliseconds for Binance
        current_start = self._start_time_ms

        # We will pull in 30 day periods
        # If the user has more than 100 dust trades in a 30 day period this will break.
        # Maybe we can set a smaller window in the .ini file?
        current_end = current_start + _THIRTY_DAYS_IN_MS
        while current_start < now_time:
            dust_trades = self._client.fetch_my_dust_trades(params=({_START_TIME: current_start, _END_TIME: current_end}))
            # CCXT returns the same json as .fetch_trades()

            # Binance only returns 100 dust trades per call. If we hit the limit we will have to crawl
            # over each 'dribblet'. Each dribblet can have multiple assets converted into BNB at the same time.
            # If the user converts more than 100 assets at one time, we can not retrieve accurate records.
            if len(dust_trades) == _DUST_TRADE_RECORD_LIMIT:

                first_dribblet = [x for x in dust_trades if x[_DIV_TIME] == dust_trades[0][_DIV_TIME]]
                if len(first_dribblet) == _DUST_TRADE_RECORD_LIMIT:
                    raise RP2RuntimeError(
                        f"Internal error: too many assets dusted at the same time: " f"{self._rp2_timestamp_from_ms_epoch(str(dust_trades[0][_DIV_TIME]))}"
                    )

                with ThreadPool(self._thread_count) as pool:
                    processing_result_list = pool.map(self._process_dust_trade, first_dribblet)

                for processing_result in processing_result_list:
                    if processing_result is None:
                        continue
                    if processing_result.in_transactions:
                        in_transactions.extend(processing_result.in_transactions)
                    if processing_result.out_transactions:
                        out_transactions.extend(processing_result.out_transactions)

                # Shift the call window forward past this dribblet
                current_start = first_dribblet[len(first_dribblet) - 1][_DIV_TIME] + 1
                current_end = current_start + _THIRTY_DAYS_IN_MS
                break

            with ThreadPool(self._thread_count) as pool:
                processing_result_list = pool.map(self._process_dust_trade, dust_trades)

            for processing_result in processing_result_list:
                if processing_result is None:
                    continue
                if processing_result.in_transactions:
                    in_transactions.extend(processing_result.in_transactions)
                if processing_result.out_transactions:
                    out_transactions.extend(processing_result.out_transactions)

            current_start = current_end + 1
            current_end = current_start + _THIRTY_DAYS_IN_MS

    def _process_dividend(self, dividend: Any, notes: Optional[str] = None) -> ProcessOperationResult:
        self._logger.debug("Dividend: %s", json.dumps(dividend))
        if dividend[_EN_INFO] in _STAKING_LIST or re.search("[dD]istribution", dividend[_EN_INFO]) or re.search("staking", dividend[_EN_INFO]):
            return self._process_gain(dividend, Keyword.STAKING, notes)
        if dividend[_EN_INFO] in _INTEREST_LIST:
            return self._process_gain(dividend, Keyword.INTEREST, notes)
        if dividend[_EN_INFO] in _AIRDROP_LIST or re.search("[aA]irdrop", dividend[_EN_INFO]):
            return self._process_gain(dividend, Keyword.AIRDROP, notes)
        self._logger.error("WARNING: Unrecognized Dividend: %s. Please open an issue at %s", dividend[_EN_INFO], self.ISSUES_URL)
        return ProcessOperationResult(in_transactions=[], out_transactions=[], intra_transactions=[])

    def _process_dust_trade(self, dust: Any, notes: Optional[str] = None) -> ProcessOperationResult:
        self._logger.debug("Dust: %s", json.dumps(dust))
        # dust trades have a null id, and if multiple assets are dusted at the same time, all are assigned same ID
        dust_trade: Trade = self._to_trade(dust[_SYMBOL], str(dust[_AMOUNT]), str(dust[_COST]))
        dust[_ID] = f"{dust[_ORDER]}{dust_trade.base_asset}"
        return self._process_buy_and_sell(dust, notes)

    def _process_gain(self, transaction: Any, transaction_type: Keyword, notes: Optional[str] = None) -> ProcessOperationResult:
        self._logger.debug("Gain: %s", json.dumps(transaction))
        in_transaction_list: List[InTransaction] = []

        if transaction_type == Keyword.MINING:
            amount: RP2Decimal = RP2Decimal(str(transaction[_PROFIT_AMOUNT]))
            notes = f"{notes + '; ' if notes else ''}'Mining profit'"
            in_transaction_list.append(
                InTransaction(
                    plugin=self.__PLUGIN_NAME,
                    unique_id=Keyword.UNKNOWN.value,
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_TIME]),
                    asset=transaction[_COIN_NAME],
                    exchange=self.__EXCHANGE_NAME,
                    holder=self.account_holder,
                    transaction_type=transaction_type.value,
                    spot_price=Keyword.UNKNOWN.value,
                    crypto_in=str(amount),
                    crypto_fee=None,
                    fiat_in_no_fee=None,
                    fiat_in_with_fee=None,
                    fiat_fee=None,
                    notes=notes,
                )
            )
        elif RP2Decimal(transaction[_AMOUNT]) != ZERO:  # Sometimes Binance reports interest payments with zero amounts
            amount = RP2Decimal(transaction[_AMOUNT])
            notes = f"{notes + '; ' if notes else ''}{transaction[_EN_INFO]}"

            in_transaction_list.append(
                InTransaction(
                    plugin=self.__PLUGIN_NAME,
                    unique_id=str(transaction[_ID]),  # Binance sometimes has two ids for one tranid
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_DIV_TIME]),
                    asset=transaction[_ASSET],
                    exchange=self.__EXCHANGE_NAME,
                    holder=self.account_holder,
                    transaction_type=transaction_type.value,
                    spot_price=Keyword.UNKNOWN.value,
                    crypto_in=str(amount),
                    crypto_fee=None,
                    fiat_in_no_fee=None,
                    fiat_in_with_fee=None,
                    fiat_fee=None,
                    notes=notes,
                )
            )

        return ProcessOperationResult(in_transactions=in_transaction_list, out_transactions=[], intra_transactions=[])

    def _process_fiat_deposit_order(self, transaction: Any, notes: Optional[str] = None) -> ProcessOperationResult:
        return self._process_fiat_order(transaction, Keyword.BUY, notes)

    def _process_fiat_withdrawal_order(self, transaction: Any, notes: Optional[str] = None) -> ProcessOperationResult:
        return self._process_fiat_order(transaction, Keyword.SELL, notes)

    def _process_fiat_order(self, transaction: Any, transaction_type: Keyword, notes: Optional[str] = None) -> ProcessOperationResult:
        self._logger.debug("Fiat Order (%s): %s", transaction_type.value, json.dumps(transaction))
        in_transaction_list: List[InTransaction] = []
        out_transaction_list: List[OutTransaction] = []
        if transaction[_STATUS] == "Completed":
            amount: RP2Decimal = RP2Decimal(transaction[_INDICATED_AMOUNT])
            fee: RP2Decimal = RP2Decimal(transaction[_TOTAL_FEE])
            notes = f"{notes + '; ' if notes else ''}Fiat {transaction_type.value} of {transaction[_FIAT_CURRENCY]}"
            if transaction_type == Keyword.BUY:
                in_transaction_list.append(
                    InTransaction(
                        plugin=self.__PLUGIN_NAME,
                        unique_id=transaction[_ORDER_NO],
                        raw_data=json.dumps(transaction),
                        timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_CREATE_TIME]),
                        asset=transaction[_FIAT_CURRENCY],
                        exchange=self.__EXCHANGE_NAME,
                        holder=self.account_holder,
                        transaction_type=transaction_type.value,
                        spot_price="1",
                        crypto_in=str(amount),
                        crypto_fee=str(fee),
                        fiat_in_no_fee=None,
                        fiat_in_with_fee=None,
                        fiat_fee=None,
                        fiat_ticker=transaction[_FIAT_CURRENCY],
                        notes=notes,
                    )
                )
            elif transaction_type == Keyword.SELL:
                out_transaction_list.append(
                    OutTransaction(
                        plugin=self.__PLUGIN_NAME,
                        unique_id=transaction[_ORDER_NO],
                        raw_data=json.dumps(transaction),
                        timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_CREATE_TIME]),
                        asset=transaction[_FIAT_CURRENCY],
                        exchange=self.__EXCHANGE_NAME,
                        holder=self.account_holder,
                        transaction_type=Keyword.SELL.value,
                        spot_price="1",
                        crypto_out_no_fee=str(amount),
                        crypto_fee=str(fee),
                        fiat_out_no_fee=None,
                        fiat_fee=None,
                        fiat_ticker=transaction[_FIAT_CURRENCY],
                        notes=notes,
                    )
                )
        return ProcessOperationResult(in_transactions=in_transaction_list, out_transactions=out_transaction_list, intra_transactions=[])

    def _process_fiat_payment(self, transaction: Any, notes: Optional[str] = None) -> ProcessOperationResult:
        self._logger.debug("Fiat Payment: %s", json.dumps(transaction))
        in_transaction_list: List[InTransaction] = []
        out_transaction_list: List[OutTransaction] = []

        if transaction[_STATUS] == "Completed":
            if self.is_native_fiat(transaction[_FIAT_CURRENCY]):

                # For double entry accounting purposes we must create a fiat InTransaction
                in_transaction_list.append(
                    InTransaction(
                        plugin=self.__PLUGIN_NAME,
                        unique_id=transaction[_ORDER_NO],
                        raw_data=json.dumps(transaction),
                        timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_CREATE_TIME]),
                        asset=transaction[_CRYPTOCURRENCY],
                        exchange=self.__EXCHANGE_NAME,
                        holder=self.account_holder,
                        transaction_type=Keyword.BUY.value,
                        spot_price=str(RP2Decimal(transaction[_PRICE])),
                        crypto_in=transaction[_OBTAIN_AMOUNT],
                        crypto_fee=None,
                        fiat_in_no_fee=str(RP2Decimal(transaction[_SOURCE_AMOUNT]) - RP2Decimal(transaction[_TOTAL_FEE])),
                        fiat_in_with_fee=str(transaction[_SOURCE_AMOUNT]),
                        fiat_fee=str(RP2Decimal(transaction[_TOTAL_FEE])),
                        fiat_ticker=transaction[_FIAT_CURRENCY],
                        notes=(f"{notes + '; ' if notes else ''}Buy transaction for native fiat payment orderNo - {transaction[_ORDER_NO]}"),
                    )
                )

                # This is an OutTransaction for a buy or conversion based on what the native fiat is.
                out_transaction_list.append(
                    OutTransaction(
                        plugin=self.__PLUGIN_NAME,
                        unique_id=transaction[_ORDER_NO],
                        raw_data=json.dumps(transaction),
                        timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_CREATE_TIME]),
                        asset=transaction[_FIAT_CURRENCY],
                        exchange=self.__EXCHANGE_NAME,
                        holder=self.account_holder,
                        transaction_type=Keyword.SELL.value,
                        spot_price=str(RP2Decimal("1")),
                        crypto_out_no_fee=str(RP2Decimal(transaction[_SOURCE_AMOUNT]) - RP2Decimal(transaction[_TOTAL_FEE])),
                        crypto_fee=str(RP2Decimal(transaction[_TOTAL_FEE])),
                        crypto_out_with_fee=str(RP2Decimal(transaction[_SOURCE_AMOUNT])),
                        fiat_out_no_fee=str(RP2Decimal(transaction[_SOURCE_AMOUNT]) - RP2Decimal(transaction[_TOTAL_FEE])),
                        fiat_fee=None,
                        fiat_ticker=transaction[_FIAT_CURRENCY],
                        notes=(f"{notes + '; ' if notes else ''}Sell transaction conversion from native fiat orderNo - " f"{transaction[_ORDER_NO]}"),
                    )
                )
            else:
                in_transaction_list.append(
                    InTransaction(
                        plugin=self.__PLUGIN_NAME,
                        unique_id=transaction[_ORDER_NO],
                        raw_data=json.dumps(transaction),
                        timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_CREATE_TIME]),
                        asset=transaction[_CRYPTOCURRENCY],
                        exchange=self.__EXCHANGE_NAME,
                        holder=self.account_holder,
                        transaction_type=Keyword.BUY.value,
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_in=transaction[_OBTAIN_AMOUNT],
                        crypto_fee=None,
                        fiat_in_no_fee=None,
                        fiat_in_with_fee=None,
                        fiat_fee=None,
                        notes=(f"{notes + '; ' if notes else ''}Buy transaction conversion from non-native_fiat orderNo - {transaction[_ORDER_NO]}"),
                    )
                )

                out_transaction_list.append(
                    OutTransaction(
                        plugin=self.__PLUGIN_NAME,
                        unique_id=transaction[_ORDER_NO],
                        raw_data=json.dumps(transaction),
                        timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_CREATE_TIME]),
                        asset=transaction[_FIAT_CURRENCY],
                        exchange=self.__EXCHANGE_NAME,
                        holder=self.account_holder,
                        transaction_type=Keyword.SELL.value,
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_out_no_fee=str(RP2Decimal(transaction[_SOURCE_AMOUNT]) - RP2Decimal(transaction[_TOTAL_FEE])),
                        crypto_fee=str(RP2Decimal(transaction[_TOTAL_FEE])),
                        crypto_out_with_fee=str(RP2Decimal(transaction[_SOURCE_AMOUNT])),
                        fiat_out_no_fee=None,
                        fiat_fee=None,
                        notes=(f"{notes + '; ' if notes else ''}Sell transaction conversion from non-native_fiat orderNo - " f"{transaction[_ORDER_NO]}"),
                    )
                )

            # An InTransaction is needed for the fiat in order for the accounting to zero out
            in_transaction_list.append(
                InTransaction(
                    plugin=self.__PLUGIN_NAME,
                    unique_id=f"{transaction[_ORDER_NO]}/fiat_buy",
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2_timestamp_from_ms_epoch(transaction[_CREATE_TIME]),
                    asset=transaction[_FIAT_CURRENCY],
                    exchange=self.__EXCHANGE_NAME,
                    holder=self.account_holder,
                    transaction_type=Keyword.BUY.value,
                    spot_price=str(RP2Decimal("1")),
                    crypto_in=str(RP2Decimal(transaction[_SOURCE_AMOUNT])),
                    crypto_fee=None,
                    fiat_in_no_fee=str(RP2Decimal(transaction[_SOURCE_AMOUNT])),
                    fiat_in_with_fee=str(RP2Decimal(transaction[_SOURCE_AMOUNT])),
                    fiat_fee=None,
                    fiat_ticker=transaction[_FIAT_CURRENCY],
                    notes=(f"{notes + '; ' if notes else ''}Fiat deposit for orderNo - {transaction[_ORDER_NO]}"),
                )
            )

        return ProcessOperationResult(in_transactions=in_transaction_list, out_transactions=out_transaction_list, intra_transactions=[])
