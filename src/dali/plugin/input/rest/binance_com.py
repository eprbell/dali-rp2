# Copyright 2022 macanudo527
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

# Binance.com REST plugin links:
# REST API: https://binance-docs.github.io/apidocs/
# Authentication: https://binance-docs.github.io/apidocs/spot/en/#introduction
# Endpoint: https://api.binance.com

import hashlib
import hmac
import json
import logging
import time
from multiprocessing.pool import ThreadPool
from typing import Any, cast, Dict, List, NamedTuple, Optional
from dateutil.relativedelta import relativedelta

from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.dali_configuration import Keyword, is_fiat
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

import ccxt

# Native format keywords
_CREATETIME: str = "createTime"
_CRYPTOCURRENCY: str = "cryptoCurrency"
_DATA: str = "data"
_FIATCURRENCY: str = "fiatCurrency"
_INDICATEDAMOUNT: str = "indicatedAmount"
_ORDERNO: str = "orderNo"
_SOURCEAMOUNT: str = "sourceAmount"
_TOTALFEE: str = "totalFee"
_UPDATETIME: str = "updateTime"

class _ProcessAccountResult(NamedTuple):
	in_transactions: List[InTransaction]
	out_transactions: List[OutTransaction]
	intra_transactions: List[IntraTransaction]

# class _BinanceAuth(AuthBase): - possibly not needed

class InputPlugin(AbstractInputPlugin):

	__DEFAULT_THREAD_COUNT: int = 3 # Maybe unnecessary
	__MAX_THREAD_COUNT: int = 4

	__BINANCE_COM: str = "Binance.com"

	def __init__(
		self,
		account_holder: str,
		api_key: str,
		api_secret: str,
		thread_count: Optional[int] = None,
	) -> None:

		super().__init__(account_holder)
		self.__logger: logging.Logger = create_logger(f"{self.__BINANCE_COM}/{self.account_holder}")
		self.__cache_key: str = f"{self.__BINANCE_COM.lower()}-{account_holder}"
		self.__thread_count = thread_count if thread_count else self.__DEFAULT_THREAD_COUNT
		if self.__thread_count > self.__MAX_THREAD_COUNT:
			raise Exception(f"Thread count is {self.__thread_count}: it exceeds the maximum value of {self.__MAX_THREAD_COUNT}")
		self.client = ccxt.binance({
			'apiKey': api_key,
			'secret': api_secret,
			})
		
		# We have to know what markets are on Binance so that we can pull orders using the market
		self.markets: set = set()
		ccxtMarkets = self.client.fetch_markets()
		for market in ccxtMarkets:
			self.markets.add(market['id'])

		# We will have a default start time of July 13th, 2017 since Binance Exchange officially launched on July 14th Beijing Time.
		self.startTime = datetime.datetime(2017,7,13,0,0,0,0)

	def cache_key(self) -> Optional[str]:
		return self.__cache_key

	def load(self) -> List[AbstractTransaction]:
		result: List[AbstractTransaction] = []
		in_transactions: List[AbstractTransaction] = []

		self._process_deposits(in_transactions)



		return result

	def _process_deposits(self, in_transactions) -> None:
		
		# We need milliseconds for Binance
		currentStart = int(self.startTime.timestamp()) * 1000
		nowTime = int(datetime.datetime.now().timestamp()) * 1000

		# Crypto Deposits can only be pulled in 90 day windows
		currentEnd = currentStart + 7776000000 
		crypto_deposits = []

		# Crypto Bought with fiat. Technically this is a deposit of fiat that is used for a market order that fills immediately.
		# No limit on the date range
		# fiat payments takes the 'beginTime' param in contrast to other functions that take 'startTime'
		fiat_payments = self.client.sapiGetFiatPayments(params=({'transactionType':0, 
			'beginTime':(int(startTime.timestamp()) * 1000), 
			'endTime':nowTime}))
		# {
		#    "code": "000000",
		#    "message": "success",
		#    "data": [
		#    {
		#       "orderNo": "353fca443f06466db0c4dc89f94f027a",
		#       "sourceAmount": "20.0",  // Fiat trade amount
		#       "fiatCurrency": "EUR",   // Fiat token
		#       "obtainAmount": "4.462", // Crypto trade amount
		#       "cryptoCurrency": "LUNA",  // Crypto token
		#       "totalFee": "0.2",     // Trade fee
		#       "price": "4.437472", 
		#       "status": "Failed",  // Processing, Completed, Failed, Refunded
		#       "createTime": 1624529919000,
		#       "updateTime": 1624529919000  
		#    }
		#    ],
		#    "total": 1,
		#    "success": true
		# }
		for payment in fiat_payments:
			self._process_fiat_payments(payment, in_transactions)

		
		# Process crypto deposits (limited to 90 day windows)
		while currentStart < nowTime:
			# The CCXT function only retrieves fiat deposits if you provide a valid 'legalMoney' code as variable.
			crypto_deposits = self.client.fetch_deposits(params=({'startTime':currentStart, 'endTime':currentEnd}))
            #     [
            #       {
            #         "amount": "0.01844487",
            #         "coin": "BCH",
            #         "network": "BCH",
            #         "status": 1,
            #         "address": "1NYxAJhW2281HK1KtJeaENBqHeygA88FzR",
            #         "addressTag": "",
            #         "txId": "bafc5902504d6504a00b7d0306a41154cbf1d1b767ab70f3bc226327362588af",
            #         "insertTime": 1610784980000,
            #         "transferType": 0,
            #         "confirmTimes": "2/2"
            #       },
            #       {
            #         "amount": "4500",
            #         "coin": "USDT",
            #         "network": "BSC",
            #         "status": 1,
            #         "address": "0xc9c923c87347ca0f3451d6d308ce84f691b9f501",
            #         "addressTag": "",
            #         "txId": "Internal transfer 51376627901",
            #         "insertTime": 1618394381000,
            #         "transferType": 1,
            #         "confirmTimes": "1/15"
            #     }
            #   ]
            for deposit in crypto_deposits:			
				self._process_crypto_deposit(deposit, in_transactions)
			currentStart += 7776000000
			currentEnd += 7776000000

		# Process actual fiat deposits (no limit on the date range)
		fiat_deposits = self.client.sapiGetFiatOrders(params=({'startTime':currentStart, 'endTime':nowTime}))
        #     {
        #       "code": "000000",
        #       "message": "success",
        #       "data": [
        #         {
        #           "orderNo": "25ced37075c1470ba8939d0df2316e23",
        #           "fiatCurrency": "EUR",
        #           "indicatedAmount": "15.00",
        #           "amount": "15.00",
        #           "totalFee": "0.00",
        #           "method": "card",
        #           "status": "Failed",
        #           "createTime": 1627501026000,
        #           "updateTime": 1627501027000
        #         }
        #       ],
        #       "total": 1,
        #       "success": True
        #     }	
        for deposit in fiat_deposits:
        	self._process_fiat_deposit(deposit, in_transactions)


    def _process_fiat_deposit(
    	self, transaction: Any, in_transaction_list: List[InTransaction], notes: Optional[str] = None
    ) -> None:
    	transaction_data = transaction[_DATA]

    	# Is this a fiat payment?
    	if [_CRYPTOCURRENCY] in transaction_data:
    		amount: RP2Decimal = RP2Decimal(transaction_data[_SOURCEAMOUNT])
    		fee = "0"	
    	else:
    		amount: RP2Decimal = RP2Decimal(transaction_data[_INDICATEDAMOUNT])
    		fee: RP2Decimal = RP2Decimal(transaction_data[_TOTALFEE])
    	notes = f"{notes + '; ' if notes else ''}{"Fiat Deposit of "}; {transaction_data[_FIATCURRENCY]}"
    	in_transaction_list.append(
    		InTransaction(
    			plugin=self.__BINANCE_COM,
    			unique_id=transaction_data[_ORDERNO],
    			raw_data=json.dumps(transaction),
    			timestamp=transaction[_CREATETIME], # Currently in epoch timestamp, PR#37 might allow this
    			asset=transaction[_FIATCURRENCY],
    			exchange=self.__BINANCE_COM,
    			holder=self.account_holder,
    			transaction_type=Keyword.BUY.value,
    			spot_price="1",
    			crypto_in=str(amount),
    			crypto_fee=str(fee),
    			fiat_in_no_fee=None,
    			fiat_in_with_fee=None,
    			fiat_fee=None,
    			notes=notes,
    		)
    	)




		






