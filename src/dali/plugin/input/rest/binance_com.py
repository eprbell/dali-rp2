# Copyright 2022 macanudo527
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	 http://www.apache.org/licenses/LICENSE-2.0
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
import datetime
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
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager

# Native format keywords
_AMOUNT: str = "amount"
_BEGINTIME: str = "beginTime"
_BUY: str = "buy" # CCXT
_COIN: str = "coin"
_COST: str = "cost" # CCXT only variable
_CREATETIME: str = "createTime"
_CRYPTOCURRENCY: str = "cryptoCurrency"
_CURRENCY: str = "currency" # CCXT only variable
_DATA: str = "data"
_DATETIME: str = "datetime" # CCXT only variable
_ENDTIME: str = "endTime"
_FEE: str = "fee"
_FIATCURRENCY: str = "fiatCurrency"
_ID: str = "id" # CCXT
_INDICATEDAMOUNT: str = "indicatedAmount"
_INFO: str = "info"
_INSERTTIME: str = "insertTime"
_ORDERNO: str = "orderNo"
_SIDE: str = "side" # CCXT
_STARTTIME: str = "startTime"
_STATUS: str = "status"
_SOURCEAMOUNT: str = "sourceAmount"
_SYMBOL: str = "symbol"
_TRANSACTIONTYPE: str = "transactionType"
_TOTALFEE: str = "totalFee"
_TXID: str = "txid" # CCXT doesn't capitalize I
_UPDATETIME: str = "updateTime"

class _ProcessAccountResult(NamedTuple):
	in_transactions: List[InTransaction]
	out_transactions: List[OutTransaction]
	intra_transactions: List[IntraTransaction]

class _Trade(NamedTuple):
	base_asset: str
	quote_asset: str
	base_info: str
	quote_info: str

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
		self.startTimeMS = int(self.startTime.timestamp()) * 1000

	@staticmethod
	def _rp2timestamp_from_ms_epoch(epoch_timestamp: str) -> str:
		return str(datetime.datetime.fromtimestamp((int(epoch_timestamp) / 1000), datetime.timezone.utc))

	@staticmethod
	def _string_to_marketPair(marketPair: str, base_amount: str, quote_amount: str) -> Optional[_Market]:
		assets = marketPair.split("/")
		return _Market(
			base_asset=assets[0],
			quote_asset=assets[1],
			base_info=f"{base_amount} {assets[0]}",
			quote_info=f"{quote_amount} {assets[1]}",
		)		

	def cache_key(self) -> Optional[str]:
		return self.__cache_key

	def load(self) -> List[AbstractTransaction]:
		result: List[AbstractTransaction] = []
		in_transactions: List[AbstractTransaction] = []
		intra_transactions: List[AbstractTransaction] = []

		self._process_deposits(in_transactions, intra_transactions)
		self._process_trades(in_transactions)


		result.extend(in_transactions)
		result.extend(intra_transactions)

		return result

	def _process_deposits(
		self, in_transactions: List[InTransaction], intra_transactions: List[IntraTransaction],
	) -> None:
		
		# We need milliseconds for Binance
		currentStart = self.startTimeMS
		nowTime = int(datetime.datetime.now().timestamp()) * 1000

		# Crypto Deposits can only be pulled in 90 day windows
		currentEnd = currentStart + 7776000000 
		crypto_deposits = []

		# Crypto Bought with fiat. Technically this is a deposit of fiat that is used for a market order that fills immediately.
		# No limit on the date range
		# fiat payments takes the 'beginTime' param in contrast to other functions that take 'startTime'
		fiat_payments = self.client.sapiGetFiatPayments(params=({_TRANSACTIONTYPE:0, 
			_BEGINTIME :self.startTimeMS, 
			_ENDTIME :nowTime}))
		# {
		#	"code": "000000",
		#	"message": "success",
		#	"data": [
		#	{
		#	   "orderNo": "353fca443f06466db0c4dc89f94f027a",
		#	   "sourceAmount": "20.0",  // Fiat trade amount
		#	   "fiatCurrency": "EUR",   // Fiat token
		#	   "obtainAmount": "4.462", // Crypto trade amount
		#	   "cryptoCurrency": "LUNA",  // Crypto token
		#	   "totalFee": "0.2",	 // Trade fee
		#	   "price": "4.437472", 
		#	   "status": "Failed",  // Processing, Completed, Failed, Refunded
		#	   "createTime": 1624529919000,
		#	   "updateTime": 1624529919000  
		#	}
		#	],
		#	"total": 1,
		#	"success": true
		# }
		for payment in fiat_payments[_DATA]:
			if payment[_STATUS] == "Completed":
				self._process_fiat_payment(payment, in_transactions)

		
		# Process crypto deposits (limited to 90 day windows)
		while currentStart < nowTime:
			# The CCXT function only retrieves fiat deposits if you provide a valid 'legalMoney' code as variable.
			crypto_deposits = self.client.fetch_deposits(params=({_STARTTIME:currentStart, _ENDTIME:currentEnd}))
			
			# CCXT returns a standardized response from fetch_deposits. 'info' is the exchange-specific information
			# in this case from Binance.com 

			# {
			# 	'info': {
			# 		'amount': '0.00999800', 
			# 		'coin': 'PAXG', 
			# 		'network': 'ETH', 
			# 		'status': '1', 
			# 		'address': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c', 
			# 		'addressTag': '', 
			# 		'txId': '0xaad4654a3234aa6118af9b4b335f5ae81c360b2394721c019b5d1e75328b09f3', 
			# 		'insertTime': '1599621997000', 
			# 		'transferType': '0', 
			# 		'confirmTimes': '12/12', 
			# 		'unlockConfirm': '12/12', 
			# 		'walletType': '0'
			# 	},
			# 	'id': None, 
			# 	'txid': '0xaad4654a3234aa6118af9b4b335f5ae81c360b2394721c019b5d1e75328b09f3', 
			# 	'timestamp': 1599621997000, 
			# 	'datetime': '2020-02-11T04:21:19.000Z', 
			# 	'network': 'ETH', 
			# 	'address': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c', 
			# 	'addressTo': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c', 
			# 	'addressFrom': None, 
			# 	'tag': None, 
			# 	'tagTo': None, 
			# 	'tagFrom': None, 
			# 	'type': 'deposit', 
			# 	'amount': 0.00999800, 
			# 	'currency': 'PAXG', 
			# 	'status': 'ok', 
			# 	'updated': None, 
			# 	'internal': False, 
			# 	'fee': None
			# }

			for deposit in crypto_deposits:
				self._process_transfer(deposit, intra_transactions)
			currentStart += 7776000000
			currentEnd += 7776000000

		# Process actual fiat deposits (no limit on the date range)
		fiat_deposits = self.client.sapiGetFiatOrders(params=({_TRANSACTIONTYPE:0,
			_STARTTIME:self.startTimeMS, _ENDTIME:nowTime}))
		#	 {
		#	   "code": "000000",
		#	   "message": "success",
		#	   "data": [
		#		 {
		#		   "orderNo": "25ced37075c1470ba8939d0df2316e23",
		#		   "fiatCurrency": "EUR",
		#		   "indicatedAmount": "15.00",
		#		   "amount": "15.00",
		#		   "totalFee": "0.00",
		#		   "method": "card",
		#		   "status": "Failed",
		#		   "createTime": 1627501026000,
		#		   "updateTime": 1627501027000
		#		 }
		#	   ],
		#	   "total": 1,
		#	   "success": True
		#	 }	
		for deposit in fiat_deposits[_DATA]:
			if deposit[_STATUS] == "Completed":
				self._process_deposit(deposit, in_transactions)

	def _process_trades(
		self, in_transactions: List[InTransaction], out_transactions: List[OutTransaction]
	) -> None:
	
		# Binance requires a symbol/market 
		# max limit is 1000
		for market in self.markets:
			while returned_records < 1000:
				marketTrades = self.client.fetch_my_trades(symbol=market, since=self.startTimeMS,
					limit=1000)
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
				# * The ``cost`` of the trade means ``amount * price``. It is the total *quote* volume of the trade (whereas ``amount`` is the *base* volume). The cost field itself is there mostly for convenience and can be deduced from other fields.
				# * The ``cost`` of the trade is a *"gross"* value. That is the value pre-fee, and the fee has to be applied afterwards.
				for trade in marketTrades:
					print(trade)
					self._process_sell(trade, out_transactions)
					self._process_buy(trade, in_transactions, out_transactions)

	def _process_buy(
		self, transaction: Any, in_transaction_list: List[InTransaction], 
		out_transaction_list: List[OutTransaction], notes: Optional[str] = None
	) -> None:
		# if fiatCurrency then fiat payment

		trade = self._to_trade(transaction[_SYMBOL], str(transaction[_AMOUNT]), str(transaction[_COST]))

		if transaction[_SIDE] == [_BUY]:
			out_asset = trade.quote_asset
			in_asset = trade.base_asset
			crypto_in: RP2Decimal = RP2Decimal(str(transaction[_COST]))
			conversion_info = f"{trade.quote_info} -> {trade.base_info}"
		elif transaction[_SIDE] == [_SELL]:
			out_asset = trade.base_asset
			in_asset = trade.quote_asset
			crypto_in: RP2Decimal = RP2Decimal(str(transaction[_AMOUNT]))
			conversion_info = f"{trade.base_info} -> {trade.quote_info}"
		else:
			raise Exception(f"Internal error: unrecognized transaction side: " {transaction[_SIDE]})
		
		if transaction[_FEE][_CURRENCY] == in_asset:
			crypto_fee: RP2Decimal = RP2Decimal(str(transaction[_FEE][_COST]))
		else:
			crypto_fee = "0"

			# Users can use BNB to pay fees on Binance
			if transaction[_FEE][_CURRENCY] != out_asset:
				out_transaction_list.append(
					OutTransaction(
						plugin=self.__BINANCE_COM,
						unique_id=transaction[_ID],
						raw_data=json.dumps(transaction),
						timestamp=transaction[_DATETIME],
						asset=transaction[_FEE][_CURRENCY],
						exchange=self.__BINANCE_COM,
						holder=self.account_holder,
						transaction_type=Keyword.FEE.value,
						spot_price=Keyword.UNKNOWN.value,
						crypto_out_no_fee=str(transaction[_FEE][_COST]),
						crypto_fee="0",
						fiat_out_no_fee=Keyword.UNKNOWN.value,
						fiat_fee=Keyword.UNKNOWN.value,
						notes=(
							f"{notes + '; ' if notes else ''} Fee for conversion from "
							f"{conversion_info}"
						)
					)
				)


		in_transaction_list.append(
			InTransaction(
				plugin=self.__BINANCE_COM,
				unique_id=transaction[_ID],
				raw_data=json.dumps(transaction),
				timestamp=transaction[_DATETIME],
				asset=in_asset,
				exchange=self.__BINANCE_COM,
				holder=self.account_holder,
				transaction_type=Keyword.SELL.value,
				spot_price=Keyword.UNKNOWN.value,
				crypto_in=crypto_in,
				crypto_fee=crypto_fee,
				fiat_in_no_fee=Keyword.UNKNOWN.value,
				fiat_in_with_fee=Keyword.UNKNOWN.value,
				fiat_fee=Keyword.UNKNOWN.value,
				notes=(
					f"{notes + '; ' if notes else ''} Buy side of conversion from "
					f"{conversion_info}"
					f"({out_asset} out-transaction unique id: {transaction[_ID]}"
				),
			)
		)



	def _process_deposit(
		self, transaction: Any, in_transaction_list: List[InTransaction], notes: Optional[str] = None
	) -> None:
		self.__logger.debug("Deposit: %s", transaction)

		# Is this a fiat payment?
		if transaction[_CRYPTOCURRENCY] is not None:
			amount: RP2Decimal = RP2Decimal(transaction[_SOURCEAMOUNT])
			fee = "0"	
		else:
			amount: RP2Decimal = RP2Decimal(transaction[_INDICATEDAMOUNT])
			fee: RP2Decimal = RP2Decimal(transaction[_TOTALFEE])
		notes = f"{notes + '; ' if notes else ''}{'Fiat Deposit of '}; {transaction[_FIATCURRENCY]}"
		in_transaction_list.append(
			InTransaction(
				plugin=self.__BINANCE_COM,
				unique_id=transaction[_ORDERNO],
				raw_data=json.dumps(transaction),
				timestamp=self._rp2timestamp_from_ms_epoch(transaction[_CREATETIME]),
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

	def _process_sell(
		self, transaction: Any, out_transaction_list: List[OutTransaction], notes: Optional[str] = None
	) -> None:
		self.__logger.debug("Sell Transaction: %s", transaction)
		trade = self._to_trade(transaction[_SYMBOL], str(transaction[_AMOUNT]), str(transaction[_COST]))

		# For some reason CCXT outputs amounts in float
		if transaction[_SIDE] == [_BUY]:
			out_asset = trade.quote_asset
			in_asset = trade.base_asset
			crypto_out_no_fee: RP2Decimal = RP2Decimal(str(transaction[_COST]))
			conversion_info = f"{trade.quote_info} -> {trade.base_info}"
		elif transaction[_SIDE] == [_SELL]:
			out_asset = trade.base_asset
			in_asset = trade.quote_asset
			crypto_out_no_fee: RP2Decimal = RP2Decimal(str(transaction[_AMOUNT]))
			conversion_info = f"{trade.base_info} -> {trade.quote_info}"
		else:
			raise Exception(f"Internal error: unrecognized transaction side: " {transaction[_SIDE]})

		if transaction[_FEE][_CURRENCY] == out_asset:
			crypto_fee: RP2Decimal = RP2Decimal(str(transaction[_FEE][_COST]))
		else:
			crypto_fee: RP2Decimal = RP2Decimal("0")
		crypto_out_with_fee: RP2Decimal = crypto_out_no_fee + crypto_fee

		# Binance does not report the value of transaction in fiat
		out_transaction_list.append(
			OutTransaction(
				plugin=self.__BINANCE_COM,
				unique_id=transaction[_ID],
				raw_data=json.dumps(transaction),
				timestamp=transaction[_DATETIME],
				asset=out_asset,
				exchange=self.__BINANCE_COM,
				holder=self.account_holder,
				transaction_type=Keyword.SELL.value,
				spot_price=Keyword.UNKNOWN.value,
				crypto_out_no_fee=crypto_out_no_fee,
				crypto_fee=crypto_fee,
				crypto_out_with_fee=crypto_out_with_fee,
				fiat_out_no_fee=Keyword.UNKNOWN.value,
				fiat_fee=Keyword.UNKNOWN.value,
				notes=(
					f"{notes + '; ' if notes else ''} Sell side of conversion from "
					f"{conversion_info}"
					f"({in_asset} in-transaction unique id: {transaction[_ID]}"
				),

			)
		)

	def _process_transfer(
		self, transaction: Any, intra_transaction_list: List[IntraTransaction]
	) -> None:
		print(transaction)
		# This is a CCXT list must convert to string
		amount: RP2Decimal = RP2Decimal(str(transaction[_AMOUNT]))

		intra_transaction_list.append(
			IntraTransaction(
				plugin=self.__BINANCE_COM,
				unique_id=transaction[_TXID],
				raw_data=json.dumps(transaction),
				timestamp=transaction[_DATETIME],
				asset=transaction[_CURRENCY],
				from_exchange=Keyword.UNKNOWN.value,
				from_holder=Keyword.UNKNOWN.value,
				to_exchange=self.__BINANCE_COM,
				to_holder=self.account_holder,
				spot_price=Keyword.UNKNOWN.value,
				crypto_sent=Keyword.UNKNOWN.value,
				crypto_received=str(amount),
			)
		)


	def _process_fiat_payment(
		self, transaction: Any, in_transaction_list: List[InTransaction], notes: Optional[str] = None 
	) -> None:
		self._process_deposit(transaction, in_transaction_list, notes)
		self._process_buy(transaction, in_transaction_list, notes)








		






