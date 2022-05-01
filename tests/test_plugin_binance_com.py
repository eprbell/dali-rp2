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

from typing import Any, Dict, List

from rp2.rp2_decimal import RP2Decimal

from dali.in_transaction import InTransaction
from dali.out_transaction import OutTransaction
from dali.plugin.input.rest.binance_com import InputPlugin
from dali.dali_configuration import Keyword


class TestBinance:

    # pylint: disable=no-self-use
    def test_deposits(self, mocker: Any) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            api_key="a",
            api_secret="b",
            username="user",
        )

        mocker.patch.object(plugin.client, "sapiGetFiatPayments").return_value = {
               "code": "000000",
               "message": "success",
               "data": [
               {
                  "orderNo": "353fca443f06466db0c4dc89f94f027a",
                  "sourceAmount": "20.0",  # Fiat trade amount
                  "fiatCurrency": "EUR",   # Fiat token
                  "obtainAmount": "4.462", # Crypto trade amount
                  "cryptoCurrency": "LUNA",  # Crypto token
                  "totalFee": "0.2",     # Trade fee
                  "price": "4.437472", 
                  "status": "Completed",  # Processing, Completed, Failed, Refunded
                  "createTime": 1624529919000,
                  "updateTime": 1624529919000  
               },
               {               
                  "orderNo": "353fca443f06466db0c4dc89f94f027b",
                  "sourceAmount": "40.0",  # Fiat trade amount
                  "fiatCurrency": "EUR",   # Fiat token
                  "obtainAmount": "8.924", # Crypto trade amount
                  "cryptoCurrency": "LUNA",  # Crypto token
                  "totalFee": "0.4",     # Trade fee
                  "price": "4.437472", 
                  "status": "Failed",  # Processing, Completed, Failed, Refunded
                  "createTime": 1624529920000,
                  "updateTime": 1624529920000  
               }
               ],
               "total": 2,
               "success": True
        }

        mocker.patch.object(plugin.client, "fetch_deposits").return_value = [
            {
                'info': {
                  'amount': '0.00999800', 
                  'coin': 'PAXG', 
                  'network': 'ETH', 
                  'status': '1', 
                  'address': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c', 
                  'addressTag': '', 
                  'txId': '0xaad4654a3234aa6118af9b4b335f5ae81c360b2394721c019b5d1e75328b09f3', 
                  'insertTime': '1599621997000', 
                  'transferType': '0', 
                  'confirmTimes': '12/12', 
                  'unlockConfirm': '12/12', 
                  'walletType': '0'
                },
                'id': None, 
                'txid': '0xaad4654a3234aa6118af9b4b335f5ae81c360b2394721c019b5d1e75328b09f3', 
                'timestamp': 1599621997000, 
                'datetime': '2020-09-09T03:26:37.000Z', 
                'network': 'ETH', 
                'address': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c', 
                'addressTo': '0x788cabe9236ce061e5a892e1a59395a81fc8d62c', 
                'addressFrom': None, 
                'tag': None, 
                'tagTo': None, 
                'tagFrom': None, 
                'type': 'deposit', 
                'amount': 0.00999800, 
                'currency': 'PAXG', 
                'status': 'ok', 
                'updated': None, 
                'internal': False, 
                'fee': None
            }
        ]

        mocker.patch.object(plugin.client, "sapiGetFiatOrders").return_value = {
            "code": "000000",
            "message": "success",
            "data": [
                {
                 "orderNo": "25ced37075c1470ba8939d0df2316e23",
                 "fiatCurrency": "EUR",
                 "indicatedAmount": "15.00",
                 "amount": "14.80",
                 "totalFee": "0.20",
                 "method": "card",
                 "status": "Completed",
                 "createTime": 1627501026000,
                 "updateTime": 1627501027000
                },
                {
                 "orderNo": "25ced37075c1470ba8939d0df2316e24",
                 "fiatCurrency": "EUR",
                 "indicatedAmount": "30.00",
                 "amount": "29.60",
                 "totalFee": "0.40",
                 "method": "card",
                 "status": "Failed",
                 "createTime": 1627501028000,
                 "updateTime": 1627501029000
                }

            ],
            "total": 1,
            "success": True
        }

        mocker.patch.object(plugin, "_process_trades").return_value = None
        mocker.patch.object(plugin, "_process_gains").return_value = None

        result = plugin.load()

        # 1 completed Fiat Payment +
        # 1 crypto Transfer +
        # 1 fiat deposit = 3
        assert len(result) == 3

        fiat_in_transaction: InTransaction = result[0]
        crypto_deposit_transaction: IntraTransaction = result[2]
        fiat_deposit: InTransaction = result[1]

        assert fiat_in_transaction.asset == "LUNA"
        assert fiat_in_transaction.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1624529919000)
        assert fiat_in_transaction.transaction_type == Keyword.BUY.value
        assert RP2Decimal(fiat_in_transaction.spot_price) == RP2Decimal("20.0") / RP2Decimal("4.462")
        assert RP2Decimal(fiat_in_transaction.crypto_in) == RP2Decimal("4.462")
        assert fiat_in_transaction.crypto_fee == None
        assert RP2Decimal(fiat_in_transaction.fiat_in_no_fee) == RP2Decimal("20.0")
        assert RP2Decimal(fiat_in_transaction.fiat_in_with_fee) == RP2Decimal("19.8")
        assert RP2Decimal(fiat_in_transaction.fiat_fee) == RP2Decimal("0.2")
        # assert fiat_in_transaction.fiat_iso_code == "EUR"

        assert crypto_deposit_transaction.asset == "PAXG"
        assert crypto_deposit_transaction.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1599621997000)
        assert crypto_deposit_transaction.from_exchange == Keyword.UNKNOWN.value
        assert crypto_deposit_transaction.to_exchange == "Binance.com"
        assert crypto_deposit_transaction.crypto_sent == Keyword.UNKNOWN.value
        assert RP2Decimal(crypto_deposit_transaction.crypto_received) == RP2Decimal("0.00999800")

        assert fiat_deposit.asset == "EUR"
        assert fiat_deposit.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1627501026000)
        assert fiat_deposit.transaction_type == Keyword.BUY.value
        assert RP2Decimal(fiat_deposit.spot_price) == RP2Decimal("1")
        assert RP2Decimal(fiat_deposit.crypto_in) == RP2Decimal("15.00")
        assert fiat_deposit.crypto_fee == None
        assert RP2Decimal(fiat_deposit.fiat_in_no_fee) == RP2Decimal("14.80")
        assert RP2Decimal(fiat_deposit.fiat_in_with_fee) == RP2Decimal("15.00")
        assert RP2Decimal(fiat_deposit.fiat_fee) == RP2Decimal("0.20")

    # pylint: disable=no-self-use
    def test_trades(self, mocker: Any) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            api_key="a",
            api_secret="b",
            username="user",
        )       
       
        plugin.markets = [{'id':'ETHBTC'}]
        mocker.patch.object(plugin.client, "fetch_my_trades").return_value = [

                # Trade using BNB for fee payment
                {
                  'info':         {'sample':'data'},          # the original decoded JSON as is
                  'id':           '12345-67890:09876/54321',  # string trade id
                  'timestamp':    1502962946216,              # Unix timestamp in milliseconds
                  'datetime':     '2017-08-17 12:42:48.000',  # ISO8601 datetime with milliseconds
                  'symbol':       'ETH/BTC',                  # symbol
                  'order':        '12345-67890:09876/54321',  # string order id or undefined/None/null
                  'type':         'limit',                    # order type, 'market', 'limit' or undefined/None/null
                  'side':         'buy',                      # direction of the trade, 'buy' or 'sell'
                  'takerOrMaker': 'taker',                    # string, 'taker' or 'maker'
                  'price':        0.06917684,                 # float price in quote currency
                  'amount':       1.5,                        # amount of base currency
                  'cost':         0.10376526,                 # total cost, `price * amount`,
                  'fee':          {                           # provided by exchange or calculated by ccxt
                      'cost':  0.0015,                        # float
                      'currency': 'BNB',                      # usually base currency for buys, quote currency for sells
                      'rate': 0.002,                          # the fee rate (if available)
                  },
                },

                # Trade using the quote currency for fee payment
                {
                  'info':         {'sample':'data'},          # the original decoded JSON as is
                  'id':           '12345-67890:09876/54321',  # string trade id
                  'timestamp':    1502962946217,              # Unix timestamp in milliseconds
                  'datetime':     '2017-08-17 12:42:48.000',  # ISO8601 datetime with milliseconds
                  'symbol':       'ETH/BTC',                  # symbol
                  'order':        '12345-67890:09876/54321',  # string order id or undefined/None/null
                  'type':         'limit',                    # order type, 'market', 'limit' or undefined/None/null
                  'side':         'buy',                      # direction of the trade, 'buy' or 'sell'
                  'takerOrMaker': 'taker',                    # string, 'taker' or 'maker'
                  'price':        0.06917684,                 # float price in quote currency
                  'amount':       3,                          # amount of base currency
                  'cost':         0.20753052,                 # total cost, `price * amount`,
                  'fee':          {                           # provided by exchange or calculated by ccxt
                      'cost':  0.0015,                        # float
                      'currency': 'ETH',                      # usually base currency for buys, quote currency for sells
                      'rate': 0.002,                          # the fee rate (if available)
                  },
                },

                # Sell trade using the quote currency for fee payment
                {
                  'info':         {'sample':'data'},          # the original decoded JSON as is
                  'id':           '12345-67890:09876/54321',  # string trade id
                  'timestamp':    1502962946218,              # Unix timestamp in milliseconds
                  'datetime':     '2017-08-17 12:42:48.000',  # ISO8601 datetime with milliseconds
                  'symbol':       'ETH/BTC',                  # symbol
                  'order':        '12345-67890:09876/54321',  # string order id or undefined/None/null
                  'type':         'limit',                    # order type, 'market', 'limit' or undefined/None/null
                  'side':         'sell',                      # direction of the trade, 'buy' or 'sell'
                  'takerOrMaker': 'taker',                    # string, 'taker' or 'maker'
                  'price':        0.06917684,                 # float price in quote currency
                  'amount':       6,                          # amount of base currency
                  'cost':         0.41506104,                 # total cost, `price * amount`,
                  'fee':          {                           # provided by exchange or calculated by ccxt
                      'cost':  0.0015,                        # float
                      'currency': 'BTC',                      # usually base currency for buys, quote currency for sells
                      'rate': 0.002,                          # the fee rate (if available)
                  },
                },
            ]

        # CCXT abstracts dust trades into regular trades, so no testing is necessary
        mocker.patch.object(plugin.client, "fetch_my_dust_trades").return_value = []
        mocker.patch.object(plugin, "_process_deposits").return_value = None
        mocker.patch.object(plugin, "_process_gains").return_value = None

        result = plugin.load()

        # One Sell of quote asset (using BNB) +
        # One Buy of base asset (using BNB) +
        # One payment for fees in BNB
        # One Sell of quote asset (for Buy order) +
        # One Buy of base asset (for Buy order) +
        # One Sell of quote asset (for Sell order) +
        # One Buy of base asset (for Sell order) = 7
        assert len(result) == 7

        BNB_sell_transaction: OutTransaction = result[3]
        BNB_buy_transaction: InTransaction = result[0]
        BNB_fee_transaction: OutTransaction = result[4]
        regular_sell: OutTransaction = result[5]
        regular_buy: InTransaction = result[1]
        sell_order_sell: OutTransaction = result[6]
        sell_order_buy: InTransaction = result[2]

        # Buy with BNB as fee payment
        assert BNB_sell_transaction.asset == "BTC"
        assert BNB_sell_transaction.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1502962946216)
        assert BNB_sell_transaction.transaction_type == Keyword.SELL.value
        assert BNB_sell_transaction.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(BNB_sell_transaction.crypto_out_no_fee) == RP2Decimal("0.10376526")
        assert RP2Decimal(BNB_sell_transaction.crypto_fee) == RP2Decimal("0")
        assert RP2Decimal(BNB_sell_transaction.crypto_out_with_fee) == RP2Decimal("0.10376526")
        assert BNB_sell_transaction.fiat_out_no_fee == None
        assert BNB_sell_transaction.fiat_fee == None

        assert BNB_buy_transaction.asset == "ETH"
        assert BNB_buy_transaction.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1502962946216)
        assert BNB_buy_transaction.transaction_type == Keyword.BUY.value
        assert BNB_buy_transaction.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(BNB_buy_transaction.crypto_in) == RP2Decimal("1.5")
        assert RP2Decimal(BNB_buy_transaction.crypto_fee) == RP2Decimal("0")
        assert BNB_buy_transaction.fiat_in_no_fee == None
        assert BNB_buy_transaction.fiat_in_with_fee == None
        assert BNB_buy_transaction.fiat_fee == None

        assert BNB_fee_transaction.asset == "BNB"
        assert BNB_fee_transaction.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1502962946216)
        assert BNB_fee_transaction.transaction_type == Keyword.FEE.value
        assert BNB_fee_transaction.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(BNB_fee_transaction.crypto_out_no_fee) == RP2Decimal("0.0015")
        assert RP2Decimal(BNB_fee_transaction.crypto_fee) == RP2Decimal("0")
        assert RP2Decimal(BNB_fee_transaction.crypto_out_with_fee) == RP2Decimal("0.0015")
        assert BNB_fee_transaction.fiat_out_no_fee == None
        assert BNB_fee_transaction.fiat_fee == None

        # Buy with base asset as fee payment
        assert regular_sell.asset == "BTC"
        assert regular_sell.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1502962946217)
        assert regular_sell.transaction_type == Keyword.SELL.value
        assert regular_sell.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(regular_sell.crypto_out_no_fee) == RP2Decimal("0.20753052")
        assert RP2Decimal(regular_sell.crypto_fee) == RP2Decimal("0")
        assert RP2Decimal(regular_sell.crypto_out_with_fee) == RP2Decimal("0.20753052")
        assert regular_sell.fiat_out_no_fee == None
        assert regular_sell.fiat_fee == None  

        assert regular_buy.asset == "ETH"
        assert regular_buy.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1502962946217)
        assert regular_buy.transaction_type == Keyword.BUY.value
        assert regular_buy.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(regular_buy.crypto_in) == RP2Decimal("3")
        assert RP2Decimal(regular_buy.crypto_fee) == RP2Decimal("0.0015")
        assert regular_buy.fiat_in_no_fee == None
        assert regular_buy.fiat_in_with_fee == None
        assert regular_buy.fiat_fee == None     

        # Sell with quote asset as fee payment
        assert sell_order_sell.asset == "ETH"
        assert sell_order_sell.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1502962946218)
        assert sell_order_sell.transaction_type == Keyword.SELL.value
        assert sell_order_sell.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(sell_order_sell.crypto_out_no_fee) == RP2Decimal("6")
        assert RP2Decimal(sell_order_sell.crypto_fee) == RP2Decimal("0")
        assert RP2Decimal(sell_order_sell.crypto_out_with_fee) == RP2Decimal("6")
        assert sell_order_sell.fiat_out_no_fee == None
        assert sell_order_sell.fiat_fee == None  

        assert sell_order_buy.asset == "BTC"
        assert sell_order_buy.timestamp == InputPlugin._rp2timestamp_from_ms_epoch(1502962946218)
        assert sell_order_buy.transaction_type == Keyword.BUY.value
        assert sell_order_buy.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(sell_order_buy.crypto_in) == RP2Decimal("0.41506104")
        assert RP2Decimal(sell_order_buy.crypto_fee) == RP2Decimal("0.0015")
        assert sell_order_buy.fiat_in_no_fee == None
        assert sell_order_buy.fiat_in_with_fee == None
        assert sell_order_buy.fiat_fee == None 

    # pylint: disable=no-self-use
    def test_gains(self, mocker: Any) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            api_key="a",
            api_secret="b",
            username="user",
        ) 