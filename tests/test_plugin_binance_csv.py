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

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

from ccxt import binance, kraken
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.cache import CACHE_DIR, load_from_cache
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.plugin.input.csv.binance_com import InputPlugin


class TestBinanceCsv:
    def test_autoinvest(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            autoinvest_csv_file="input/test_binance_autoinvest.csv",
            betheth_csv_file=None,
            native_fiat="USD",
        )

        result = plugin.load()

        # 1 ETH in transaction for 0.01 ETH +
        # 1 USDT out transaction for 0.01 ETH +
        # 1 BTC in transaction for 0.002 BTC +
        # 1 BTC out transaction for 0.002 BTC = 4
        assert len(result) == 4

        eth_transaction_in: InTransaction = result[0] # type: ignore
        eth_transaction_out: OutTransaction = result[2] # type: ignore
        btc_transaction_in: InTransaction = result[1] # type: ignore
        btc_transaction_out: OutTransaction = result[3] # type: ignore

        # Buy ETH autoinvest
        assert eth_transaction_in.asset == "ETH"
        assert eth_transaction_in.timestamp == "2021-05-01 14:00:00 +0000"
        assert eth_transaction_in.transaction_type == Keyword.BUY.value
        assert eth_transaction_in.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(eth_transaction_in.crypto_in) == RP2Decimal("0.01")
        assert RP2Decimal(eth_transaction_in.crypto_fee) == ZERO
        assert eth_transaction_in.fiat_in_no_fee is None
        assert eth_transaction_in.fiat_in_with_fee is None
        assert eth_transaction_in.fiat_fee is None

        assert eth_transaction_out.asset == "USDT"
        assert eth_transaction_out.timestamp == "2021-05-01 14:00:00 +0000"
        assert eth_transaction_out.transaction_type == Keyword.SELL.value
        assert eth_transaction_out.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(eth_transaction_out.crypto_out_no_fee) == RP2Decimal("10.00")
        assert RP2Decimal(eth_transaction_out.crypto_fee) == RP2Decimal("0.0025")
        assert RP2Decimal(eth_transaction_out.crypto_out_with_fee) == RP2Decimal("10.0025")
        assert eth_transaction_out.fiat_out_no_fee is None
        assert eth_transaction_out.fiat_fee is None

        # Buy BTC autoinvest
        assert btc_transaction_in.asset == "BTC"
        assert btc_transaction_in.timestamp == "2021-05-01 14:00:00 +0000"
        assert btc_transaction_in.transaction_type == Keyword.BUY.value
        assert btc_transaction_in.spot_price == Keyword.UNKNOWN.value
        assert btc_transaction_in.crypto_in == RP2Decimal("0.002")
        assert btc_transaction_in.crypto_fee == ZERO
        assert btc_transaction_in.fiat_in_no_fee is None
        assert btc_transaction_in.fiat_in_with_fee is None
        assert btc_transaction_in.fiat_fee is None

        assert btc_transaction_out.asset == "USDT"
        assert btc_transaction_out.timestamp == "2021-05-01 14:00:00 +0000"
        assert btc_transaction_out.transaction_type == Keyword.SELL.value
        assert btc_transaction_out.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(btc_transaction_out.crypto_out_no_fee) == RP2Decimal("20.00")
        assert RP2Decimal(btc_transaction_out.crypto_fee) == RP2Decimal("0.005")
        assert RP2Decimal(btc_transaction_out.crypto_out_with_fee) == RP2Decimal("20.005")
        assert btc_transaction_out.fiat_out_no_fee is None
        assert btc_transaction_out.fiat_fee is None

    def test_betheth(self) -> None:
        plugin = InputPlugin(
            account_holder="tester",
            autoinvest_csv_file=None,
            betheth_csv_file="input/test_binance_betheth.csv",
            native_fiat="USD",
        )

        result = plugin.load()

        # 1 ETH out transaction for 0.1 ETH +
        # 1 BETH in transaction for 0.1 BETH = 2
        assert len(result) == 2

        betheth_transaction_in: InTransaction = result[0] # type: ignore
        betheth_transaction_out: OutTransaction = result[1] # type: ignore

        assert betheth_transaction_in.asset == "BETH"
        assert betheth_transaction_in.timestamp == "2021-03-01 12:00:00 +0000"
        assert betheth_transaction_in.transaction_type == Keyword.BUY.value
        assert betheth_transaction_in.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(betheth_transaction_in.crypto_in) == RP2Decimal("0.1")
        assert RP2Decimal(betheth_transaction_in.crypto_fee) == ZERO
        assert betheth_transaction_in.fiat_in_no_fee is None
        assert betheth_transaction_in.fiat_in_with_fee is None
        assert betheth_transaction_in.fiat_fee is None

        assert betheth_transaction_out.asset == "ETH"
        assert betheth_transaction_out.timestamp == "2021-03-01 12:00:00 +0000"
        assert betheth_transaction_out.transaction_type == Keyword.SELL.value
        assert betheth_transaction_out.spot_price == Keyword.UNKNOWN.value
        assert RP2Decimal(betheth_transaction_out.crypto_out_no_fee) == RP2Decimal("0.1")
        assert RP2Decimal(betheth_transaction_out.crypto_fee) == ZERO
        assert RP2Decimal(betheth_transaction_out.crypto_out_with_fee) == RP2Decimal("0.1")
        assert betheth_transaction_out.fiat_out_no_fee is None
        assert betheth_transaction_out.fiat_fee is None
