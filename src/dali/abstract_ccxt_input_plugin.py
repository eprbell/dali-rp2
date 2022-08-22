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

# CCXT documentation:
# https://docs.ccxt.com/en/latest/index.html

import json
import logging
import re
from datetime import datetime, timezone
from time import sleep
from typing import Any, Dict, List, NamedTuple, Optional, Union

from ccxt import DDoSProtection, InvalidNonce, binance
from rp2.logger import create_logger
from rp2.rp2_decimal import ZERO, RP2Decimal

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

# Native format keywords
_BUY: str = "buy"  # CCXT only variable
_COST: str = "cost"  # CCXT only variable
_CURRENCY: str = "currency"  # CCXT only variable
_DATE_TIME: str = "datetime"  # CCXT only variable
_DEPOSIT: str = "deposit"  # CCXT only variable
_ID: str = "id"  # CCXT only variable
_ORDER: str = "order"  # CCXT only variable
_SELL: str = "sell"  # CCXT only variable
_SIDE: str = "side"  # CCXT only variable
_TIMESTAMP: str = "timestamp"  # CCXT only variable
_TX_ID: str = "txid"  # CCXT doesn't capitalize I
_WITHDRAWAL: str = "withdrawal"  # CCXT only variable

# Time period constants
_NINETY_DAYS_IN_MS: int = 7776000000
_THIRTY_DAYS_IN_MS: int = 2592000000
_ONE_DAY_IN_MS: int = 86400000
_MS_IN_SECOND: int = 1000

# Default names
_DEFAULT_PLUGIN_NAME: str = "Abstract_CCXT_Plugin"
_DEFAULT_EXCHANGE_NAME: str = "CCXT_Compatible_Exchange"

class _ProcessAccountResult(NamedTuple):
    in_transactions: List[InTransaction]
    out_transactions: List[OutTransaction]
    intra_transactions: List[IntraTransaction]


class _Trade(NamedTuple):
    base_asset: str
    quote_asset: str
    base_info: str
    quote_info: str


class AbstractCcxtInputPlugin(AbstractInputPlugin):

    def __init__(
        self,
        account_holder: str,
        api_key: str,
        api_secret: str,
        native_fiat: str,
		exchange_start_time: datetime,
    ) -> None:

        super().__init__(account_holder, native_fiat)
        self.__logger: logging.Logger = create_logger(f"{exchange_name}/{self.account_holder}")
        self.__cache_key: str = f"{exchange_name.lower()}-{account_holder}"
        self.client: Exchange = exchange_class(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True, 
            }
        )

        self.markets: List[str] = []
        self.start_time: datetime = exchange_start_time
        self.start_time_ms: int = int(self.start_time.timestamp()) * _MS_IN_SECOND

    def cache_key(self) -> Optional[str]:
        return self.__cache_key

    def plugin_name(self) -> Optional[str]:
        return _DEFAULT_PLUGIN_NAME

    def exchange_class(self) -> Exchange
        raise NotImplementedError("Abstract method: it must be implemented in the plugin class")

    def exchange_name(self) -> Optional[str]:
        return _DEFAULT_EXCHANGE_NAME

    @staticmethod
    def _rp2_timestamp_from_ms_epoch(epoch_timestamp: str) -> str:
        rp2_time = datetime.fromtimestamp((int(epoch_timestamp) / _MS_IN_SECOND), timezone.utc)

        return rp2_time.strftime("%Y-%m-%d %H:%M:%S%z")

    @staticmethod
    def _to_trade(market_pair: str, base_amount: str, quote_amount: str) -> _Trade:
        assets = market_pair.split("/")
        return _Trade(
            base_asset=assets[0],
            quote_asset=assets[1],
            base_info=f"{base_amount} {assets[0]}",
            quote_info=f"{quote_amount} {assets[1]}",
        )

    def load(self) -> List[AbstractTransaction]:
        result: List[AbstractTransaction] = []
        in_transactions: List[InTransaction] = []
        out_transactions: List[OutTransaction] = []
        intra_transactions: List[IntraTransaction] = []

        ccxt_markets: Any = self.client.fetch_markets()
        for market in ccxt_markets:
            self.__logger.debug("Market: %s", json.dumps(market))
            self.markets.append(market[_ID])

        # TODO - Pull valid fiat from abstract_converter_plugin

        # TODO - Call standard list of unified functions (Trade, Deposit, Withdrawal)

        result.extend(in_transactions)
        result.extend(out_transactions)
        result.extend(intra_transactions)

        return result

    ### Multiple Transaction Processing or Pagination Methods

    # TODO - _process_trades()
    # TODO - _process_deposits() - need generic fetch_deposits()
    # TODO - _process_withdrawals()

    # TODO - generic pagination methods that take functions?

    ### Single Transaction Processing

    def _process_buy(
        self, transaction: Any, exchange: str, in_transaction_list: List[InTransaction], out_transaction_list: List[OutTransaction], notes: Optional[str] = None
    ) -> _ProcessAccountResult:

        account_result: _ProcessAccountResult
        crypto_in: RP2Decimal
        crypto_fee: RP2Decimal

        # TODO not CCXT standard
        if _IS_FIATPAYMENT in transaction:
            in_transaction_list.append(
                InTransaction(
                    plugin=self.plugin_name(),
                    unique_id=transaction[_ORDER_NO],
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2timestamp_from_ms_epoch(transaction[_CREATE_TIME]),
                    asset=transaction[_CRYPTOCURRENCY],
                    exchange=self.exchange_name(),
                    holder=self.account_holder,
                    transaction_type=Keyword.BUY.value,
                    spot_price=str(RP2Decimal(transaction[_PRICE])),
                    crypto_in=transaction[_OBTAIN_AMOUNT],
                    crypto_fee=None,
                    fiat_in_no_fee=str(RP2Decimal(transaction[_SOURCEAMOUNT]) - RP2Decimal(transaction[_TOTALFEE])),
                    fiat_in_with_fee=str(transaction[_SOURCEAMOUNT]),
                    fiat_fee=str(RP2Decimal(transaction[_TOTALFEE])),
                    fiat_ticker=transaction[_FIAT_CURRENCY],
                    notes=(f"Buy transaction for fiat payment orderNo - " f"{transaction[_ORDER_NO]}"),
                )
            )

        else:
            trade: _Trade = self._to_trade(transaction[_SYMBOL], str(transaction[_AMOUNT]), str(transaction[_COST]))
            if transaction[_SIDE] == _BUY:
                out_asset = trade.quote_asset
                in_asset = trade.base_asset
                crypto_in = RP2Decimal(str(transaction[_AMOUNT]))
                conversion_info = f"{trade.quote_info} -> {trade.base_info}"
            elif transaction[_SIDE] == _SELL:
                out_asset = trade.base_asset
                in_asset = trade.quote_asset
                crypto_in = RP2Decimal(str(transaction[_COST]))
                conversion_info = f"{trade.base_info} -> {trade.quote_info}"
            else:
                raise Exception(f"Internal error: unrecognized transaction side: {transaction[_SIDE]}")

            if transaction[_FEE][_CURRENCY] == in_asset:
                crypto_fee = RP2Decimal(str(transaction[_FEE][_COST]))
            else:
                crypto_fee = ZERO
                transaction_fee = transaction[_FEE][_COST]

                # Users can use other crypto assets to pay for trades
                # CCXT standard format
                if transaction[_FEE][_CURRENCY] != out_asset and float(transaction_fee) > 0:
                    out_transaction_list.append(
                        OutTransaction(
                            plugin=self.plugin_name(),
                            unique_id=transaction[_ID],
                            raw_data=json.dumps(transaction),
                            timestamp=self._rp2timestamp_from_ms_epoch(transaction[_TIMESTAMP]),
                            asset=transaction[_FEE][_CURRENCY],
                            exchange=self.exchange_name(),
                            holder=self.account_holder,
                            transaction_type=Keyword.FEE.value,
                            spot_price=Keyword.UNKNOWN.value,
                            crypto_out_no_fee="0",
                            crypto_fee=str(transaction_fee),
                            crypto_out_with_fee=str(transaction_fee),
                            fiat_out_no_fee=None,
                            fiat_fee=None,
                            notes=(f"{notes + '; ' if notes else ''} Fee for conversion from " f"{conversion_info}"),
                        )
                    )

            # Is this a plain buy or a conversion?
            if trade.quote_asset in self.client.options[_LEGAL_MONEY]:  # TODO use list of fiat pulled from exchangerate.hosts
                fiat_in_with_fee = RP2Decimal(str(transaction[_COST]))
                fiat_fee = RP2Decimal(crypto_fee)
                spot_price = RP2Decimal(str(transaction[_PRICE]))
                if transaction[_SIDE] == _BUY:
                    transaction_notes = f"Fiat buy of {trade.base_asset} with {trade.quote_asset}"
                    fiat_in_no_fee = fiat_in_with_fee - (fiat_fee * spot_price)
                elif transaction[_SIDE] == _SELL:
                    transaction_notes = f"Fiat sell of {trade.base_asset} into {trade.quote_asset}"
                    fiat_in_no_fee = fiat_in_with_fee - fiat_fee

                # CCXT standard format
                in_transaction_list.append(
                    InTransaction(
                        plugin=self.plugin_name(),
                        unique_id=transaction[_ID],
                        raw_data=json.dumps(transaction),
                        timestamp=self._rp2timestamp_from_ms_epoch(transaction[_TIMESTAMP]),
                        asset=in_asset,
                        exchange=self.exchange_name(),
                        holder=self.account_holder,
                        transaction_type=Keyword.BUY.value,
                        spot_price=str(spot_price),
                        crypto_in=str(crypto_in),
                        crypto_fee=str(crypto_fee),
                        fiat_in_no_fee=str(fiat_in_no_fee),
                        fiat_in_with_fee=str(fiat_in_with_fee),
                        fiat_fee=None,
                        fiat_ticker=trade.quote_asset,
                        notes=(f"{notes + '; ' if notes else ''} {transaction_notes}"),
                    )
                )

            else:
                transaction_notes = f"Buy side of conversion from " f"{conversion_info}" f"({out_asset} out-transaction unique id: {transaction[_ID]}"

                # CCXT standard format
                in_transaction_list.append(
                    InTransaction(
                        plugin=self.plugin_name(),
                        unique_id=transaction[_ID],
                        raw_data=json.dumps(transaction),
                        timestamp=self._rp2timestamp_from_ms_epoch(transaction[_TIMESTAMP]),
                        asset=in_asset,
                        exchange=self.exchange_name(),
                        holder=self.account_holder,
                        transaction_type=Keyword.BUY.value,
                        spot_price=Keyword.UNKNOWN.value,
                        crypto_in=str(crypto_in),
                        crypto_fee=str(crypto_fee),
                        fiat_in_no_fee=None,
                        fiat_in_with_fee=None,
                        fiat_fee=None,
                        notes=(f"{notes + '; ' if notes else ''} {transaction_notes}"),
                    )
                )

    def _process_deposit(self, transaction: Any, in_transaction_list: List[InTransaction], notes: Optional[str] = None) -> None:

        amount: RP2Decimal = RP2Decimal(transaction[_INDICATED_AMOUNT])
        fee: RP2Decimal = RP2Decimal(transaction[_TOTALFEE])
        notes = f"{notes + '; ' if notes else ''}{'Fiat Deposit of '}; {transaction[_FIAT_CURRENCY]}"
        # not CCXT standard 
        in_transaction_list.append(
            InTransaction(
                plugin=self.plugin_name(),
                unique_id=transaction[_ORDER_NO],
                raw_data=json.dumps(transaction),
                timestamp=self._rp2timestamp_from_ms_epoch(transaction[_CREATE_TIME]),
                asset=transaction[_FIAT_CURRENCY],
                exchange=self.exchange_name(),
                holder=self.account_holder,
                transaction_type=Keyword.BUY.value,
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

    def _process_gain(self, transaction: Any, transaction_type: Keyword, in_transaction_list: List[InTransaction], notes: Optional[str] = None) -> None:

        if transaction_type == Keyword.MINING:
            amount: RP2Decimal = RP2Decimal(str(transaction[_PROFITAMOUNT]))
            notes = f"{notes + '; ' if notes else ''}'Mining profit'"

            # not CCXT standard
            in_transaction_list.append(
                InTransaction(
                    plugin=self.plugin_name(),
                    unique_id=Keyword.UNKNOWN.value,
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2timestamp_from_ms_epoch(transaction[_TIME]),
                    asset=transaction[_COIN_NAME],
                    exchange=self.exchange_name(),
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
        else:
            amount = RP2Decimal(transaction[_AMOUNT])
            notes = f"{notes + '; ' if notes else ''}{transaction[_EN_INFO]}" # TODO - Generify this variable

            # not CCXT standard
            in_transaction_list.append(
                InTransaction(
                    plugin=self.plugin_name(),
                    unique_id=str(transaction[_ID]),
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2timestamp_from_ms_epoch(transaction[_DIV_TIME]), # TODO 
                    asset=transaction[_ASSET],
                    exchange=self.exchange_name(),
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

    def _process_sell(self, transaction: Any, out_transaction_list: List[OutTransaction], notes: Optional[str] = None) -> None:
        trade: _Trade = self._to_trade(transaction[_SYMBOL], str(transaction[_AMOUNT]), str(transaction[_COST]))

        # For some reason CCXT outputs amounts in float
        if transaction[_SIDE] == _BUY:
            out_asset = trade.quote_asset
            in_asset = trade.base_asset
            crypto_out_no_fee: RP2Decimal = RP2Decimal(str(transaction[_COST]))
            conversion_info = f"{trade.quote_info} -> {trade.base_info}"
        elif transaction[_SIDE] == _SELL:
            out_asset = trade.base_asset
            in_asset = trade.quote_asset
            crypto_out_no_fee = RP2Decimal(str(transaction[_AMOUNT]))
            conversion_info = f"{trade.base_info} -> {trade.quote_info}"
        else:
            raise Exception(f"Internal error: unrecognized transaction side: {transaction[_SIDE]}")

        if transaction[_FEE][_CURRENCY] == out_asset:
            crypto_fee: RP2Decimal = RP2Decimal(str(transaction[_FEE][_COST]))
        else:
            crypto_fee = ZERO
        crypto_out_with_fee: RP2Decimal = crypto_out_no_fee + crypto_fee

        # Is this a plain buy or a conversion?
        if trade.quote_asset in self.client.options[_LEGAL_MONEY]:  # TODO - use list of fiat from exchangehosts
            fiat_out_no_fee: RP2Decimal = RP2Decimal(str(transaction[_COST]))
            fiat_fee: RP2Decimal = RP2Decimal(crypto_fee)
            spot_price: RP2Decimal = RP2Decimal(str(transaction[_PRICE]))

            # CCXT standard format
            out_transaction_list.append(
                OutTransaction(
                    plugin=self.plugin_name(),
                    unique_id=transaction[_ID],
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2timestamp_from_ms_epoch(transaction[_TIMESTAMP]),
                    asset=out_asset,
                    exchange=self.exchange_name(),
                    holder=self.account_holder,
                    transaction_type=Keyword.SELL.value,
                    spot_price=str(spot_price),
                    crypto_out_no_fee=str(crypto_out_no_fee),
                    crypto_fee=str(crypto_fee),
                    crypto_out_with_fee=str(crypto_out_with_fee),
                    fiat_out_no_fee=str(fiat_out_no_fee),
                    fiat_fee=str(fiat_fee),
                    fiat_ticker=trade.quote_asset,
                    notes=(f"{notes + ';' if notes else ''} Fiat sell of {trade.base_asset} with {trade.quote_asset}."),
                )
            )

        else:
            # CCXT does not report the value of the transaction in fiat
            # CCXT standard format
            out_transaction_list.append(
                OutTransaction(
                    plugin=self.plugin_name(),
                    unique_id=transaction[_ID],
                    raw_data=json.dumps(transaction),
                    timestamp=self._rp2timestamp_from_ms_epoch(transaction[_TIMESTAMP]),
                    asset=out_asset,
                    exchange=self.exchange_name(),
                    holder=self.account_holder,
                    transaction_type=Keyword.SELL.value,
                    spot_price=Keyword.UNKNOWN.value,
                    crypto_out_no_fee=str(crypto_out_no_fee),
                    crypto_fee=str(crypto_fee),
                    crypto_out_with_fee=str(crypto_out_with_fee),
                    fiat_out_no_fee=None,
                    fiat_fee=None,
                    notes=(
                        f"{notes + '; ' if notes else ''} Sell side of conversion from "
                        f"{conversion_info}"
                        f"({in_asset} in-transaction unique id: {transaction[_ID]}"
                    ),
                )
            )

    def _process_transfer(self, transaction: Any, intra_transaction_list: List[IntraTransaction]) -> None:

        # This is a CCXT list must convert to string from float
        amount: RP2Decimal = RP2Decimal(str(transaction[_AMOUNT]))

        if transaction[_TYPE] == _DEPOSIT:
            intra_transaction_list.append(
                IntraTransaction(
                    plugin=self.plugin_name(),
                    unique_id=transaction[_TXID],
                    raw_data=json.dumps(transaction),
                    timestamp=transaction[_DATE_TIME],
                    asset=transaction[_CURRENCY],
                    from_exchange=Keyword.UNKNOWN.value,
                    from_holder=Keyword.UNKNOWN.value,
                    to_exchange=self.exchange_name(),
                    to_holder=self.account_holder,
                    spot_price=Keyword.UNKNOWN.value,
                    crypto_sent=Keyword.UNKNOWN.value,
                    crypto_received=str(amount),
                )
            )
        elif transaction[_TYPE] == _WITHDRAWAL:
            intra_transaction_list.append(
                IntraTransaction(
                    plugin=self.plugin_name(),
                    unique_id=transaction[_TXID],
                    raw_data=json.dumps(transaction),
                    timestamp=transaction[_DATE_TIME],
                    asset=transaction[_CURRENCY],
                    from_exchange=self.exchange_name(),
                    from_holder=self.account_holder,
                    to_exchange=Keyword.UNKNOWN.value,
                    to_holder=Keyword.UNKNOWN.value,
                    spot_price=Keyword.UNKNOWN.value,
                    crypto_sent=str(amount),
                    crypto_received=Keyword.UNKNOWN.value,
                )
            )
        else:
            self.__logger.error("Unrecognized Crypto transfer: %s", json.dumps(transaction))

    def _process_withdrawal(self, transaction: Any, out_transaction_list: List[OutTransaction], notes: Optional[str] = None) -> None:

        # not CCXT standard format
        amount: RP2Decimal = RP2Decimal(transaction[_INDICATED_AMOUNT])
        fee: RP2Decimal = RP2Decimal(transaction[_TOTALFEE])
        notes = f"{notes + '; ' if notes else ''}{'Fiat Withdrawal of '}; {transaction[_FIAT_CURRENCY]}"
        out_transaction_list.append(
            OutTransaction(
                plugin=self.plugin_name(),
                unique_id=transaction[_ORDER_NO],
                raw_data=json.dumps(transaction),
                timestamp=self._rp2timestamp_from_ms_epoch(transaction[_CREATE_TIME]),
                asset=transaction[_FIAT_CURRENCY],
                exchange=self.exchange_name(),
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
