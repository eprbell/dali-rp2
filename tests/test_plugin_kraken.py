# Copyright 2023 ndopencode
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

# pylint: disable=protected-access

from typing import Any, Dict

import pytest
from ccxt import Exchange
from rp2.rp2_error import RP2RuntimeError

from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction
from dali.plugin.input.rest.kraken import _BASE, _BASE_ID, _ID, _QUOTE, InputPlugin


@pytest.fixture(name="private_post_ledgers_return")
def private_post_ledgers_return_fixture() -> Dict[str, Any]:  # type: ignore
    return {
        "error": [],
        "result": {
            "count": "4",
            "ledger": {
                "deposit": {
                    "aclass": "currency",
                    "amount": "3.0000000000",
                    "asset": "XLTC",
                    "balance": "3.0000000000",
                    "fee": "0.0000000000",
                    "refid": "somerefid0",
                    "time": "1406104521.2614448",
                    "type": "deposit",
                    "subtype": "",
                },
                "withdrawal": {
                    "aclass": "currency",
                    "amount": "9.4149333100",
                    "asset": "XLTC",
                    "balance": "9.4149333100",
                    "fee": "0.0000000000",
                    "refid": "somerefid1",
                    "time": "1406812141.5408714",
                    "type": "withdrawal",
                    "subtype": "",
                },
                "buy_trade": {
                    "aclass": "currency",
                    "amount": "8.0906135700",
                    "asset": "XLTC",
                    "balance": "8.0906189200",
                    "fee": "0.0000000000",
                    "refid": "somerefid2",
                    "time": "1403860822.8546317",
                    "type": "trade",
                    "subtype": "",
                },
                "sell_trade": {
                    "aclass": "currency",
                    "amount": "-3.0000000000",
                    "asset": "XLTC",
                    "balance": "0.0000000000",
                    "fee": "0.0000000000",
                    "refid": "somerefid3",
                    "time": "1406122046.414707",
                    "type": "trade",
                    "subtype": "",
                },
            },
        },
    }


@pytest.fixture(name="private_post_tradeshistory_return")
def private_post_tradeshistory_return_fixture() -> Dict[str, Any]:  # type: ignore
    return {
        "error": [],
        "result": {
            "count": "2",
            "trades": {
                "somerefid2": {
                    "ordertxid": "ordertxid",
                    "pair": "XLTCZUSD",
                    "time": "1403860822.8528068",
                    "type": "buy",
                    "ordertype": "limit",
                    "price": "43.80001",
                    "cost": "500.00000",
                    "fee": "0.80000",
                    "vol": "8.09061357",
                    "margin": "0.00000",
                    "leverage": "0",
                    "misc": "",
                    "trade_id": "0",
                },
                "somerefid3": {
                    "ordertxid": "ordertxid",
                    "pair": "XLTCXXBT",
                    "time": "1406122046.410762",
                    "type": "sell",
                    "ordertype": "market",
                    "price": "0.01133800",
                    "cost": "0.03401400",
                    "fee": "0.00008844",
                    "vol": "3.00000000",
                    "margin": "0.00000000",
                    "leverage": "0",
                    "misc": "",
                    "trade_id": "1",
                },
            },
        },
    }


@pytest.fixture(name="plugin")
def plugin_fixture() -> InputPlugin:
    return InputPlugin(
        account_holder="tester",
        api_key="a",
        api_secret="b",
        native_fiat="USD",
        use_cache=False,
    )


def test_initialize_markets_exception(plugin: InputPlugin, mocker: Any) -> None:
    """
    This tests failure when old data format is used
    """
    client: Exchange = plugin._client

    mocker.patch.object(client, "load_markets").return_value = None
    mocker.patch.object(
        client,
        "markets_by_id",
        {
            "XLTCZUSD": {_ID: "XLTCZUSD", _BASE_ID: "XLTC", _BASE: "LTC", _QUOTE: "USD"},
            "XLTCXXBT": {_ID: "XLTCXXBT", _BASE_ID: "XLTC", _BASE: "LTC", _QUOTE: "BTC"},
        },
    )

    with pytest.raises(RP2RuntimeError) as excinfo:
        plugin.load(country=None)  # type: ignore

    assert "Incompatible CCXT library - make sure to follow Dali setup instructions" in str(excinfo.value)


def test_initialize_markets_multiple_bases(plugin: InputPlugin, mocker: Any) -> None:
    """
    This tests failure when multiple bases exist for a base_id in set of markets
    """
    client: Exchange = plugin._client

    mocker.patch.object(client, "load_markets").return_value = None
    mocker.patch.object(
        client,
        "markets_by_id",
        {
            "XLTCZUSD": [{_ID: "XLTCZUSD", _BASE_ID: "XLTC", _BASE: "LTC", _QUOTE: "USD"}],
            "XLTCXXBT": [
                {_ID: "XLTCXXBT", _BASE_ID: "XLTC", _BASE: "LTC", _QUOTE: "BTC"},
                {_ID: "XLTCXXBT", _BASE_ID: "XLTC", _BASE: "XLTC", _QUOTE: "BTC"},
            ],
        },
    )

    with pytest.raises(RP2RuntimeError) as excinfo:
        plugin.load(country=None)  # type: ignore

    assert "A Kraken market's BASE differs with another BASE for the same BASE_ID" in str(excinfo.value)


def test_initialize_markets_multiple_quotes_to_base_pair(
    mocker: Any,
    plugin: InputPlugin,
    private_post_ledgers_return: Dict[str, Any],
    private_post_tradeshistory_return: Dict[str, Any],
) -> None:
    """
    This tests failure when there are multiple quote symbols to a base symbol in a set of markets
    """
    client: Exchange = plugin._client

    mocker.patch.object(client, "load_markets").return_value = None
    mocker.patch.object(
        client,
        "markets_by_id",
        {
            "XLTCZUSD": [{_ID: "XLTCZUSD", _BASE_ID: "XLTC", _BASE: "LTC", _QUOTE: "USD"}, {_ID: "XLTCZUSD", _BASE_ID: "XLTC", _BASE: "LTC", _QUOTE: "ZUSD"}],
            "XLTCXXBT": [{_ID: "XLTCXXBT", _BASE_ID: "XLTC", _BASE: "LTC", _QUOTE: "BTC"}],
        },
    )
    mocker.patch.object(client, "private_post_ledgers").return_value = private_post_ledgers_return
    mocker.patch.object(client, "private_post_tradeshistory").return_value = private_post_tradeshistory_return

    with pytest.raises(RP2RuntimeError) as excinfo:
        plugin.load(country=None)  # type: ignore

    assert "Multiple quotes for pair. Please open an issue at" in str(excinfo.value)


def test_kraken(
    mocker: Any,
    plugin: InputPlugin,
    private_post_ledgers_return: Dict[str, Any],
    private_post_tradeshistory_return: Dict[str, Any],
) -> None:
    """
    This tests withdraw, deposit, buy and a sell.
    """
    client: Exchange = plugin._client

    mocker.patch.object(client, "load_markets").return_value = None
    mocker.patch.object(
        client,
        "markets_by_id",
        {
            "XLTCZUSD": [{_ID: "XLTCZUSD", _BASE_ID: "XLTC", _BASE: "LTC", _QUOTE: "USD"}],
            "XLTCXXBT": [{_ID: "XLTCXXBT", _BASE_ID: "XLTC", _BASE: "LTC", _QUOTE: "BTC"}],
        },
    )

    mocker.patch.object(client, "private_post_ledgers").return_value = private_post_ledgers_return
    mocker.patch.object(client, "private_post_tradeshistory").return_value = private_post_tradeshistory_return
    actual_result = plugin.load(country=None)  # type: ignore

    assert len(actual_result) == 4

    expect_result = [
        IntraTransaction(
            plugin="kraken_REST",
            unique_id=Keyword.UNKNOWN.value,
            raw_data="{"
            "'aclass': 'currency', "
            "'amount': '3.0000000000', "
            "'asset': 'XLTC', "
            "'balance': '3.0000000000', "
            "'fee': '0.0000000000', "
            "'refid': 'somerefid0', "
            "'time': '1406104521.2614448', "
            "'type': 'deposit', "
            "'subtype': ''"
            "}",
            timestamp="2014-07-23 08:35:21+0000",
            asset="LTC",
            from_exchange=Keyword.UNKNOWN.value,
            from_holder=Keyword.UNKNOWN.value,
            to_exchange="kraken",
            to_holder="tester",
            spot_price="__unknown",
            crypto_sent=Keyword.UNKNOWN.value,
            crypto_received="3.0000000000",
            notes="deposit",
        ),
        IntraTransaction(
            plugin="kraken_REST",
            unique_id=Keyword.UNKNOWN.value,
            raw_data="{"
            "'aclass': 'currency', "
            "'amount': '9.4149333100', "
            "'asset': 'XLTC', "
            "'balance': '9.4149333100', "
            "'fee': '0.0000000000', "
            "'refid': 'somerefid1', "
            "'time': '1406812141.5408714', "
            "'type': 'withdrawal', "
            "'subtype': ''"
            "}",
            timestamp="2014-07-31 13:09:01+0000",
            asset="LTC",
            from_exchange="kraken",
            from_holder="tester",
            to_exchange=Keyword.UNKNOWN.value,
            to_holder=Keyword.UNKNOWN.value,
            spot_price="__unknown",
            crypto_sent="9.4149333100",
            crypto_received=Keyword.UNKNOWN.value,
            notes="withdrawal",
        ),
        InTransaction(
            plugin="kraken_REST",
            unique_id="__unknown",
            raw_data="{"
            "'aclass': 'currency', "
            "'amount': '8.0906135700', "
            "'asset': 'XLTC', "
            "'balance': '8.0906189200', "
            "'fee': '0.0000000000', "
            "'refid': 'somerefid2', "
            "'time': '1403860822.8546317', "
            "'type': 'trade', "
            "'subtype': ''"
            "}",
            timestamp="2014-06-27 09:20:22+0000",
            asset="LTC",
            exchange="kraken",
            holder="tester",
            transaction_type=Keyword.BUY.value,
            spot_price="43.80001",
            crypto_in="8.0906135700",
            fiat_fee=None,
            fiat_in_no_fee="499.20000",
            fiat_in_with_fee="500.00000",
            notes="buy_trade",
        ),
        OutTransaction(
            plugin="kraken_REST",
            unique_id=Keyword.UNKNOWN.value,
            raw_data="{"
            "'aclass': 'currency', "
            "'amount': '-3.0000000000', "
            "'asset': 'XLTC', "
            "'balance': '0.0000000000', "
            "'fee': '0.0000000000', "
            "'refid': 'somerefid3', "
            "'time': '1406122046.414707', "
            "'type': 'trade', "
            "'subtype': ''"
            "}",
            timestamp="2014-07-23 13:27:26+0000",
            asset="LTC",
            exchange="kraken",
            holder="tester",
            transaction_type=Keyword.SELL.value,
            spot_price="__unknown",
            crypto_out_no_fee="3.0000000000",
            crypto_fee="0.0000000000",
            crypto_out_with_fee="3.0000000000",
            fiat_out_no_fee="0.03392556",
            fiat_fee=None,
            notes="sell_trade",
        ),
    ]

    assert str(actual_result) == str(expect_result)
