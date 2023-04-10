# Copyright 2022 eprbell
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

from enum import Enum
from typing import Dict, Optional, Set, Union

from rp2.rp2_error import RP2ValueError


# Configuration file keywords
class Keyword(Enum):
    AIRDROP: str = "airdrop"
    ASSET: str = "asset"
    BUY: str = "buy"
    CRYPTO_FEE: str = "crypto_fee"
    CRYPTO_IN: str = "crypto_in"
    CRYPTO_OUT_NO_FEE: str = "crypto_out_no_fee"
    CRYPTO_OUT_WITH_FEE: str = "crypto_out_with_fee"
    CRYPTO_RECEIVED: str = "crypto_received"
    CRYPTO_SENT: str = "crypto_sent"
    DONATE: str = "donate"
    EXCHANGE: str = "exchange"
    FEE: str = "fee"
    FIAT_FEE: str = "fiat_fee"
    FIAT_IN_NO_FEE: str = "fiat_in_no_fee"
    FIAT_IN_WITH_FEE: str = "fiat_in_with_fee"
    FIAT_OUT_NO_FEE: str = "fiat_out_no_fee"
    FIAT_TICKER: str = "fiat_ticker"
    FROM_EXCHANGE: str = "from_exchange"
    FROM_HOLDER: str = "from_holder"
    GIFT: str = "gift"
    HARDFORK: str = "hardfork"
    HISTORICAL_MARKET_DATA: str = "historical_market_data"  # Deprecated
    HISTORICAL_PAIR_CONVERTERS: str = "historical_pair_converters"
    HISTORICAL_PRICE_CLOSE: str = "close"
    HISTORICAL_PRICE_HIGH: str = "high"
    HISTORICAL_PRICE_LOW: str = "low"
    HISTORICAL_PRICE_NEAREST: str = "nearest"
    HISTORICAL_PRICE_OPEN: str = "open"
    HOLDER: str = "holder"
    IN: str = "in"
    INCOME: str = "income"
    INTEREST: str = "interest"
    INTRA: str = "intra"
    IN_HEADER: str = "in_header"
    INTRA_HEADER: str = "intra_header"
    IS_SPOT_PRICE_FROM_WEB: str = "is_spot_price_from_web"
    MINING: str = "mining"
    MOVE: str = "move"
    NATIVE_FIAT: str = "native_fiat"
    NOTES: str = "notes"
    OUT: str = "out"
    OUT_HEADER: str = "out_header"
    PLUGIN: str = "plugin"
    RAW_DATA: str = "raw_data"
    SELL: str = "sell"
    SPOT_PRICE: str = "spot_price"
    STAKING: str = "staking"
    TIMESTAMP: str = "timestamp"
    TO_EXCHANGE: str = "to_exchange"
    TO_HOLDER: str = "to_holder"
    TRANSACTION_HINTS: str = "transaction_hints"
    TRANSACTION_TYPE: str = "transaction_type"
    UNIQUE_ID: str = "unique_id"
    UNKNOWN = "__unknown"
    WAGES: str = "wages"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in _keyword_values

    @classmethod
    def type_check_from_string(cls, keyword: str) -> "Keyword":
        if not Keyword.has_value(keyword.lower()):
            raise RP2ValueError(f"Invalid keyword: {keyword}")
        return Keyword[keyword.upper()]


_keyword_values: Set[str] = {item.value for item in Keyword}

# List of supported fiat currencies
_FIAT_SET: Set[str] = {"AUD", "CAD", "CHF", "CNY", "EUR", "GBP", "HKD", "ILS", "INR", "JPY", "KRW", "SEK", "USD"}

_FIAT_FIELD_SET: Set[str] = {
    Keyword.FIAT_FEE.value,
    Keyword.FIAT_IN_NO_FEE.value,
    Keyword.FIAT_IN_WITH_FEE.value,
    Keyword.FIAT_OUT_NO_FEE.value,
    Keyword.SPOT_PRICE.value,
}

_CRYPTO_FIELD_SET: Set[str] = {
    Keyword.CRYPTO_FEE.value,
    Keyword.CRYPTO_IN.value,
    Keyword.CRYPTO_OUT_NO_FEE.value,
    Keyword.CRYPTO_OUT_WITH_FEE.value,
    Keyword.CRYPTO_RECEIVED.value,
    Keyword.CRYPTO_SENT.value,
}

_INTERNAL_FIELD_SET: Set[str] = {
    Keyword.FIAT_TICKER.value,
    Keyword.IS_SPOT_PRICE_FROM_WEB.value,
    Keyword.PLUGIN.value,
    Keyword.RAW_DATA.value,
}

DIRECTION_SET: Set[str] = {
    Keyword.IN.value,
    Keyword.OUT.value,
    Keyword.INTRA.value,
}

DIRECTION_2_TRANSACTION_TYPE_SET: Dict[str, Set[str]] = {
    Keyword.IN.value: {
        Keyword.AIRDROP.value,
        Keyword.BUY.value,
        Keyword.DONATE.value,
        Keyword.GIFT.value,
        Keyword.HARDFORK.value,
        Keyword.INCOME.value,
        Keyword.INTEREST.value,
        Keyword.MINING.value,
        Keyword.STAKING.value,
        Keyword.WAGES.value,
    },
    Keyword.OUT.value: {
        Keyword.DONATE.value,
        Keyword.GIFT.value,
        Keyword.FEE.value,
        Keyword.SELL.value,
    },
    Keyword.INTRA.value: {
        Keyword.MOVE.value,
    },
}

HISTORICAL_PRICE_KEYWORD_SET: Set[str] = {
    Keyword.HISTORICAL_PRICE_CLOSE.value,
    Keyword.HISTORICAL_PRICE_HIGH.value,
    Keyword.HISTORICAL_PRICE_LOW.value,
    Keyword.HISTORICAL_PRICE_NEAREST.value,
    Keyword.HISTORICAL_PRICE_OPEN.value,
}

BUILTIN_CONFIGURATION_SECTIONS: Set[str] = {
    Keyword.TRANSACTION_HINTS.value,
    Keyword.IN_HEADER.value,
    Keyword.OUT_HEADER.value,
    Keyword.INTRA_HEADER.value,
}

DEFAULT_CONFIGURATION: Dict[str, Union[Dict[str, int], Dict[str, str]]] = {
    Keyword.IN_HEADER.value: {
        Keyword.TIMESTAMP.value: 0,
        Keyword.ASSET.value: 1,
        Keyword.EXCHANGE.value: 2,
        Keyword.HOLDER.value: 3,
        Keyword.TRANSACTION_TYPE.value: 4,
        Keyword.SPOT_PRICE.value: 6,
        Keyword.CRYPTO_IN.value: 7,
        Keyword.CRYPTO_FEE.value: 8,
        Keyword.FIAT_IN_NO_FEE.value: 9,
        Keyword.FIAT_IN_WITH_FEE.value: 10,
        Keyword.FIAT_FEE.value: 11,
        Keyword.UNIQUE_ID.value: 12,
        Keyword.NOTES.value: 13,
    },
    Keyword.OUT_HEADER.value: {
        Keyword.TIMESTAMP.value: 0,
        Keyword.ASSET.value: 1,
        Keyword.EXCHANGE.value: 2,
        Keyword.HOLDER.value: 3,
        Keyword.TRANSACTION_TYPE.value: 4,
        Keyword.SPOT_PRICE.value: 6,
        Keyword.CRYPTO_OUT_NO_FEE.value: 7,
        Keyword.CRYPTO_FEE.value: 8,
        Keyword.CRYPTO_OUT_WITH_FEE.value: 9,
        Keyword.FIAT_OUT_NO_FEE.value: 10,
        Keyword.FIAT_FEE.value: 11,
        Keyword.UNIQUE_ID.value: 12,
        Keyword.NOTES.value: 13,
    },
    Keyword.INTRA_HEADER.value: {
        Keyword.TIMESTAMP.value: 0,
        Keyword.ASSET.value: 1,
        Keyword.FROM_EXCHANGE.value: 2,
        Keyword.FROM_HOLDER.value: 3,
        Keyword.TO_EXCHANGE.value: 4,
        Keyword.TO_HOLDER.value: 5,
        Keyword.SPOT_PRICE.value: 6,
        Keyword.CRYPTO_SENT.value: 7,
        Keyword.CRYPTO_RECEIVED.value: 8,
        Keyword.UNIQUE_ID.value: 12,
        Keyword.NOTES.value: 13,
    },
}


def is_builtin_section_name(section_name: str) -> bool:
    return section_name in BUILTIN_CONFIGURATION_SECTIONS


def is_fiat_field(field: str) -> bool:
    return field in _FIAT_FIELD_SET


def is_crypto_field(field: str) -> bool:
    return field in _CRYPTO_FIELD_SET


def is_internal_field(field: str) -> bool:
    return field in _INTERNAL_FIELD_SET


def is_fiat(currency: str) -> bool:
    return currency in _FIAT_SET


def is_unknown(value: str) -> bool:
    return value == Keyword.UNKNOWN.value


def is_unknown_or_none(value: Optional[str]) -> bool:
    return value in {Keyword.UNKNOWN.value, None}


def is_transaction_type_valid(direction: str, transaction_type: str) -> bool:
    return transaction_type.lower() in DIRECTION_2_TRANSACTION_TYPE_SET[direction]
