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

from typing import Callable, Dict, List, Optional, Union

from rp2.rp2_error import RP2RuntimeError

from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword, is_transaction_type_valid


class IntraTransaction(AbstractTransaction):
    @classmethod
    def _validate_transaction_type_field(cls, name: str, value: str, raw_data: str) -> str:
        value = cls._validate_string_field(name, value, raw_data, disallow_empty=True, disallow_unknown=True)
        Keyword.type_check_from_string(value)
        if not is_transaction_type_valid(Keyword.INTRA.value, value):
            raise RP2RuntimeError(f"Invalid transaction type {value} for {cls.__name__}")
        return value.capitalize()

    def __init__(
        self,
        plugin: str,
        unique_id: str,
        raw_data: str,
        timestamp: str,
        asset: str,
        from_exchange: str,
        from_holder: str,
        to_exchange: str,
        to_holder: str,
        spot_price: Optional[str],
        crypto_sent: str,
        crypto_received: str,
        notes: Optional[str] = None,
        is_spot_price_from_web: Optional[bool] = None,
        fiat_ticker: Optional[str] = None,
    ) -> None:
        super().__init__(
            plugin=plugin,
            unique_id=unique_id,
            raw_data=raw_data,
            timestamp=timestamp,
            asset=asset,
            notes=notes,
            is_spot_price_from_web=is_spot_price_from_web,
            fiat_ticker=fiat_ticker,
        )

        self.__from_exchange: str = self._validate_string_field(
            Keyword.FROM_EXCHANGE.value, from_exchange, raw_data, disallow_empty=True, disallow_unknown=False
        )
        self.__from_holder: str = self._validate_string_field(Keyword.FROM_HOLDER.value, from_holder, raw_data, disallow_empty=True, disallow_unknown=False)
        self.__to_exchange: str = self._validate_string_field(Keyword.TO_EXCHANGE.value, to_exchange, raw_data, disallow_empty=True, disallow_unknown=False)
        self.__to_holder: str = self._validate_string_field(Keyword.TO_HOLDER.value, to_holder, raw_data, disallow_empty=True, disallow_unknown=False)
        self.__spot_price: Optional[str] = self._validate_optional_numeric_field(
            Keyword.SPOT_PRICE.value, spot_price, raw_data, disallow_empty=False, disallow_unknown=False
        )
        self.__crypto_sent: str = self._validate_numeric_field(Keyword.CRYPTO_SENT.value, crypto_sent, raw_data, disallow_empty=True, disallow_unknown=False)
        self.__crypto_received: str = self._validate_numeric_field(
            Keyword.CRYPTO_RECEIVED.value, crypto_received, raw_data, disallow_empty=True, disallow_unknown=False
        )
        self.__constructor_parameter_dictionary: Dict[str, Union[str, bool, Optional[str], Optional[bool]]] = {}
        self.__is_unresolved: bool = self._setup_constructor_parameter_dictionary(self.__constructor_parameter_dictionary)

    def to_string(self, indent: int = 0, repr_format: bool = True, extra_data: Optional[List[str]] = None) -> str:

        class_specific_data: List[str] = []
        stringify: Callable[[object], str] = repr
        if not repr_format:
            stringify = str
        class_specific_data = [
            f"{Keyword.FROM_EXCHANGE.value}={stringify(self.from_exchange)}",
            f"{Keyword.FROM_HOLDER.value}={stringify(self.from_holder)}",
            f"{Keyword.TO_EXCHANGE.value}={stringify(self.to_exchange)}",
            f"{Keyword.TO_HOLDER.value}={stringify(self.to_holder)}",
            f"{Keyword.SPOT_PRICE.value}={self.spot_price}",
            f"{Keyword.CRYPTO_SENT.value}={self.crypto_sent}",
            f"{Keyword.CRYPTO_RECEIVED.value}={self.crypto_received}",
            f"{Keyword.NOTES.value}={self.notes}",
        ]
        if extra_data:
            class_specific_data.extend(extra_data)

        return super().to_string(indent=indent, repr_format=repr_format, extra_data=class_specific_data)

    @property
    def from_exchange(self) -> str:
        return self.__from_exchange

    @property
    def from_holder(self) -> str:
        return self.__from_holder

    @property
    def to_exchange(self) -> str:
        return self.__to_exchange

    @property
    def to_holder(self) -> str:
        return self.__to_holder

    @property
    def spot_price(self) -> Optional[str]:
        return self.__spot_price

    @property
    def crypto_sent(self) -> str:
        return self.__crypto_sent

    @property
    def crypto_received(self) -> str:
        return self.__crypto_received

    @property
    def is_unresolved(self) -> bool:
        return self.__is_unresolved

    @property
    def constructor_parameter_dictionary(self) -> Dict[str, Union[str, bool, Optional[str], Optional[bool]]]:
        return self.__constructor_parameter_dictionary
