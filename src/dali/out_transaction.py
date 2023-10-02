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


class OutTransaction(AbstractTransaction):
    @classmethod
    def _validate_transaction_type_field(cls, name: str, value: str, raw_data: str) -> str:
        value = cls._validate_string_field(name, value, raw_data, disallow_empty=True, disallow_unknown=True)
        Keyword.type_check_from_string(value)
        if not is_transaction_type_valid(Keyword.OUT.value, value):
            raise RP2RuntimeError(f"Invalid transaction type {value} for {cls.__name__}")
        return value.capitalize()

    def __init__(
        self,
        plugin: str,
        unique_id: str,
        raw_data: str,
        timestamp: str,
        asset: str,
        exchange: str,
        holder: str,
        transaction_type: str,
        spot_price: str,
        crypto_out_no_fee: str,
        crypto_fee: str,
        crypto_out_with_fee: Optional[str] = None,
        fiat_out_no_fee: Optional[str] = None,
        fiat_fee: Optional[str] = None,
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

        self.__exchange: str = self._validate_string_field(Keyword.EXCHANGE.value, exchange, raw_data, disallow_empty=True, disallow_unknown=True)
        self.__holder: str = self._validate_string_field(Keyword.HOLDER.value, holder, raw_data, disallow_empty=True, disallow_unknown=True)
        self.__transaction_type: str = self._validate_transaction_type_field(Keyword.TRANSACTION_TYPE.value, transaction_type, raw_data)
        self.__spot_price: str = self._validate_numeric_field(Keyword.SPOT_PRICE.value, spot_price, raw_data, disallow_empty=True, disallow_unknown=False)
        self.__crypto_out_no_fee: str = self._validate_numeric_field(
            Keyword.CRYPTO_OUT_NO_FEE.value, crypto_out_no_fee, raw_data, disallow_empty=True, disallow_unknown=True
        )
        # unlike InTransactions, a fee is required here. More info: https://github.com/eprbell/dali-rp2/pull/75
        self.__crypto_fee: str = self._validate_numeric_field(Keyword.CRYPTO_FEE.value, crypto_fee, raw_data, disallow_empty=True, disallow_unknown=True)
        self.__crypto_out_with_fee: Optional[str] = self._validate_optional_numeric_field(
            Keyword.CRYPTO_OUT_WITH_FEE.value, crypto_out_with_fee, raw_data, disallow_empty=False, disallow_unknown=True
        )
        self.__fiat_out_no_fee: Optional[str] = self._validate_optional_numeric_field(
            Keyword.FIAT_OUT_NO_FEE.value, fiat_out_no_fee, raw_data, disallow_empty=False, disallow_unknown=True
        )
        self.__fiat_fee: Optional[str] = self._validate_optional_numeric_field(
            Keyword.FIAT_FEE.value, fiat_fee, raw_data, disallow_empty=False, disallow_unknown=True
        )
        self.__constructor_parameter_dictionary: Dict[str, Union[str, bool, Optional[str], Optional[bool]]] = {}
        self.__is_unresolved: bool = self._setup_constructor_parameter_dictionary(self.__constructor_parameter_dictionary)

    def to_string(self, indent: int = 0, repr_format: bool = True, extra_data: Optional[List[str]] = None) -> str:
        class_specific_data: List[str] = []
        stringify: Callable[[object], str] = repr
        if not repr_format:
            stringify = str
        class_specific_data = [
            f"{Keyword.EXCHANGE.value}={stringify(self.exchange)}",
            f"{Keyword.HOLDER.value}={stringify(self.holder)}",
            f"{Keyword.TRANSACTION_TYPE.value}={stringify(self.transaction_type)}",
            f"{Keyword.SPOT_PRICE.value}={self.spot_price}",
            f"{Keyword.CRYPTO_OUT_NO_FEE.value}={self.crypto_out_no_fee}",
            f"{Keyword.CRYPTO_FEE.value}={self.crypto_fee}",
            f"{Keyword.CRYPTO_OUT_WITH_FEE.value}={self.crypto_out_with_fee}",
            f"{Keyword.FIAT_OUT_NO_FEE.value}={self.fiat_out_no_fee}",
            f"{Keyword.FIAT_FEE.value}={self.fiat_fee}",
            f"{Keyword.NOTES.value}={self.notes}",
        ]
        if extra_data:
            class_specific_data.extend(extra_data)

        return super().to_string(indent=indent, repr_format=repr_format, extra_data=class_specific_data)

    @property
    def exchange(self) -> str:
        return self.__exchange

    @property
    def holder(self) -> str:
        return self.__holder

    @property
    def transaction_type(self) -> str:
        return self.__transaction_type

    @property
    def spot_price(self) -> str:
        return self.__spot_price

    @property
    def crypto_out_no_fee(self) -> str:
        return self.__crypto_out_no_fee

    @property
    def crypto_fee(self) -> str:
        return self.__crypto_fee

    @property
    def crypto_out_with_fee(self) -> Optional[str]:
        return self.__crypto_out_with_fee

    @property
    def fiat_out_no_fee(self) -> Optional[str]:
        return self.__fiat_out_no_fee

    @property
    def fiat_fee(self) -> Optional[str]:
        return self.__fiat_fee

    @property
    def is_unresolved(self) -> bool:
        return self.__is_unresolved

    @property
    def constructor_parameter_dictionary(self) -> Dict[str, Union[str, bool, Optional[str], Optional[bool]]]:
        return self.__constructor_parameter_dictionary
