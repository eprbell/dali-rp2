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

from datetime import datetime
from inspect import signature
from typing import Callable, Dict, List, NamedTuple, Optional, Union

from dateutil.parser import parse
from prezzemolo.utility import to_string
from rp2.rp2_error import RP2RuntimeError

from dali.configuration import Keyword, is_internal_field, is_unknown


class StringAndDatetime(NamedTuple):
    string: str
    value: datetime


class AssetAndUniqueId(NamedTuple):
    asset: str
    unique_id: str


class DirectionTypeAndNotes(NamedTuple):
    direction: str
    transaction_type: str
    notes: str


class AbstractTransaction:
    @classmethod
    def _validate_string_field(cls, name: str, value: str, raw_data: str, disallow_empty: bool, disallow_unknown: bool) -> str:
        if not isinstance(name, str):
            raise RP2RuntimeError(f"Internal error: parameter name is not a string: {name}")
        if not isinstance(value, str):
            raise RP2RuntimeError(f"Internal error: {name} is not a string: {value}\n{raw_data}")
        if disallow_empty and len(value) == 0:
            raise RP2RuntimeError(f"Internal error: {name} is empty: {raw_data}")
        if disallow_unknown and is_unknown(value):
            raise RP2RuntimeError(f"Internal error: {name} is unknown: {raw_data}")
        return value.strip()

    @classmethod
    def _validate_numeric_field(cls, name: str, value: str, raw_data: str, disallow_empty: bool, disallow_unknown: bool) -> str:
        value = cls._validate_string_field(name, value, raw_data, disallow_empty, disallow_unknown)
        if is_unknown(value):
            return value
        if not value:
            return value
        try:
            float(value)
        except ValueError as exc:
            raise RP2RuntimeError(f"Internal error parsing {name} as number: {value}\n{raw_data}\n{str(exc)}") from exc
        return value

    @classmethod
    def _validate_timestamp_field(cls, name: str, value: str, raw_data: str) -> StringAndDatetime:
        value = cls._validate_string_field(name, value, raw_data, disallow_empty=True, disallow_unknown=True)
        try:
            result: datetime = parse(value)
        except RP2RuntimeError as exc:
            raise RP2RuntimeError(f"Internal error parsing {name} as datetime: {value}\n{raw_data}\n{str(exc)}") from exc
        if result.tzinfo is None:
            raise RP2RuntimeError(f"Internal error: {name} has no timezone info: {value}\n{raw_data}")
        if result.microsecond == 0:
            return StringAndDatetime(result.strftime("%Y-%m-%d %H:%M:%S%z"), result)
        return StringAndDatetime(result.strftime("%Y-%m-%d %H:%M:%S.%f%z"), result)

    @classmethod
    def _validate_optional_string_field(cls, name: str, value: Optional[str], raw_data: str, disallow_empty: bool, disallow_unknown: bool) -> Optional[str]:
        if not value:
            return None
        return cls._validate_string_field(name, value, raw_data, disallow_empty, disallow_unknown)

    @classmethod
    def _validate_optional_numeric_field(cls, name: str, value: Optional[str], raw_data: str, disallow_empty: bool, disallow_unknown: bool) -> Optional[str]:
        if not value:
            return None
        return cls._validate_numeric_field(name, value, raw_data, disallow_empty, disallow_unknown)

    # Unique_id is used by transaction_resolver: it must contain hash for IntraTransactions or other account-specific id
    # that identifies the transaction uniquely. Some exchanges don't report hash information, so allow unknown (e.g. BlockFi CSV)
    def __init__(
        self,
        plugin: str,
        unique_id: str,
        raw_data: str,
        timestamp: str,
        asset: str,
        notes: Optional[str] = None,
        is_spot_price_from_web: Optional[bool] = None,
        fiat_ticker: Optional[str] = None,
    ) -> None:
        self.__plugin: str = self._validate_string_field(Keyword.PLUGIN.value, plugin, raw_data, disallow_empty=True, disallow_unknown=True)
        self.__unique_id: str = self._validate_string_field(Keyword.UNIQUE_ID.value, unique_id, raw_data, disallow_empty=True, disallow_unknown=False)
        if unique_id.startswith("0x"):
            self.__unique_id = unique_id[len("0x") :]
        self.__raw_data: str = self._validate_string_field(Keyword.RAW_DATA.value, raw_data, raw_data, disallow_empty=True, disallow_unknown=True)
        self.__timestamp: str
        self.__timestamp_value: datetime
        (self.__timestamp, self.__timestamp_value) = self._validate_timestamp_field(Keyword.TIMESTAMP.value, timestamp, raw_data)
        self.__asset: str = self._validate_string_field(Keyword.ASSET.value, asset, raw_data, disallow_empty=True, disallow_unknown=True)
        self.__notes: Optional[str] = self._validate_optional_string_field(Keyword.NOTES.value, notes, raw_data, disallow_empty=False, disallow_unknown=True)
        if is_spot_price_from_web and not isinstance(is_spot_price_from_web, bool):
            raise RP2RuntimeError(f"Internal error: {Keyword.IS_SPOT_PRICE_FROM_WEB.value} is not boolean: {is_spot_price_from_web}")
        self.__is_spot_price_from_web: bool = is_spot_price_from_web if is_spot_price_from_web else False
        self.__fiat_ticker: Optional[str] = self._validate_optional_string_field(
            "fiat_ticker", fiat_ticker, raw_data, disallow_empty=True, disallow_unknown=True
        )

    def to_string(self, indent: int = 0, repr_format: bool = True, extra_data: Optional[List[str]] = None) -> str:
        class_specific_data: List[str] = []
        stringify: Callable[[object], str] = repr
        if not repr_format:
            stringify = str

        if repr_format:
            class_specific_data.append(f"{type(self).__name__}({Keyword.PLUGIN.value}={repr(self.plugin)}")
        else:
            class_specific_data.append(f"{type(self).__name__}:")
            class_specific_data.append(f"{Keyword.PLUGIN.value}={str(self.plugin)}")

        class_specific_data.append(f"{Keyword.UNIQUE_ID.value}={stringify(self.unique_id)}")
        class_specific_data.append(f"{Keyword.RAW_DATA.value}={stringify(self.raw_data)}")
        class_specific_data.append(f"{Keyword.TIMESTAMP.value}={stringify(self.timestamp)}")
        class_specific_data.append(f"{Keyword.ASSET.value}={stringify(self.asset)}")

        if extra_data:
            class_specific_data.extend(extra_data)

        return to_string(indent=indent, repr_format=repr_format, data=class_specific_data)

    def __str__(self) -> str:
        return self.to_string(indent=0, repr_format=False)

    def __repr__(self) -> str:
        return self.to_string(indent=0, repr_format=True)

    def __eq__(self, other: object) -> bool:
        if not other:
            return False
        if not isinstance(other, AbstractTransaction):
            raise RP2RuntimeError(f"Internal error: operand has non-AbstractTransaction value {repr(other)}")
        result: bool = self.unique_id == other.unique_id and self.plugin == self.plugin and self.asset == self.asset
        return result

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((self.unique_id, self.plugin, self.asset))

    # Build a dictionary of constructor initialization parameters. Return true if any of them have UNKNOWN value
    def _setup_constructor_parameter_dictionary(self, parameter_dictionary: Dict[str, Union[str, bool, Optional[str], Optional[bool]]]) -> bool:
        result: bool = False
        for parameter in signature(self.__class__).parameters:
            value: str = getattr(self, parameter)
            parameter_dictionary[parameter] = value
            if is_internal_field(parameter) or parameter == Keyword.UNIQUE_ID.value:
                continue
            if is_unknown(value):
                result = True
        return result

    @property
    def plugin(self) -> str:
        return self.__plugin

    @property
    def unique_id(self) -> str:
        return self.__unique_id

    @property
    def raw_data(self) -> str:
        return self.__raw_data

    @property
    def timestamp(self) -> str:
        return self.__timestamp

    @property
    def timestamp_value(self) -> datetime:
        return self.__timestamp_value

    @property
    def asset(self) -> str:
        return self.__asset

    @property
    def notes(self) -> Optional[str]:
        return self.__notes

    @property
    def is_spot_price_from_web(self) -> bool:
        return self.__is_spot_price_from_web

    @property
    def fiat_ticker(self) -> Optional[str]:
        return self.__fiat_ticker

    @property
    def is_unresolved(self) -> bool:
        raise NotImplementedError("Abstract method: it must be implemented in subclasses")

    @property
    def constructor_parameter_dictionary(self) -> Dict[str, Union[str, bool, Optional[str], Optional[bool]]]:
        raise NotImplementedError("Abstract method: it must be implemented in subclasses")
