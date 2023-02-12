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

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type

import ezodf
from rp2.rp2_error import RP2RuntimeError, RP2TypeError

from dali.abstract_transaction import AbstractTransaction
from dali.configuration import (
    Keyword,
    is_crypto_field,
    is_fiat_field,
    is_internal_field,
)
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.logger import LOGGER
from dali.out_transaction import OutTransaction

_MIN_ROWS: int = 40
_MAX_COLUMNS: int = 20
_TABLE_END: str = "TABLE END"

_IN: str = "IN"
_OUT: str = "OUT"
_INTRA: str = "INTRA"

_TABLE_ORDER: Dict[str, int] = {
    _IN: 1,
    _OUT: 2,
    _INTRA: 3,
}

_TABLE_TO_SECTION_NAME: Dict[str, Keyword] = {
    _IN: Keyword.IN_HEADER,
    _OUT: Keyword.OUT_HEADER,
    _INTRA: Keyword.INTRA_HEADER,
}

_TRANSACTION_CLASS_TO_SECTION_NAME: Dict[Type[AbstractTransaction], Keyword] = {
    InTransaction: Keyword.IN_HEADER,
    OutTransaction: Keyword.OUT_HEADER,
    IntraTransaction: Keyword.INTRA_HEADER,
}

_TRANSACTION_CLASS_TO_TABLE: Dict[Type[AbstractTransaction], str] = {
    InTransaction: _IN,
    OutTransaction: _OUT,
    IntraTransaction: _INTRA,
}


def _transaction_sort_key(entry: AbstractTransaction) -> Tuple[str, int, datetime]:
    return (entry.asset, _TABLE_ORDER[_TRANSACTION_CLASS_TO_TABLE[entry.__class__]], entry.timestamp_value)


def generate_input_file(
    output_dir_path: str,
    output_file_prefix: str,
    output_file_name: str,
    transactions: List[AbstractTransaction],
    global_configuration: Dict[str, Any],
) -> Any:

    native_fiat: str = global_configuration[Keyword.NATIVE_FIAT.value]

    _table_to_header: Dict[str, Dict[str, str]] = {
        _IN: {
            Keyword.UNIQUE_ID.value: "Unique ID",
            Keyword.TIMESTAMP.value: "Timestamp",
            Keyword.ASSET.value: "Asset",
            Keyword.EXCHANGE.value: "Exchange",
            Keyword.HOLDER.value: "Holder",
            Keyword.TRANSACTION_TYPE.value: "Transaction Type",
            Keyword.SPOT_PRICE.value: "Spot Price",
            Keyword.CRYPTO_IN.value: "Crypto In",
            Keyword.CRYPTO_FEE.value: "Crypto Fee",
            Keyword.FIAT_IN_NO_FEE.value: f"{native_fiat} In No Fee",
            Keyword.FIAT_IN_WITH_FEE.value: f"{native_fiat} In With Fee",
            Keyword.FIAT_FEE.value: f"{native_fiat} Fee",
            Keyword.NOTES.value: "Notes",
        },
        _OUT: {
            Keyword.UNIQUE_ID.value: "Unique ID",
            Keyword.TIMESTAMP.value: "Timestamp",
            Keyword.ASSET.value: "Asset",
            Keyword.EXCHANGE.value: "Exchange",
            Keyword.HOLDER.value: "Holder",
            Keyword.TRANSACTION_TYPE.value: "Transaction Type",
            Keyword.SPOT_PRICE.value: "Spot Price",
            Keyword.CRYPTO_OUT_NO_FEE.value: "Crypto Out No Fee",
            Keyword.CRYPTO_FEE.value: "Crypto Fee",
            Keyword.CRYPTO_OUT_WITH_FEE.value: "Crypto Out With Fee",
            Keyword.FIAT_OUT_NO_FEE.value: f"{native_fiat} Out No Fee",
            Keyword.FIAT_FEE.value: f"{native_fiat} Fee",
            Keyword.NOTES.value: "Notes",
        },
        _INTRA: {
            Keyword.UNIQUE_ID.value: "Unique ID",
            Keyword.TIMESTAMP.value: "Timestamp",
            Keyword.ASSET.value: "Asset",
            Keyword.FROM_EXCHANGE.value: "From Exchange",
            Keyword.FROM_HOLDER.value: "From Holder",
            Keyword.TO_EXCHANGE.value: "To Exchange",
            Keyword.TO_HOLDER.value: "To Holder",
            Keyword.SPOT_PRICE.value: "Spot Price",
            Keyword.CRYPTO_SENT.value: "Crypto Sent",
            Keyword.CRYPTO_RECEIVED.value: "Crypto Received",
            Keyword.NOTES.value: "Notes",
        },
    }

    if not isinstance(output_dir_path, str):
        raise RP2TypeError(f"Parameter output_dir_path is not of type string: {repr(output_dir_path)}")
    if not isinstance(output_file_prefix, str):
        raise RP2TypeError(f"Parameter output_file_prefix is not of type string: {repr(output_file_prefix)}")
    if not isinstance(output_file_name, str):
        raise RP2TypeError(f"Parameter output_file_name is not of type string: {repr(output_file_name)}")
    if not isinstance(transactions, List):
        raise RP2TypeError(f"Parameter transactions is not of type List: {repr(transactions)}")

    output_file_path: Path = Path(output_dir_path) / Path(f"{output_file_prefix}{output_file_name}")
    if Path(output_file_path).exists():
        output_file_path.unlink()

    template_path: str = str(Path(os.path.dirname(__file__)).absolute() / Path("data/template.ods"))
    output_file: Any = ezodf.newdoc("ods", str(output_file_path), template=template_path)

    index: int = 0
    sheet_name: str
    sheet_indexes_to_remove: List[int] = []
    for sheet_name in output_file.sheets.names():
        if sheet_name.startswith("__"):
            # Template sheet we don't want to keep: mark it for removal
            sheet_indexes_to_remove.append(index)
        index += 1

    # Remove sheets that were marked for removal
    for index in reversed(sheet_indexes_to_remove):
        del output_file.sheets[index]

    transactions.sort(key=_transaction_sort_key)

    current_asset: Optional[str] = None
    current_sheet: Optional[Any] = None
    current_table: Optional[str] = None
    row_index: int = 0
    for transaction in transactions:
        if not isinstance(transaction, AbstractTransaction):
            raise RP2RuntimeError(f"Internal error: Parameter 'transaction' is not a subclass of AbstractTransaction. {transaction}")
        if transaction.asset == global_configuration[Keyword.NATIVE_FIAT.value]:
            continue
        table_type: str = _TRANSACTION_CLASS_TO_TABLE[transaction.__class__]
        if transaction.asset != current_asset:
            if current_sheet:
                _fill_cell(current_sheet, row_index, 0, _TABLE_END, visual_style="bold")
            current_asset = transaction.asset
            current_sheet = ezodf.Table(current_asset)
            current_sheet.reset(size=(len(transactions) + _MIN_ROWS, _MAX_COLUMNS))
            output_file.sheets += current_sheet
            row_index = 0
            current_table = None
        if table_type != current_table:
            if current_table is not None:
                _fill_cell(current_sheet, row_index, 0, _TABLE_END, visual_style="bold")
                row_index += 2
            current_table = table_type
            _fill_cell(current_sheet, row_index, 0, current_table, visual_style="bold")
            _fill_header_row(current_sheet, current_table, row_index + 1, _table_to_header[current_table], global_configuration)
            row_index += 2

        _fill_transaction_row(current_sheet, row_index, transaction, global_configuration)
        row_index += 1
    _fill_cell(current_sheet, row_index, 0, _TABLE_END, visual_style="bold")

    output_file.save()
    LOGGER.info("Generated output: %s", Path(output_file.docname).resolve())


def _fill_header_row(
    sheet: Any,
    current_table: str,
    row_index: int,
    header_2_name: Dict[str, str],
    global_configuration: Dict[str, Any],
) -> None:
    count: int = 0
    current_section_name: str = _TABLE_TO_SECTION_NAME[current_table].value

    for count in range(0, _MAX_COLUMNS):
        _fill_cell(sheet, row_index, count, "", visual_style="header", data_style="default")

    count = 0
    for header, name in header_2_name.items():
        column_index: int = count
        if current_section_name in global_configuration:
            if header in global_configuration[current_section_name]:
                column_index = global_configuration[current_section_name][header]
            else:
                continue
        _fill_cell(sheet, row_index, column_index, name, visual_style="header", data_style="default")
        count += 1


def _fill_transaction_row(
    sheet: Any,
    row_index: int,
    transaction: AbstractTransaction,
    global_configuration: Dict[str, Any],
) -> None:
    if not isinstance(transaction, AbstractTransaction):
        raise RP2TypeError(f"Parameter 'transaction' is not of type AbstractTransaction: {transaction}")
    count: int = 0
    current_section_name: str = _TRANSACTION_CLASS_TO_SECTION_NAME[transaction.__class__].value
    for parameter, value in transaction.constructor_parameter_dictionary.items():
        if is_internal_field(parameter):
            continue
        column_index: int = count
        if current_section_name in global_configuration:
            column_index = global_configuration[current_section_name][parameter]
        data_style: str = "default"
        if is_fiat_field(parameter):
            data_style = "fiat"
        elif is_crypto_field(parameter):
            data_style = "crypto"
        _fill_cell(sheet, row_index, column_index, value, data_style=data_style)
        count += 1


def _apply_style_to_cell(sheet: Any, row_index: int, column_index: int, style_name: str) -> None:
    sheet[row_index, column_index].style_name = style_name


def _fill_cell(
    sheet: Any,
    row_index: int,
    column_index: int,
    value: Any,
    visual_style: str = "transparent",
    data_style: str = "default",
) -> None:

    if value is None:
        return

    is_formula: bool = False
    if isinstance(value, str) and value and value[0] == "=":
        # If the value starts with '=' it is assumed to be a formula
        is_formula = True

    style_name: str = f"{visual_style}_{data_style}"
    try:
        # The ezodf API doesn't accept RP2Decimal, so we are forced to cast to float before writing to the spreadsheet
        value = float(value)
    except ValueError:
        pass
    if is_formula:
        sheet[row_index, column_index].formula = value
    else:
        sheet[row_index, column_index].set_value(value)

    _apply_style_to_cell(sheet=sheet, row_index=row_index, column_index=column_index, style_name=style_name)
