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

from configparser import ConfigParser
from pathlib import Path
from typing import Any, Dict, List, Set

from rp2.rp2_error import RP2RuntimeError

from dali.abstract_transaction import AbstractTransaction
from dali.configuration import Keyword
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.out_transaction import OutTransaction

_ASSETS: str = "assets"
_EXCHANGES: str = "exchanges"
_HOLDERS: str = "holders"


def generate_configuration_file(
    output_dir_path: str,
    output_file_prefix: str,
    output_file_name: str,
    transactions: List[AbstractTransaction],
    global_configuration: Dict[str, Any],
) -> Any:

    if not isinstance(output_dir_path, str):
        raise RP2RuntimeError(f"Internal error: parameter output_dir_path is not of type string: {repr(output_dir_path)}")
    if not isinstance(output_file_prefix, str):
        raise RP2RuntimeError(f"Internal error: parameter output_file_prefix is not of type string: {repr(output_file_prefix)}")
    if not isinstance(output_file_name, str):
        raise RP2RuntimeError(f"Internal error: parameter output_file_name is not of type string: {repr(output_file_name)}")
    if not isinstance(transactions, List):
        raise RP2RuntimeError(f"Internal error: parameter transactions is not of type List: {repr(transactions)}")

    output_file_path: Path = Path(output_dir_path) / Path(f"{output_file_prefix}{output_file_name}")
    if Path(output_file_path).exists():
        output_file_path.unlink()

    ini_object = ConfigParser()

    assets: Set[str] = set()
    holders: Set[str] = set()
    exchanges: Set[str] = set()
    for transaction in transactions:
        if transaction.asset == global_configuration[Keyword.NATIVE_FIAT.value]:
            continue
        if isinstance(transaction, InTransaction):
            holders.add(transaction.holder)
            exchanges.add(transaction.exchange)
        elif isinstance(transaction, OutTransaction):
            holders.add(transaction.holder)
            exchanges.add(transaction.exchange)
        elif isinstance(transaction, IntraTransaction):
            holders.add(transaction.from_holder)
            holders.add(transaction.to_holder)
            exchanges.add(transaction.from_exchange)
            exchanges.add(transaction.to_exchange)
        else:
            raise RP2RuntimeError(f"Internal error: transaction is not a subclass of AbstractTransaction: {transaction}")
        assets.add(transaction.asset)

    if Keyword.UNKNOWN.value in assets:
        assets.remove(Keyword.UNKNOWN.value)
    if Keyword.UNKNOWN.value in holders:
        holders.remove(Keyword.UNKNOWN.value)
    if Keyword.UNKNOWN.value in exchanges:
        exchanges.remove(Keyword.UNKNOWN.value)

    ini_object["general"] = {
        _ASSETS: ", ".join(assets),
        _HOLDERS: ", ".join(holders),
        _EXCHANGES: ", ".join(exchanges),
    }

    for section_name in [Keyword.IN_HEADER, Keyword.OUT_HEADER, Keyword.INTRA_HEADER]:
        ini_object[section_name.value] = global_configuration[section_name.value]

    with open(str(output_file_path), "w", encoding="utf-8") as output_file:
        ini_object.write(output_file)
