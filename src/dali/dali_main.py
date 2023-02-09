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

import cProfile
import os
import sys
from argparse import ArgumentParser, Namespace, RawTextHelpFormatter
from configparser import ConfigParser
from importlib import import_module
from inspect import Signature, signature
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Set, Type, Union

from rp2.abstract_country import AbstractCountry
from rp2.logger import LOG_FILE

from dali.abstract_input_plugin import AbstractInputPlugin
from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin
from dali.abstract_transaction import AbstractTransaction, DirectionTypeAndNotes
from dali.configuration import (
    DEFAULT_CONFIGURATION,
    DIRECTION_2_TRANSACTION_TYPE_SET,
    DIRECTION_SET,
    Keyword,
    is_builtin_section_name,
    is_internal_field,
    is_transaction_type_valid,
    is_unknown,
)
from dali.configuration_generator import generate_configuration_file
from dali.in_transaction import InTransaction
from dali.intra_transaction import IntraTransaction
from dali.logger import LOGGER
from dali.ods_generator import generate_input_file
from dali.out_transaction import OutTransaction
from dali.plugin.pair_converter.ccxt import (
    PairConverterPlugin as CcxtPairConverterPlugin,
)
from dali.plugin.pair_converter.historic_crypto import (
    PairConverterPlugin as HistoricCryptoPairConverterPlugin,
)
from dali.transaction_resolver import resolve_transactions

_VERSION: str = "0.6.3"


class _InputPluginHelperArgs(NamedTuple):
    input_plugin: AbstractInputPlugin
    package_name: str
    use_cache: bool


def dali_main(country: AbstractCountry) -> None:
    if "RP2_ENABLE_PROFILER" in os.environ:
        cProfile.runctx("_dali_main_internal(country)", globals(), locals())
    else:
        _dali_main_internal(country)


def _dali_main_internal(country: AbstractCountry) -> None:

    args: Namespace
    parser: ArgumentParser

    dali_configuration: Dict[str, Any] = DEFAULT_CONFIGURATION

    parser = _setup_argument_parser()
    args = parser.parse_args()

    _setup_paths(parser=parser, output_dir=args.output_dir)

    if not Path(args.ini_file).exists():
        print(f"Configuration file '{args.ini_file}' not found")
        parser.print_help()
        sys.exit(1)

    try:
        LOGGER.info("Country: %s", country.country_iso_code)

        ini_config: ConfigParser = ConfigParser()
        ini_config.read(args.ini_file)

        pair_converter_list: List[AbstractPairConverterPlugin] = []
        input_plugin_args_list: List[_InputPluginHelperArgs] = []
        section_name: str
        for section_name in ini_config.sections():
            # Plugin sections can have extra trailing words to make them unique: this way there can be multiple sections
            # with the same plugin. Builtin sections cannot have extra trailing words. This split() call extracts the section
            # name and removes any trailing words.
            normalized_section_name: str = section_name.split(" ", 1)[0]
            if is_builtin_section_name(normalized_section_name):
                if section_name != normalized_section_name:
                    LOGGER.error("Builtin section '%s' cannot have extra trailing keywords: '%s'", normalized_section_name, section_name)
                    sys.exit(1)
                if section_name == Keyword.TRANSACTION_HINTS.value:
                    dali_configuration[section_name] = _validate_transaction_hints_configuration(ini_config, section_name)
                elif section_name == Keyword.HISTORICAL_MARKET_DATA.value:
                    LOGGER.error(
                        "Builtin section '%s' is deprecated: use pair converter plugins instead (see configuration file documentation)", normalized_section_name
                    )
                    sys.exit(1)
                else:
                    dali_configuration[section_name] = _validate_header_configuration(ini_config, section_name)
                continue

            dali_configuration[Keyword.NATIVE_FIAT.value] = country.currency_iso_code.upper()

            # Plugin section
            plugin_module = import_module(normalized_section_name)
            plugin_configuration: Dict[str, Union[str, int, float, bool]]
            if hasattr(plugin_module, "PairConverterPlugin"):
                plugin_configuration = _validate_plugin_configuration(ini_config, section_name, signature(plugin_module.PairConverterPlugin))
                pair_converter_plugin: AbstractPairConverterPlugin = plugin_module.PairConverterPlugin(**plugin_configuration)
                LOGGER.info("Initialized pair converter plugin '%s'", section_name)
                LOGGER.debug("PairConverterPlugin object: '%s'", pair_converter_plugin)
                if (
                    not hasattr(pair_converter_plugin, "name")
                    or not hasattr(pair_converter_plugin, "cache_key")
                    or not hasattr(pair_converter_plugin, "get_historic_bar_from_native_source")
                ):
                    LOGGER.error("Plugin '%s' doesn't have all required methods. Exiting...", normalized_section_name)
                    sys.exit(1)
                pair_converter_list.append(pair_converter_plugin)

            elif hasattr(plugin_module, "InputPlugin"):
                plugin_configuration = _validate_plugin_configuration(ini_config, section_name, signature(plugin_module.InputPlugin))
                if Keyword.NATIVE_FIAT.value not in plugin_configuration:
                    LOGGER.error("No '%s' parameter in plugin '%s' constructor", Keyword.NATIVE_FIAT.value, normalized_section_name)
                    sys.exit(1)
                plugin_configuration[Keyword.NATIVE_FIAT.value] = dali_configuration[Keyword.NATIVE_FIAT.value]
                input_plugin: AbstractInputPlugin = plugin_module.InputPlugin(**plugin_configuration)
                LOGGER.debug("InputPlugin object: '%s'", input_plugin)
                if not hasattr(input_plugin, "load"):
                    LOGGER.error("Plugin '%s' has no 'load' method. Exiting...", normalized_section_name)
                    sys.exit(1)
                LOGGER.info("Initialized input plugin '%s'", section_name)
                input_plugin_args_list.append(_InputPluginHelperArgs(input_plugin, section_name, args.use_cache))

        if not input_plugin_args_list:
            LOGGER.error("No input plugin configuration found in config file. Exiting...")
            sys.exit(1)

        if not pair_converter_list:
            pair_converter_list.append(HistoricCryptoPairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value))
            pair_converter_list.append(CcxtPairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value))
            LOGGER.info("No pair converter plugins found in configuration file: using default pair converters.")

        dali_configuration[Keyword.HISTORICAL_PAIR_CONVERTERS.value] = pair_converter_list

        with ThreadPool(args.thread_count) as pool:
            result_list = pool.map(_input_plugin_helper, input_plugin_args_list)

        transactions: List[AbstractTransaction] = [transaction for result in result_list for transaction in result]

        LOGGER.info("Resolving transactions")
        resolved_transactions: List[AbstractTransaction] = resolve_transactions(transactions, dali_configuration, args.read_spot_price_from_web)

        LOGGER.info("Generating config file in %s", args.output_dir)
        generate_configuration_file(args.output_dir, args.prefix, "crypto_data.ini", resolved_transactions, dali_configuration)

        LOGGER.info("Generating input file in %s", args.output_dir)
        generate_input_file(args.output_dir, args.prefix, "crypto_data.ods", resolved_transactions, dali_configuration)

    except Exception:  # pylint: disable=broad-except
        LOGGER.exception("Fatal exception occurred:")

    LOGGER.info("Log file: %s", LOG_FILE)
    LOGGER.info("Generated output directory: %s", args.output_dir)
    LOGGER.info("Done")


def _input_plugin_helper(args: _InputPluginHelperArgs) -> List[AbstractTransaction]:
    input_plugin: AbstractInputPlugin = args.input_plugin
    package_name: str = args.package_name
    use_cache: bool = args.use_cache
    plugin_transactions: List[AbstractTransaction]
    if use_cache and input_plugin.cache_key() is not None:
        cache = input_plugin.load_from_cache()
        if cache:
            LOGGER.info("Reading crypto data for plugin '%s' from cache", package_name)
            plugin_transactions = cache
        else:
            LOGGER.info("Reading crypto data using plugin '%s'", package_name)
            plugin_transactions = input_plugin.load()
            input_plugin.save_to_cache(plugin_transactions)
    else:
        LOGGER.info("Reading crypto data using plugin '%s'", package_name)
        plugin_transactions = input_plugin.load()

    for transaction in plugin_transactions:
        if not isinstance(transaction, AbstractTransaction):
            LOGGER.error("Plugin '%s' returned a non-transaction object: %s. Exiting...", package_name, str(transaction))  # type: ignore
            sys.exit(1)
    return plugin_transactions


def _setup_argument_parser() -> ArgumentParser:
    parser: ArgumentParser = ArgumentParser(
        description=(
            "Generate RP2 input and configuration files. Links:\n"
            "- documentation: https://github.com/eprbell/dali-rp2/blob/main/README.md\n"
            "- FAQ: https://github.com/eprbell/dali-rp2/blob/main/docs/user_faq.md\n"
            "- support DaLI by leaving a star on Github: https://github.com/eprbell/dali-rp2"
        ),
        formatter_class=RawTextHelpFormatter,
    )

    parser.add_argument(
        "-c",
        "--use-cache",
        action="store_true",
        help="Cache input plugin data load (developers only)",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        action="store",
        default="output/",
        help="Write RP2 input and configuration files to OUTPUT_DIR",
        metavar="OUTPUT_DIR",
        type=str,
    )
    parser.add_argument(
        "-p",
        "--prefix",
        action="store",
        default="",
        help="Prepend RP2 input and configuration file names with PREFIX",
        metavar="PREFIX",
        type=str,
    )
    parser.add_argument(
        "-s",
        "--read-spot-price-from-web",
        action="store_true",
        help="Read spot price from the Internet, for transactions where it's missing",
    )
    parser.add_argument(
        "-t",
        "--thread-count",
        action="store",
        default=1,
        help="Number of concurrent threads for data loaders",
        metavar="THREAD_COUNT",
        type=int,
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"DaLI {_VERSION} (https://github.com/eprbell/dali_rp2)",
        help="Print DaLI version",
    )
    parser.add_argument(
        "ini_file",
        action="store",
        help="INI file",
        metavar="INI_FILE",
        type=str,
    )

    return parser


def _setup_paths(parser: ArgumentParser, output_dir: str) -> None:
    output_dir_path: Path = Path(output_dir)
    if not output_dir_path.exists():
        output_dir_path.mkdir(parents=True)
    if not output_dir_path.is_dir():
        print(f"output_dir '{output_dir}' exists but it's not a directory")
        parser.print_help()
        sys.exit(1)


# Sanity check of transaction_hints section. Ensure line format conforms to:
# <unique_id> = <direction>:<transaction_type>:<notes>
# where:
# - <unique_id> is the hash or exchange-specific id of a transaction
# - <direction> is IN/OUT,INTRA
# - <transaction_type> is a RP2 transaction type (see documentation) and
# - <notes> is an optional English sentence
def _validate_transaction_hints_configuration(ini_config: ConfigParser, section_name: str) -> Dict[str, DirectionTypeAndNotes]:
    result: Dict[str, DirectionTypeAndNotes] = {}

    if section_name != Keyword.TRANSACTION_HINTS.value:
        LOGGER.error("Invalid section name: %s", section_name)
        sys.exit(1)

    for unique_id, transaction_hint in ini_config[section_name].items():
        tokenized_transaction_hint: List[str]
        direction: str
        transaction_type: str
        notes: str
        if not unique_id or is_unknown(unique_id):
            LOGGER.error("Invalid unique id value in %s = %s", unique_id, transaction_hint)
            sys.exit(1)
        if not transaction_hint or is_unknown(transaction_hint):
            LOGGER.error("Invalid transaction_hint value in %s = %s", unique_id, transaction_hint)
            sys.exit(1)

        tokenized_transaction_hint = transaction_hint.split(":", 2)
        if len(tokenized_transaction_hint) != 3:
            LOGGER.error("Invalid transaction_hint format (expected <direction>:<transaction_type>:<notes>): %s = %s", unique_id, transaction_hint)
            sys.exit(1)

        # Check direction
        direction = tokenized_transaction_hint[0].strip().lower()
        if direction not in DIRECTION_SET:
            LOGGER.error("Direction substring in transaction_hint must be one of '%s', instead it was '%s'", str(DIRECTION_SET), direction)
            sys.exit(1)

        # Check transaction_type
        transaction_type = tokenized_transaction_hint[1].strip().lower()
        if not is_transaction_type_valid(direction, transaction_type):
            LOGGER.error(
                "Transaction type substring for '%s' direction in transaction_hint must be one of '%s', instead it was '%s'",
                direction,
                str(DIRECTION_2_TRANSACTION_TYPE_SET[direction]),
                transaction_type,
            )
            sys.exit(1)

        notes = tokenized_transaction_hint[2].strip()

        result[unique_id] = DirectionTypeAndNotes(direction, transaction_type, notes)
    return result


# Check that header section parameters map 1-1 against respective transaction constructor signature and build an parameter-to-column dictionary
def _validate_header_configuration(ini_config: ConfigParser, section_name: str) -> Dict[str, int]:
    transaction_class: Union[Type[InTransaction], Type[OutTransaction], Type[IntraTransaction]]
    result: Dict[str, int] = {}

    if section_name == Keyword.IN_HEADER.value:
        transaction_class = InTransaction
    elif section_name == Keyword.OUT_HEADER.value:
        transaction_class = OutTransaction
    elif section_name == Keyword.INTRA_HEADER.value:
        transaction_class = IntraTransaction
    else:
        LOGGER.error("Invalid section name: %s", section_name)
        sys.exit(1)

    constructor_parameter_set: Set[str] = set()
    section_parameter_set: Set[str] = set()

    for parameter in signature(transaction_class).parameters:
        if is_internal_field(parameter):
            continue
        constructor_parameter_set.add(parameter)

    column_to_header: Dict[int, str] = {}
    for header, column in ini_config[section_name].items():
        try:
            column_value: int = int(column)
        except ValueError:
            LOGGER.error("In section %s, %s has non-integer value %s", section_name, header, column)
            sys.exit(1)
        result[header] = ini_config.getint(section_name, header)
        section_parameter_set.add(header)
        if column_value in column_to_header:
            LOGGER.error("In section %s both %s and %s have the same value %s", section_name, column_to_header[column_value], header, column_value)
            sys.exit(1)
        column_to_header[column_value] = header

    # Ensure section parameters correspond 1-1 to transaction constructor parameters
    if section_parameter_set != constructor_parameter_set:
        LOGGER.error(
            "Invalid parameter set in section '%s': expected '%s', but received '%s'", section_name, str(constructor_parameter_set), str(section_parameter_set)
        )
        sys.exit(1)

    return result


# Typecheck plugin section parameters against the plugin constructor signature and build an initialization parameter dictionary
def _validate_plugin_configuration(ini_config: ConfigParser, plugin_name: str, constructor_signature: Signature) -> Dict[str, Union[str, int, float, bool]]:
    result: Dict[str, Union[str, int, float, bool]] = {}

    for parameter in constructor_signature.parameters:
        annotation: Any = constructor_signature.parameters[parameter].annotation
        if getattr(annotation, "__origin__", None) is Union and len(annotation.__args__) == 2 and annotation.__args__[1] is type(None):
            if ini_config[plugin_name].get(parameter) is not None:
                annotation = annotation.__args__[0]
            else:
                result[parameter] = None
                continue
        if annotation is str:
            result[parameter] = ini_config[plugin_name][parameter]
        elif annotation is int:
            result[parameter] = ini_config.getint(plugin_name, parameter)
        elif annotation is float:
            result[parameter] = ini_config.getfloat(plugin_name, parameter)
        elif annotation is bool:
            result[parameter] = ini_config.getboolean(plugin_name, parameter)
        else:
            LOGGER.error("Unsupported type for parameter '%s' in plugin '%s' constructor (only str, int, float and bool are allowed)", parameter, plugin_name)
            sys.exit(1)

    return result
