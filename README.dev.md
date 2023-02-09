<!--- Copyright 2022 eprbell --->

<!--- Licensed under the Apache License, Version 2.0 (the "License"); --->
<!--- you may not use this file except in compliance with the License. --->
<!--- You may obtain a copy of the License at --->

<!---     http://www.apache.org/licenses/LICENSE-2.0 --->

<!--- Unless required by applicable law or agreed to in writing, software --->
<!--- distributed under the License is distributed on an "AS IS" BASIS, --->
<!--- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. --->
<!--- See the License for the specific language governing permissions and --->
<!--- limitations under the License. --->

# DaLI for RP2 v0.6.3 Developer Guide
[![Static Analysis / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/static_analysis.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/static_analysis.yml)
[![Documentation Check / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/documentation_check.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/documentation_check.yml)
[![Unix Unit Tests / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/unix_unit_tests.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/unix_unit_tests.yml)
[![Windows Unit Tests / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/windows_unit_tests.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/windows_unit_tests.yml)
[![CodeQL/Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/codeql-analysis.yml)

## Table of Contents
* **[Introduction](#introduction)**
* **[License](#license)**
* **[Download](#download)**
* **[Setup](#setup)**
  * [Ubuntu Linux](#setup-on-ubuntu-linux)
  * [macOS](#setup-on-macos)
  * [Windows 10](#setup-on-windows-10)
  * [Other Unix-like Systems](#setup-on-other-unix-like-systems)
* **[Source Code](#source-code)**
* **[Development](#development)**
  * [Design Guidelines](#design-guidelines)
  * [Development Workflow](#development-workflow)
  * [Unit Tests](#unit-tests)
* **[Creating a Release](#creating-a-release)**
* **[Internal Design](#internal-design)**
* **[The Transaction Resolver](#the-transaction-resolver)**
* **[Plugin Development](#plugin-development)**
  * [Data Loader Plugin Development](#data-loader-plugin-development)
  * [Pair Converter Plugin Development](#pair-converter-plugin-development)
  * [Country Plugin Development](#country-plugin-development)
  * [Plugin Laundry List](#plugin-laundry-list)
* **[Frequently Asked Developer Questions](#frequently-asked-developer-questions)**

## Introduction
This document describes [DaLI](https://github.com/eprbell/dali-rp2) setup instructions, development workflow, design principles, source tree structure, internal design and plugin architecture.

## License
DaLI is released under the terms of Apache License Version 2.0. For more information see [LICENSE](LICENSE) or <http://www.apache.org/licenses/LICENSE-2.0>.

## Download
The latest DaLI source can be downloaded at: <https://github.com/eprbell/dali-rp2>.

## Setup
DaLI has been tested on Ubuntu Linux, macOS and Windows 10 but it should work on all systems that have Python version 3.7.0 or greater. Virtualenv is recommended for DaLI development.

### Setup on Ubuntu Linux
First make sure Python, pip and virtualenv are installed. If not, open a terminal window and enter the following commands:
```
sudo apt-get update
sudo apt-get install python3 python3-pip virtualenv
```

Then install DaLI Python package requirements:
```
cd <rp2_directory>
virtualenv -p python3 .venv
. .venv/bin/activate
.venv/bin/pip3 install -e '.[dev]'
```
### Setup on macOS
First make sure [Homebrew](https://brew.sh) is installed, then open a terminal window and enter the following commands:
```
brew update
brew install python3 virtualenv
```

Then install DaLI Python package requirements:
```
cd <rp2_directory>
virtualenv -p python3 .venv
. .venv/bin/activate
.venv/bin/pip3 install -e '.[dev]'
```
### Setup on Windows 10
First make sure [Python](https://python.org) 3.7 or greater is installed (in the Python installer window be sure to click on "Add Python to PATH"), then open a PowerShell window and enter the following commands:
```
python -m pip install virtualenv
```

Then install DaLI Python package requirements:
```
cd <rp2_directory>
virtualenv -p python .venv
.venv\Scripts\activate.ps1
python -m pip install -e ".[dev]"
```
### Setup on Other Unix-like Systems
* install python 3.7 or greater
* install pip3
* install virtualenv
* cd _<dali_directory>_
* `virtualenv -p python3 .venv`
* `.venv/bin/pip3 install -e '.[dev]'`

## Source Code
The RP2 source tree is organized as follows:
* `.bumpversion.cfg`: bumpversion configuration;
* `CHANGELOG.md`: change log document;
* `config/`: config files for examples and tests;
* `CONTRIBUTING.md`: contribution guidelines;
* `docs/`: additional documentation, referenced from the README files;
* `.editorconfig`;
* `.gitattributes`;
* `.github/workflows/`: configuration of Github continuous integration;
* `.gitignore`;
* `input/`: examples and tests;
* `input/golden/`: expected outputs that Dali tests compare against;
* `.isort.cfg`: isort configuration;
* `LICENSE`: license information;
* `Makefile`: alternative old-school build flow;
* `MANIFEST.in`: source distribution configuration;
* `mypy.ini`: mypy configuration;
* `.pre-commit-config.yaml`: pre-commit configuration;
* `.pylintrc`: pylint configuration;
* `pyproject.toml`: packaging configuration;
* `README.dev.md`: developer documentation;
* `README.md`: user documentation;
* `setup.cfg`: static packaging configuration file;
* `setup.py`: dynamic packaging configuration file;
* `src/dali`: DaLI code, including classes for transactions, ODS and config genator, transaction resolver, etc.;
* `src/dali/data/`: spreadsheet templates that are used by the ODS generator;
* `src/dali/plugin/country/`: country plugins/entry points;
* `src/dali/plugin/input/csv/`: CSV-based data loader plugins;
* `src/dali/plugin/input/rest/`: REST-based data loader plugins;
* `src/dali/plugin/pair_converter/`: pair converter plugins;
* `src/stubs/`: DaLI relies on third-party libraries, some of which don't have typing information, so it is added here;
* `tests/`: unit tests.

## Development
Read the [Contributing](CONTRIBUTING.md) document on pull requests guidelines.

### Design Guidelines
DaLI code adheres to these principles:
* user privacy is of paramount importance: user data never leaves the user's machine. The only networking logic allowed is read-only REST API calls in data loader plugins to collect transaction data;
* all identifiers have [descriptive names](https://realpython.com/python-pep8/#how-to-choose-names);
* immutability:
  * global variables have upper case names, are initialized where declared and are never modified afterwards;
  * generally data structures are read-only (the only exceptions are for data structures that would incur a major complexity increase without write permission: e.g. AVL tree node):
    * class fields are private (prepended with double-underscore). Fields that need public access have a read-only property. Write-properties are not used;
    * @dataclass classes have `frozen=True`;
* data encapsulation: all data fields are private (prepended with double-underscore):
  * for private access nothing else is needed;
  * for protected access add a read-only property starting with single underscore or an accessor function starting with `_get_`;
  * for public access add a read-only property starting with no underscore or an accessor function starting with `get_`;
* runtime checks: parameters of public functions are type-checked at runtime;
* type hints: all variables and functions have Python type hints (with the exception of local variables, for which type hints are optional);
* no id-based hashing: classes that are added to dictionaries and sets redefine `__eq__()`, `__neq__()` and `__hash__()`;
* encapsulated math: all high-precision math is done via `RP2Decimal` (a subclass of Decimal), to ensure the correct precision is used throughout the code. `RP2Decimal` instances are never mixed with other types in expressions;
* f-strings only: every time string interpolation is needed, f-strings are used;
* no raw strings (unless they occur only once): use global constants instead;
* logging: logging is done via the `logger` module;
* no unnamed tuples: dataclasses or named tuples are used instead;
* one class per file (with exceptions for trivial classes);
* files containing a class must have the same name as the class (but lowercase with underscores): e.g. class AbstractEntry lives in file abstract_entry.py;
* abstract classes' name starts with `Abstract`;
* no imports with `*`.

### Development Workflow
DaLI uses pre-commit hooks for quick validation at commit time and continuous integration via Github actions for deeper testing. Pre-commit hooks invoke: flake8, black, isort, pyupgrade and more. Github actions invoke: mypy, pylint, bandit, unit tests (on Linux, Mac and Windows), markdown link check and more.

While every commit and push are automatically tested as described, sometimes it's useful to run some of the above commands locally without waiting for continuous integration. Here's how to run the most common ones:
* run unit tests: `pytest --tb=native --verbose`
* type check: `mypy src tests`
* lint: `pylint -r y src tests/*.py`
* security check: `bandit -r src`
* reformat code: `black src tests`
* sort imports: `isort .`
* run pre-commit tests without committing: `pre-commit run --all-files`

Logs are stored in the `log` directory. To generate debug logs, prepend the command line with `LOG_LEVEL=DEBUG`, e.g.:
```
LOG_LEVEL=DEBUG bin/dali_us -s -o output/ config/test_config.ini
```

### Unit Tests
Unit tests are in the [tests](tests) directory. Please add unit tests for any new code.

## Creating a Release
This section is for project maintainers.

To create a new release:
* add a section named as the new version in CHANGELOG.md
* use the output of `git log` to collect significant changes since last version and add them to CHANGELOG.md as a list of brief bullet points
* `git add CHANGELOG.md`
* `git commit -m "Updated with latest changes" CHANGELOG.md`
* `bumpversion patch` (or `bumpversion minor` or `bumpversion major`)
* `git push`
* wait for all tests to pass successfully on Github
* add a tag in Github (named the same as the version but with a `v` in front, e.g. `v1.0.4`):  click on "Releases" and then "Draft a new release"

To create a Pypi distribution:
* `make distribution`
* `make upload_distribution`

## Internal Design
DaLI's control flow is as follows (see [dali_main.py](src/dali/dali_main.py)):
* parse the INI configuration file which includes plugin initialization parameters and global configuration sections;
* discover and instantiate pair converter plugins using the initialization parameters from the config file and store them in a list to be passed to the transaction resolver (see below);
* discover and instantiate data loader plugins using the initialization parameters from the config file and call their `load()` method, which reads data from native sources (CSV files or REST endpoints) and returns it in a standardized format: a list of [AbstractTransaction](src/dali/abstract_transaction.py) instances. This list can contain instances of any `AbstractTransaction` subclass: [InTransaction](src/dali/in_transaction.py) (acquired crypto), [OutTransaction](src/dali/out_transaction.py) (disposed-of crypto) or [IntraTransaction](src/dali/intra_transaction.py) (crypto transferred across accounts controlled by the same person or by people filing together);
* join the lists returned by plugin `load()` calls and pass them to the [transaction resolver](src/dali/transaction_resolver.py), which merges incomplete transactions, filling in any missing information (e.g. the spot price) and returning a normalized list of transactions (see below for more details);
* pass the resolved data to the RP2 [ODS input file generator](src/dali/ods_generator.py) and the RP2 [config file generator](src/dali/configuration_generator.py), which create the input files for RP2.

## The Transaction Resolver
The [transaction resolver](src/dali/transaction_resolver.py) is a critical component of DaLI and has the purpose of merging and normalizing transaction data from data loader plugins. Data loader plugins operate on incomplete information: e.g. if a transaction transfers crypto from Coinbase to Trezor, the Coinbase data loader plugin has no way of knowing that the destination address represents a Trezor account (because Coinbase itself doesn't have this information): so the plugin cannot fill the `to_exchange`, `to_holder` and `crypto_received` fields of the IntraTransaction (so it fills them with `Keyword.UNKNOWN`). Similarly the Trezor data loader plugin cannot know that the source address belongs to a Coinbase account and therefore it cannot fill the `from_exchange`, `from_holder` and `crypto_sent` fields of the IntraTransaction (so it fills them with `Keyword.UNKNOWN`). So how does DaLI merge these two incomplete transaction parts into one complete IntraTransaction? It uses the transaction resolver, which relies on the `unique_id` field of each incomplete transaction to pair them: typically this is the transaction hash, but in certain special cases it could also be an exchange-specific value that identifies uniquely the transaction. The transaction resolver analyzes all generated transactions, looks for pairs of incomplete ones with the same `unique_id` and merges them into a single one (be sure to read the FAQ on [how to populate the unique_id field](https://github.com/eprbell/dali-rp2/blob/main/docs/developer_faq.md#how-to-fill-the-unique-id-field), which discusses this topic in detail, and the [Manual Plugin examples on partial transaction resolution](https://github.com/eprbell/dali-rp2/blob/main/docs/configuration_file.md#partial-transactions-and-transaction-resolution)).

For this reason it's essential that all data loader plugins populate the `unique_id` field as best they can and with a global identifier that is understood by all plugins (again, the transaction hash): without it the transaction resolver cannot merge incomplete data. Sometimes hash information is missing (especially in CSV files) and so it's impossible to populate the `unique_id` field: in such cases use `Keyword.UNKNOWN`, but the user will have to manually modify the generated result and perform transaction resolution manually, which is not ideal.

Another feature of the transaction resolver is filling in or converting certain fiat values, using the pair converter plugin list passed to it:
* `spot_price`: sometimes the native sources read by data loader plugins don't provide this information. If instructed by the user with the `-s` option, the transaction resolver tries to retrieve this information from pair converter plugins;
* foreign fiat values: transactions that occurred on foreign exchanges can have their fiat values denominated in non-native fiat. When the transaction resolver detects this condition, it converts these foreign fiat values to native fiat (e.g. USD for US, JPY for Japan, etc.), using pair converter plugins.

## Plugin Development
DaLI has a plugin architecture for data loaders, pair converters and countries, which makes it extensible for new use cases:
* data loader plugins read crypto data from a native source (REST endpoint or CSV file) and convert it into DaLI's internal format;
* pair converter plugins convert from a currency to another (both fiat and crypto);
* country plugins enable support for new countries.

### Data Loader Plugin Development
Data loader plugins live in one of the following directories, depending on their type (CSV or REST):
* `src/dali/plugin/input/csv/`;
* `src/dali/plugin/input/rest/`.

If at all possible [prefer implementing a REST plugin over a CSV](https://github.com/eprbell/dali-rp2/blob/main/docs/developer_faq.md#should-i-implement-a-csv-or-a-rest-data-loader-plugin) one, because CSV sources are often incomplete. Furthermore, if the exchange is [compatible with CCXT](https://github.com/ccxt/ccxt#certified-cryptocurrency-exchanges), a CCXT-based REST plugin is the most preferred.

CCXT-based data loader plugins are subclasses of [AbstractCcxtInputPlugin](src/dali/abstract_ccxt_input_plugin.py).
* define their own constructor with any custom parameters;
* invoke the superclass constructor in their own constructor;
* implement the `exchange_name()` and `plugin_name()` methods that return the name of the exchange and the name of the plugin without spaces as a `str`.
* implement the `_initialize_client()` method that returns an initialized instance of the subclass of `Exchange` that handles the plugin's exchange. The initialization of a CCXT exchange instance is described [here](https://docs.ccxt.com/en/latest/manual.html#instantiation).
* implement the `_get_process_deposits_pagination_detail_set()`, `_get_process_trades_pagination_detail_set()` and `_get_process_withdrawals_pagination_detail_set()` methods that return a [AbstractPaginationDetailSet](src/dali/ccxt_pagination.py) instance. If an exchange does not support one of these unified functions, return `None`.
* implement the `_process_gains()` method, which reads data for gains received using [the implicit API of CCXT](https://docs.ccxt.com/en/latest/manual.html#implicit-api), accepts a list of [InTransaction](src/dali/in_transaction.py) instances, and [OutTransaction](src/dali/out_transaction.py) instances. If the exchange doesn't support gains, the plugin can use `pass` in the method.
* implement the `_process_implicit_api()` method, which reads any data not covered by `_process_gains()` using [the implicit api of CCXT](https://docs.ccxt.com/en/latest/manual.html#implicit-api) and accepts a list of [InTransaction](src/dali/in_transaction.py) instances, [OutTransaction](src/dali/out_transaction.py) instances, and [IntraTransaction](src/dali/intra_transaction.py). If the the implicit API is not needed, the plugin can use `pass` in the method.
* the protected property `_client` can be used in the subclasses to access the exchange instance to make calls to the implicit api in the subclass.
* the protected property `_logger` can be used for logging.
* implementing the `load()` method is not required for CCXT-based data loader plugins.

All other data loader plugins are subclasses of [AbstractInputPlugin](src/dali/abstract_input_plugin.py).
* define their own constructor with any custom parameters;
* invoke the superclass constructor in their own constructor;
* implement the `load()` method, which reads data from the native source and returns a list of [AbstractTransaction](src/dali/abstract_transaction.py) instances, which can be of any of the following classes: [InTransaction](src/dali/in_transaction.py) (acquired crypto), [OutTransaction](src/dali/out_transaction.py) (disposed-of crypto) or [IntraTransaction](src/dali/intra_transaction.py) (crypto transferred across accounts controlled by the same person or by people filing together). The fields of transaction classes are described [here](docs/configuration_file.md#manual-section-csv).

If a field is unknown the plugin can fill it with `Keyword.UNKNOWN`, unless it's an optional field (check its type hints in the Python code), in which case it can be `None`. The `unique_id` requires special attention, because the transaction resolver uses it to match and join incomplete transactions: the plugin must ensure to [populate it with the correct value](https://github.com/eprbell/dali-rp2/blob/main/docs/developer_faq.md#how-to-fill-the-unique-id-field). See the [transaction resolver](#the-transaction-resolver) section for more details on `unique_id`.

For an example of a CCXT-based data loader look at the [Binance](src/dali/plugin/input/rest/binance_com.py) plugin, for an example of a REST-based data loader look at the [Coinbase](src/dali/plugin/input/rest/coinbase.py) plugin, for an example of a CSV-based data loader look at the [Trezor](src/dali/plugin/input/csv/trezor.py) plugin.

### Pair Converter Plugin Development
Pair converter plugins live in the following directory:
* `src/dali/plugin/pair_converter`.

All pair converter plugins are subclasses of [AbstractPairConverterPlugin](src/dali/abstract_pair_converter_plugin.py) and they must:
* implement the `name()` method;
* implement the `cache_key()` method;
* implement the `get_historic_bar_from_native_source()` method.

For an example of pair converter look at the [Historic-Crypto](src/dali/plugin/pair_converter/historic_crypto.py) plugin.

### Country Plugin Development
Country plugins are reused from RP2 and their DaLI counterpart has trivial implementation.

To add support for a new country in DaLI:
* [add a country plugin to RP2](https://github.com/eprbell/rp2/blob/main/README.dev.md#adding-support-for-a-new-country);
* add a new Python file to the `src/dali/plugin/country` directory and name it after the [ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) 2-letter code for the country;
* in the newly added file add a DaLI-specific entry point instantiating the new country instance and passing it to `dali_main()`. As an example see the [us.py](src/dali/plugin/country/us.py) file;
* add a console script to setup.cfg pointing the new country dali_entry (see the US example in the console_scripts section of setup.cfg).

### Plugin Laundry List
When submitting a new plugin open a [PR](https://github.com/eprbell/dali-rp2/pulls) and make sure all the following bullet points apply to your code:
1. the plugin is privacy-focused: it doesn't send user data anywhere;
2. the plugin follows the [contribution guidelines](CONTRIBUTING.md#contributing-to-the-repository);
3. the plugin has one or more [unit test](tests/);
4. the plugin and its initialization parameters are documented in a section of [docs/configuration_file.md](docs/configuration_file.md).
5. the plugin lives in the appropriate subdirectory of `src/dali/plugin/`;

Data-loader-specific list:

6. the plugin lives in `src/dali/plugin/input/rest/` or `src/dali/plugin/input/csv/`, depending on its type;
7. the plugin creates transactions that have `unique_id` populated (typically with the transaction hash), unless the information is missing from the native source: this is essential to the proper operation of the [transaction resolver](#the-transaction-resolver);
8. REST plugins have three comments at the beginning of the file, containing links to:
  * REST API documentation
  * authentication procedure documentation
  * URL of the REST endpoint

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;This makes it easier to review the code. E.g.:
```
    # REST API: https://developers.coinbase.com/api/v2
    # Authentication: https://developers.coinbase.com/docs/wallet/api-key-authentication
    # Endpoint: https://api.coinbase.com
```
9. CSV plugins have a comment at the beginning of the file, documenting the format. E.g.:
    ```
    # CSV Format: timestamp; type; transaction_id; address; fee; total
    ```
10. REST plugins document what a sample JSON response looks like after the JSON call.

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;This also makes it easier to review the code. For example:
```
    # [
    #   {
    #     'time': '1624233772000', // epoch timestamp in ms
    #     'asset': 'BTC',          // locked asset
    #     'amount': '0.017666',    // Amount locked
    #   },
    # ]
```
11. the plugin's `load()` method is implemented and returns a list of AbstractTransaction subclasses;
12. the plugin's `__init__()` method calls the superclass constructor:
    ```
    super().__init__(account_holder, native_fiat=native_fiat)
    ```
13. the plugin's `__init__()` method creates a plugin-specific logger with a name that uniquely identifies the specific instance of the plugin (typically you can add a subset of constructor parameters to ensure uniqueness): this way log lines can be easily distinguished by plugin instance. Example of a plugin-specific log in the constructor of the Trezor plugin:
    ```
        self.__logger: logging.Logger = create_logger(f"{self.__TREZOR}/{currency}/{self.__account_nickname}/{self.account_holder}")
    ```
14. the plugin uses `self.__logger.debug()` throughout its code to capture all native-format data (which is is useful for debugging). Note that `logger.debug()` calls only occur if the user sets `LOG_LEVEL=DEBUG`;
15. CSV plugins have one or more [unit test](tests/);
16. REST plugins have one or more [unit tests](tests/): use pytest-mock to simulate network calls (see [test_plugin_coinbase.py](tests/test_plugin_coinbase.py) for an example of this);

## Frequently Asked Developer Questions
Read the [frequently asked developer questions](docs/developer_faq.md).
