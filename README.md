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

# DaLI for RP2 v0.6.3

Privacy-focused, free, extensible data loader for RP2 (the crypto tax calculator)

[![Static Analysis / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/static_analysis.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/static_analysis.yml)
[![Documentation Check / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/documentation_check.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/documentation_check.yml)
[![Unix Unit Tests / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/unix_unit_tests.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/unix_unit_tests.yml)
[![Windows Unit Tests / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/windows_unit_tests.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/windows_unit_tests.yml)
[![CodeQL/Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/codeql-analysis.yml)

[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=I%20use%20RP2,%20the%20privacy-focused,%20open%20source,%20free,%20non-commercial%20crypto%20tax%20calculator&url=https://github.com/eprbell/rp2/?anything)

## Table of Contents
* **[Introduction](#introduction)**
* **[License](#license)**
* **[Download](#download)**
* **[Installation](#installation)**
  * [Ubuntu Linux](#installation-on-ubuntu-linux)
  * [macOS](#installation-on-macos)
  * [Windows 10](#installation-on-windows-10)
  * [Other Unix-like Systems](#installation-on-other-unix-like-systems)
* **[Running](#running)**
  * [Linux, macOS, Windows 10 and Other Unix-like Systems](#running-on-linux-macos-windows-10-and-other-unix-like-systems)
  * [Windows 10](#running-on-windows-10)
* **[Configuration File](#configuration-file)**
* **[Reporting Bugs](#reporting-bugs)**
* **[Contributing](#contributing)**
* **[Developer Documentation](#developer-documentation)**
* **[Frequently Asked Questions](#frequently-asked-questions)**
* **[Change Log](#change-log)**

## Introduction
[DaLI](https://github.com/eprbell/dali-rp2) (Data Loader Interface) is a data loader and input generator for [RP2](https://github.com/eprbell/rp2), the privacy-focused, free, non-commercial, open-source cryptocurrency tax calculator: DaLI removes the need to manually prepare RP2 input files. Just like RP2, DaLI is also free, non-commercial, open-source and it prioritizes user privacy by storing crypto transaction data on the user's computer and never sending it anywhere else.

It performs the following operations:
* it reads in crypto transaction information from multiples native sources: CSV files and/or REST-based services;
* it analyzes, processes and merges this data;
* it uses the processed data to generate an ODS input file for RP2 and its respective JSON configuration file.

DaLI has a programmable plugin architecture for [data loaders](https://github.com/eprbell/dali-rp2/blob/main/README.dev.md#data-loader-plugin-development) (both CSV and REST-based), [pair converters](https://github.com/eprbell/dali-rp2/blob/main/README.dev.md#pair-converter-plugin-development) and [countries](https://github.com/eprbell/dali-rp2/blob/main/README.dev.md#country-plugin-development). While some exchanges and wallets are already supported out-of-the-box, more are needed: help us make DaLI a robust open-source, community-driven crypto data loader by [contributing](https://github.com/eprbell/dali-rp2/tree/main/CONTRIBUTING.md#contributing-to-the-repository) plugins for exchanges and wallets! Check [open issues](https://github.com/eprbell/dali-rp2/issues) or open a new one.

DaLI has [unit test](https://github.com/eprbell/dali-rp2/tree/main/tests/) coverage to reduce the risk of regression.

Note that DaLI has RP2 as a dependency, so installing DaLI causes RP2 to be installed as well.

**IMPORTANT DISCLAIMER**:
* DaLI offers no guarantee of correctness (read the [license](https://github.com/eprbell/dali-rp2/tree/main/LICENSE)): always verify results with the help of a tax professional.
* The author of DaLI and RP2 is not a tax professional, but has used RP2 personally for a few years.

## License
DaLI is released under the terms of Apache License Version 2.0. For more information see [LICENSE](https://github.com/eprbell/dali-rp2/tree/main/LICENSE) or <http://www.apache.org/licenses/LICENSE-2.0>.

## Download
The latest version of DaLI can be downloaded at: <https://pypi.org/project/dali-rp2/>

## Installation
DaLI has been tested on Ubuntu Linux, macOS and Windows 10 but it should work on all systems that have Python version 3.7.0 or greater.

### Installation on Ubuntu Linux
Open a terminal window and enter the following commands:

```console
sudo apt-get update
sudo apt-get install python3 python3-pip
```

Then install DaLI:

```console
pip install dali-rp2
```

### Installation on macOS
First make sure [Homebrew](https://brew.sh) is installed, then open a terminal window and enter the following commands:

```console
brew update
brew install python3
```

Then install DaLI:

```console
pip install dali-rp2
```

### Installation on Windows 10
First make sure [Python](https://python.org) 3.7 or greater is installed (in the Python installer window be sure to click on "Add Python to PATH"), then open a PowerShell window and enter the following:

```console
pip install dali-rp2
```

### Installation on Other Unix-like Systems
* install python 3.7 or greater
* install pip3
* `pip install dali-rp2`

## Running
DaLI reads in a user-prepared configuration file in [INI format](https://en.wikipedia.org/wiki/INI_file), which is used to initialize data loaders and configure DaLI's behavior. The format of the configuration file is described in detail in the [configuration file](https://github.com/eprbell/dali-rp2/tree/main/docs/configuration_file.md) documentation.

An example of a configuration file can be found in [test_config.ini](https://github.com/eprbell/dali-rp2/tree/main/config/test_config.ini).

After processing the configuration file, DaLI reads crypto data from native sources and generates a RP2 input ODS file and a RP2 configuration file in the `output` directory or where specified with the `-o` CLI option.

The DaLI executable is country-dependent: `dali_<country_code>`, where country code is a [ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2), 2-letter identifier (e.g. `dali_us`, `dali_jp`, etc).

Provided [DaLI is installed](#installation), to generate RP2 input files from the example configuration, open a terminal window (or PowerShell if on Windows).

Create the directory structure and download the example input files using wget or manually.

```console
mkdir -p input
```

If your system has wget:

```console
wget https://raw.githubusercontent.com/eprbell/dali-rp2/main/config/test_config.ini
wget https://raw.githubusercontent.com/eprbell/dali-rp2/main/input/test_manual_in.csv -P input/
wget https://raw.githubusercontent.com/eprbell/dali-rp2/main/input/test_manual_intra.csv -P input/
wget https://raw.githubusercontent.com/eprbell/dali-rp2/main/input/test_trezor_alice_btc.csv -P input/
wget https://raw.githubusercontent.com/eprbell/dali-rp2/main/input/test_trezor_bob_btc.csv -P input/
```

If your system doesn't have wget, download the following files manually:
* [test_config.ini](https://github.com/eprbell/dali-rp2/tree/main/config/test_config.ini)
* [test_manual_in.csv](https://github.com/eprbell/dali-rp2/tree/main/input/test_manual_in.csv)
* [test_manual_intra.csv](https://github.com/eprbell/dali-rp2/tree/main/input/test_manual_intra.csv)
* [test_trezor_alice.csv](https://github.com/eprbell/dali-rp2/tree/main/input/test_trezor_alice_btc.csv)
* [test_trezor_bob.csv](https://github.com/eprbell/dali-rp2/tree/main/input/test_trezor_bob_btc.csv)

And move the csv files to the input directory:
```console
mv *.csv input
```

Then generate the US output files for rp2:
```console
dali_us -s -o output -p test_ test_config.ini
 ```

* `-s` option allows DaLI to retrieve spot price information from the Internet, when it's not available from the CSV files or REST endpoints.
* `-o` option specifies the directory where the ODS output file is generated.
* `-p` is the prefix of the output file.
* `test_config.ini` is the configuration that ties the inputs together.

To print command usage information for the `dali_us` command:

```console
dali_us --help
```

Finally compute taxes with RP2 using the generated input files (using both FIFO and LIFO accounting methods):

```console
rp2_us -m fifo -o output/ -p rp2_ output/test_crypto_data.config output/test_crypto_data.ods
rp2_us -m lifo -o output/ -p rp2_ output/test_crypto_data.config output/test_crypto_data.ods
```

## Configuration File
Read the [configuration file](https://github.com/eprbell/dali-rp2/tree/main/docs/configuration_file.md) documentation.

## Reporting Bugs
Read the [Contributing](https://github.com/eprbell/dali-rp2/tree/main/CONTRIBUTING.md#reporting-bugs) document.

## Contributing
Read the [Contributing](https://github.com/eprbell/dali-rp2/tree/main/CONTRIBUTING.md) document.

## Developer Documentation
Read the [developer documentation](https://github.com/eprbell/dali-rp2/tree/main/README.dev.md).

## Frequently Asked Questions
Read the [user FAQ list](https://github.com/eprbell/dali-rp2/tree/main/docs/user_faq.md) and the [developer FAQ list](https://github.com/eprbell/dali-rp2/tree/main/docs/developer_faq.md).

## Change Log
Read the [Change Log](https://github.com/eprbell/dali-rp2/tree/main/CHANGELOG.md) document.
