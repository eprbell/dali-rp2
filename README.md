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

# DaLI for RP2 v0.3.5
[![Static Analysis / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/static_analysis.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/static_analysis.yml)
[![Documentation Check / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/documentation_check.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/documentation_check.yml)
[![Unix Unit Tests / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/unix_unit_tests.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/unix_unit_tests.yml)
[![Windows Unit Tests / Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/windows_unit_tests.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/windows_unit_tests.yml)
[![CodeQL/Main Branch](https://github.com/eprbell/dali-rp2/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/eprbell/dali-rp2/actions/workflows/codeql-analysis.yml)

## Table of Contents
* **[Introduction](https://github.com/eprbell/dali-rp2/tree/main/README.md#introduction)**
* **[License](https://github.com/eprbell/dali-rp2/tree/main/README.md#license)**
* **[Download](https://github.com/eprbell/dali-rp2/tree/main/README.md#download)**
* **[Installation](https://github.com/eprbell/dali-rp2/tree/main/README.md#installation)**
  * [Ubuntu Linux](https://github.com/eprbell/dali-rp2/tree/main/README.md#installation-on-ubuntu-linux)
  * [macOS](https://github.com/eprbell/dali-rp2/tree/main/README.md#installation-on-macos)
  * [Windows 10](https://github.com/eprbell/dali-rp2/tree/main/README.md#installation-on-windows-10)
  * [Other Unix-like Systems](https://github.com/eprbell/dali-rp2/tree/main/README.md#installation-on-other-unix-like-systems)
* **[Running](https://github.com/eprbell/dali-rp2/tree/main/README.md#running)**
  * [Linux, macOS, Windows 10 and Other Unix-like Systems](https://github.com/eprbell/dali-rp2/tree/main/README.md#running-on-linux-macos-windows-10-and-other-unix-like-systems)
  * [Windows 10](https://github.com/eprbell/dali-rp2/tree/main/README.md#running-on-windows-10)
* **[Input and Output Files](https://github.com/eprbell/dali-rp2/tree/main/README.md#input-and-output-files)**
* **[Reporting Bugs](https://github.com/eprbell/dali-rp2/tree/main/README.md#reporting-bugs)**
* **[Contributing](https://github.com/eprbell/dali-rp2/tree/main/README.md#contributing)**
* **[Developer Documentation](https://github.com/eprbell/dali-rp2/tree/main/README.md#developer-documentation)**
* **[Frequently Asked Questions](https://github.com/eprbell/dali-rp2/tree/main/README.md#frequently-asked-questions)**
* **[Change Log](https://github.com/eprbell/dali-rp2/tree/main/README.md#change-log)**

## Introduction
[DaLI](https://pypi.org/project/dali-rp2) (Data Loader Interface) is an experimental data loader and input generator for [RP2](https://pypi.org/project/rp2/), the privacy-focused, free, [open-source](https://github.com/eprbell/dali-rp2) US cryptocurrency tax calculator.

It performs the following operations:
* it reads in crypto transaction information from multiples sources: CSV files and/or REST-based services;
* it analyzes, processes and merges this data;
* it uses it the processed data to generate an ODS input file for RP2 and its respective JSON configuration file;

DaLI has a programmable plugin architecture for data loaders (both CSV and REST-based): help us make DaLI a robust open-source, community-driven crypto data loader by contributing plugins for exchanges and wallets!

Read instructions on [how to run DaLI](https://github.com/eprbell/dali-rp2/tree/main/README.md#running).

**IMPORTANT DISCLAIMER**:
* DaLI offers no guarantee of correctness (read the [license](https://github.com/eprbell/dali-rp2/tree/main/LICENSE)): always verify results with the help of a tax professional.

## License
DaLI is released under the terms of Apache License Version 2.0. For more information see [LICENSE](https://github.com/eprbell/dali-rp2/tree/main/LICENSE) or <http://www.apache.org/licenses/LICENSE-2.0>.

## Download
The latest version of DaLI can be downloaded at: <https://pypi.org/project/dali-rp2/>

## Installation
DaLI has been tested on Ubuntu Linux, macOS and Windows 10 but it should work on all systems that have Python version 3.7.0 or greater.

### Installation on Ubuntu Linux
Open a terminal window and enter the following commands:
```
sudo apt-get update
sudo apt-get install python3 python3-pip
```

Then install DaLI:
```
pip install dali-rp2
```
### Installation on macOS
First make sure [Homebrew](https://brew.sh) is installed, then open a terminal window and enter the following commands:
```
brew update
brew install python3
```

Then install DaLI:
```
pip install dali-rp2
```
### Installation on Windows 10
First make sure [Python](https://python.org) 3.7 or greater is installed (in the Python installer window be sure to click on "Add Python to PATH"), then open a PowerShell window and enter the following:
```
pip install dali-rp2
```

### Installation on Other Unix-like Systems
* install python 3.7 or greater
* install pip3
* `pip install dali-rp2`

## Running
DaLI reads in a user-prepared configuration file in [INI format](https://en.wikipedia.org/wiki/INI_file), which is used to initialize data loaders (plugins) and configure DaLI's behavior. The format of the configuration file is described in detail in the [configuration file](https://github.com/eprbell/dali-rp2/tree/main/docs/configuration_file.md) documentation.

An example of a configuration file can be found in [test_config.ini](config/test_config.ini)

After reading the configuration file, DaLI reads crypto data from native sources and generates a RP2 input ODS file and RP2 configuration file in the `output` directory or where specified with the `-o` CLI option.

To try DaLI with the example configuration, download:
* [test_config.ini](config/test_config.ini)
* [test_manual_in.csv](input/test_manual_in.csv)
* [test_manual_intra.csv](input/test_manual_intra.csv)
* [test_trezor_alice.csv](input/test_trezor_alice_btc.csv)
* [test_trezor_bob.csv](input/test_trezor_bob_btc.csv)

Let's call `<download_directory>` the location of the downloaded files and copy all CSV files into it. Open a terminal window (or PowerShell if on Windows) and enter the following commands:

Create an `input` directory in the `<download_directory>`:
  ```
  cd <download_directory>
  mkdir input
  mv *.csv input
  ```
To generate output for the example files::
  ```
  cd <download_directory>
  dali -s -o output -p test_ test_config.ini
  ```
The `-s` option enables reading spot price information from Yahoo Finance, when it's not available in the CSV or REST service.

The ODS output file is generated in the output directory (or wherever specified with the -o option).

To print command usage information for the `dali` command:
  ```
  dali --help
  ```

To compute taxes with RP2 using the generated input files:
  ```
  cd <download_directory>
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
