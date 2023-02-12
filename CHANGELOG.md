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

# RP2 Change Log

## 0.6.3
* added CCXT pair converter subclasses that are fixed to one exchange (Kraken, Binance)
* added fiat deposit support to Bitbank plugin
* small fixes and improvements

## 0.6.2
* added Kraken CSV price importer
* various fixes to CCXT-based plugins
* small improvements to some CSV data loaders
* updated documentation

## 0.6.1
* added CCXT-based abstract superclass for REST data loader plugins (#80): this makes it much easier to add a new REST-based data loader
* added Bitbank REST input plugin and CSV supplemental input plugin
* added Coincheck CSV supplemental input plugin
* updated RP2 to 1.3.1
* updated documentation

## 0.6.0
* As of RP2 1.3.0, RP2 configuration files are no longer expressed in JSON format: they now use the INI format. So DaLI has been updated to generates INI-format configuration files instead of JSON. Any old JSON-format configuration file can be converted to the new INI format with the following command: rp2_config <json_config>
* added Pionex CSV data loader plugin

## 0.5.2
* added Binance.com REST data loader plugin
* added Binance.com supplemental (CSV) data loader plugin (for autoinvest purchases and ETH to BETH conversions)
* added CCXT-based pair converter
* updated documentation

## 0.5.1
* updated RP2 dependency to latest version, which fixes 2 bugs in HIFO. If you're using HIFO be sure to use RP2 1.10 or better.
* tweaked documentation

## 0.5.0
* add top-level multi-thread support: it's now possible to run data loader plugins in parallel, using the -t option (which selects the number of parallel threads)
* added Nexo CSV input plugin
* Coinbase REST input plugin: added support for Coinbase Earn Reversals (due to CC refunds, etc.)
* BlockFI CSV input plugin: added new transaction types
* revised and improved all documentation
* added FAQs

## 0.4.12
* added country plugin infrastructure (US is the default country plugin). Default fiat is no longer hardcoded to USD (it now comes from the country plugin)
* updated generators and rest of the code to use the native fiat from the country plugin (previously it was hard coded as USD)
* the dali script has been renamed to dali_us to support the new country plugin architecture
* added exchange hint to pair converter API
* added Ledger CSV data loader
* added credit card spend transactions to Coinbase plugin
* renamed cache folder from .cache to .dali_cache
* updated documentation to reflect latest changes
* small additions and fixes to documentation and code

## 0.4.9
* added new pair conversion plugin infrastructure
* added support for currency conversion to transaction resolver (useful to support non-USD fiats from foreign exchanges)
* fixed PR #44: added configurable historical data behavior and improved caching
* small improvements to code and documentation

## 0.4.8
* fixed PR #35: added exchange_withdrawal transaction type to Coinbase plugin
* various documentation improvements
* updated RP2 dependency to 1.0.0

## 0.4.7
* fixed issue #34: sometimes Coinbase returns bad fiat data for crypto conversions (fiat amount of sale < fiat amount of buy). DaLI used to raise an exception: now it issues a warning and sets the fiat fee to 0
* Coinbase plugin: _process_transfer() refactoring

## 0.4.6
* fixed PRs #31 and #36: various improvements to the logic that reads prices from Web

## 0.4.5
* Coinbase Pro: fixed a bug which caused the total fiat value (including fee) to be incorrect for certain crypto conversions
* added developer FAQ

## 0.4.4
* fixed a regression introduced recently: in certain cases, some fees related to conversions would no longer be generated (thus causing small deductions to be ignored)
* added support for USDC: previously it was conflated into fiat and didn't generate a tab, now it does
* Coinbase plugin: added support for fiat_withdrawal and fiat_deposit transactions. They are now tracked internally and no longer cause an unsupported transaction warning
* Coinbase Pro plugin: added support for "conversion" native transactions (these seem to occur when CBPro converts from/to a stable coin)
* minor changes to documentation

## 0.4.3
* rewrote Coinbase coin swap logic: it now handles correctly swaps from stable coins with a fee (previously it would ignore the fee)
* various documentation fixes and some refactoring

## 0.4.2
* fixed issue #21: fee-typed out-transactions in the manual plugin would cause an exception
* reviewed all documentation

## 0.4.1
* minor documentation fixes

## 0.4.0
* fixed issue #20: Coinbase plugin now supports the staking_reward transaction type
* fix for Coinbase reporting fiat with low precision (only 2 decimal digits): if the value is less than 1c Coinbase rounds it to zero, which caused various computation problems (spot_price, etc.). As a workaround, when this condition is detected the plugin sets affected fields to UNKNOWN or None (depending on their nature), so that they can be filled later by the transaction resolver and RP2

## 0.3.28
* added threading support to Coinbase Pro
* thread count can now be specified in the plugin configuration section of the .ini file
* additional fix in missing fiat_fee issue in crypto swaps on Coinbase (issue #15)
* updated documentation

## 0.3.27
* fix for missing fiat_fee issue in crypto swaps on Coinbase (issue #15)

## 0.3.26
* -s CLI option to read missing spot prices from Web: rewrote implementation from Yahoo Finance (daily granularity) to Coinbase Pro (minute granularity)
* moved cache logic to its own module, so it can be used in multiple places: plugins, transaction_resolver, etc.
* fixed issue #19: the user needs to pass -s to read highly granular price data from Coinbase Pro, when it's missing (or computed as 0)

## 0.3.25
* merged PR #18: Coinbase plugin wasn't handling crypto conversions correctly and generated negative amounts.

## 0.3.23
* merged PR #14: in Coinbase Pro plugin crypto fee is now modeled correctly both for buy and sell-side fills
* fixed issue #16: added support for inflation_reward transaction in Coinbase plugin
* added load caching to speed up development (PR #12)
* added parallelism to Coinbase plugin (PR #12)

## 0.3.22
* fixed issue #16: added support for Coinbase inflation reward transaction
* added error log for unknown transaction type in all plugins, so they are easy to identify and report
* added FAQ on supported exchanges/wallets
* added support for more transaction types in BlockFi plugin (PR #11)

## 0.3.21
* fixed bug reported in issue https://github.com/eprbell/dali-rp2/issues/10
* fixed from/to_currency detection in Coinbase Pro plugin (PR #9)
* added initial version of developer FAQ document: https://github.com/eprbell/dali-rp2/blob/main/docs/developer_faq.md
* minor fixes to documentation

## 0.3.20
* fixed a bug in transaction resolver: IN -> IN transaction hint was assigning wrong value to fiat_fee
* fixed a bug in Coinbase plugin: receiving a crypto gift from another Coinbase user generated a transaction with wrong fiat_in_with_fee value
* reworked some of the code to use latest RP2 features: specifically fee-only transactions and crypto_fee in in-transactions. E.g.: Coinbase Pro coin swap code, transaction resolver INTRA -> OUT transaction hint application, etc.
* DaLI output file (the RP2 input file) now has in-transaction crypto_fee. Also updated unit test golden file
* refactored all transaction constructor calls to use keyword arg calling style
* updated setup.cfg dependencies to latest RP2

## 0.3.19
* Trezor plugin: AM/PM was not parsed correctly in timestamp. Fixed.
* Minor fixes

## 0.3.18
* fixed a limitation in timestamp processing in Trezor and Trezor Old plugins. The timezone that was passed to the plugin constructor could only be an ISO 8601 format offset: something like "America/Los Angeles" would not be accepted. This caused problems with daylight saving timestamps in Trezor CSV files: daylight savings time would just be ignored, thus causing Trezor summer timestamps to be 1 hour off
* updated input, input/golden, config and docs/configuration_file.md to reflect the above fix
* added pytz stubs

## v0.3.17
* added new FAQ on adding support for new wallets and exchanges
* minor fixes

## v0.3.16
* Trezor plugin: dusting attack was incorrectly interpreted as cost-only transaction. Now it issues a warning to the user
* minor fixes to documentation

## v0.3.15
* minor edits to documentation and metafiles

## v0.3.12
* fixed a few broken links in README.md (when used from Pypi only)
* fixed CLI --help description
* minor improvements to documentation

## v0.3.7
* major improvements to documentation
* added notes in generated file to describe crypto conversion transactions in Coinbase Pro
* small performance improvement: signature reflexive call is now called only once per class, instead of once per instance

## v0.3.6
* template.ods was missing from the final package. Fixed in setup.cfg
* various fixes to documentation

## v0.3.4
* First version tracked in change log and uploaded to Github
