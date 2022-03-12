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
