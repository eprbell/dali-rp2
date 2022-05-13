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

# RP2 Frequently Asked Questions (User)

## Table of Contents
* **[General Questions](#general-questions)**
  * [What Is the Timestamp Format?](#what-is-the-timestamp-format)
  * [Can I Avoid Writing a Config File from Scratch?](#can-i-avoid-writing-a-config-file-from-scratch)
  * [What Exchanges/Wallets Are Supported?](#what-exchangeswallets-are-supported)
  * [Can You Add Support for a New Wallet or Exchange?](#can-you-add-support-for-a-new-wallet-or-exchange)
  * [How to Represent Transactions between Unsupported Exchanges and Wallets?](#how-to-represent-transactions-between-unsupported-exchanges-and-wallets)
  * [How to Represent a Transaction from/to a Supported Exchange/Wallet to/from an Unsupported One](#how-to-represent-a-transaction-fromto-a-supported-exchangewallet-tofrom-an-unsupported-one)
  * [What if the Spot Price Is Missing for Some Transactions?](#what-if-the-spot-price-is-missing-for-some-transactions)
  * [What if I and My Spouse File Taxes Jointly?](#what-if-i-and-my-spouse-file-taxes-jointly)
  * [What if a Transaction Is Generated Differently Than I Expect?](#what-if-a-transaction-is-generated-differently-than-i-expect)
  * [What if a Transaction Is Generated With Some Fields Set to __UNKNOWN?](#what-if-a-transaction-is-generated-with-some-fields-set-to-__unknown)
  * [How to Report a DaLI Bug Without Sharing Personal Information?](#how-to-report-a-dali-bug-without-sharing-personal-information)
  * [What if I Don't Trust DaLI With My Crypto Data?](#what-if-i-dont-trust-dali-with-my-crypto-data)
  * [Who is the Author of DaLI?](#who-is-the-author-of-dali)

* **[Tax Questions](#tax-questions)**
  * [What Events Are Taxable?](#what-events-are-taxable)
  * [Can I Avoid Paying Crypto Taxes?](#can-i-avoid-paying-crypto-taxes)
  * [Which Resources Can I Use to Learn About Crypto Taxes?](#which-resources-can-i-use-to-learn-about-crypto-taxes)

## General Questions

### What Is the Timestamp Format?
Timestamp format is [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) (see [examples](https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations) of timestamps in this format). Note that RP2 requires full timestamps, including date, time and timezone.

### Can I Avoid Writing a Config File from Scratch?
You can use the [test_config.ini](../config/test_config.ini) as a starting point and the [configuration file](configuration_file.md) documentation as reference.

### What Exchanges/Wallets Are Supported?
Supported exchange and wallet plugin are listed in the [configuration_file](configuration_file.md#data-loader-plugin-sections) documentation.

### Can You Add Support for a New Wallet or Exchange?
Since there are hundreds of CSV formats and REST APIs, it's not possible for DaLI's author to single-handedly add support for everything. For this reason DaLI has been imagined as a community effort and designed with a robust data loader plugin infrastructure, allowing people to contribute plugins for new exchanges and wallets with minimal work. The plugin API is encapsulated and well defined: this makes it easy for new coders to enter the project.

Here are a few pointers to start working on a plugin:
* [contributing guidelines](../CONTRIBUTING.md#contributing-to-the-repository);
* plugin development is described in the [developer documentation](../README.dev.md), in particular read the [Dali Internals](../README.dev.md#dali-internals) and [Plugin Development](../README.dev.md#plugin-development) sections, which contain all the information needed to build a new data loader plugin;
* before submitting a PR double-check the [DaLI Plugin Laundry List](../README.dev.md#plugin-laundry-list);
* the [Coinbase plugin](../src/dali/plugin/input/rest/coinbase.py) can be used as an example for REST-based plugins, the [Trezor plugin](../src/dali/plugin/input/csv/trezor.py) for CSV-based plugins.

Also check [open issues](https://github.com/eprbell/dali-rp2/issues), or open a new one, if needed.

Finally read the question on [how to represent transactions for unsupported exchanges and wallets](#how-to-represent-transactions-from-unsupported-exchanges-and-wallets).

### How to Represent Transactions between Unsupported Exchanges and Wallets?
The [Manual data loader plugin](configuration_file.md#manual-section-csv) can be used for this purpose.

### How to Represent a Transaction from/to a Supported Exchange/Wallet to/from an Unsupported One?
The [Manual data loader plugin](configuration_file.md#manual-section-csv) can be used for this purpose.

### What if the Spot Price Is Missing for Some Transactions?
In some cases exchange reports don't have spot price information. In such situations spot price information can be retrieved automatically from Coinbase Pro by passing the `-s` option to DaLI. If the transaction belongs to one of the CSV files of the manual plugin, write `__unknown` as its spot price (and also use `-s`). If spot price information is still missing even after using `-s`, read about transaction resolution in the [Manual Plugin](configuration_file.md#manual-section-csv) section of the documentation.

### What if I and My Spouse File Taxes Jointly?
Suppose Alice and Bob are filing together and they both have a Coinbase account and a Trezor wallet each. They can configure 4 plugin sections in the configuration file:
* Coinbase / Bob;
* Coinbase / Alice;
* Trezor / Bob;
* Trezor / Alice.

See the [configuration file](configuration_file.md) section of the documentation for more details.

### What if a Transaction Is Generated Differently Than I Expect?
In certain cases DaLI doesn't know the user's intentions and it needs hints to generate a transaction correctly. For example an out transaction could be represented either as a partial intra transaction or as a normal out transaction (perhaps a gift to another person): only the user knows the correct meaning of the transaction. In such cases [transaction_hints](configuration_file.md#transaction-hints-section) in the configuration file can be used to solve the problem.

### What if a Transaction Is Generated With Some Fields Set to __UNKNOWN?
Such a transaction is called "unresolved" and occurs when DaLI doesn't have enough information to complete it. The user can provide the missing information using the Manual Plugin: read about transaction resolution (and how to fix such issues) in the [Manual Plugin](configuration_file.md#manual-section-csv) section of the documentation.

### How to Report a DaLI Bug Without Sharing Personal Information?
See the Reporting Bugs section in the [CONTRIBUTING](../CONTRIBUTING.md#reporting-bugs) document.

### What if I Don't Trust DaLI With My Crypto Data?
In other words, how to be sure DaLI is not malware/spyware? After all, Bitcoin's motto is *"don't trust, verify"*. DaLI is open-source and written in Python, so anybody with Python skills can inspect the code anytime: if DaLI were to try anything untoward, someone would likely notice. However if you don't have the time, patience or skill to verify the code and you don't trust others to do so for you, you can still use DaLI in an isolated environment (but this will limit its functionality to CSV-based plugins because REST-based ones need networking):
- start a fresh virtual machine with your OS of choice;
- install DaLI in the virtual machine;
- isolate the virtual machine: kill networking, shared directories and other mechanisms of outside communication;
- copy your crypto CSV files to the virtual machine via USB key or other physical medium (because the machine is now isolated);
- run DaLI in the virtual machine.

### Who is the Author of DaLI?
The author of DaLI is a Silicon Valley veteran, a software engineer and bitcoiner who also dabbles in Quantum Computing.

## Tax Questions

### What Events Are Taxable?
Selling, swapping, donating, mining, staking, earning cryptocurrency are some common taxable events. For an up-to-date list in any given year, ask your tax professional. For additional information on taxable events read the [Cryptocurrency Tax FAQ](https://www.reddit.com/r/CryptoTax/comments/re6jal/cryptocurrency_tax_faq/) on Reddit and
<!-- markdown-link-check-disable -->
[CoinTracker's summary on crypto taxes](https://www.cointracker.io/blog/what-tax-forms-should-crypto-holders-file).
<!-- markdown-link-check-enable-->

### Can I Avoid Paying Crypto Taxes?
No. The IRS has made it clear that [crypto taxes must be paid](https://www.irs.gov/newsroom/irs-reminds-taxpayers-to-report-virtual-currency-transactions).

### Which Resources Can I Use to Learn About Crypto Taxes?
A good starting point is the [Cryptocurrency Tax FAQ](https://www.reddit.com/r/CryptoTax/comments/re6jal/cryptocurrency_tax_faq/) on Reddit. Also read the RP2 FAQ question on [which tax forms to file](https://github.com/eprbell/rp2/blob/main/docs/user_faq.md#which-crypto-tax-forms-to-file) and consult with your tax professional.
