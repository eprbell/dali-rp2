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
  * [How to Represent Transactions from Unsupported Exchanges and Wallets?](#how-to-represent-transactions-from-unsupported-exchanges-and-wallets)
  * [How to Represent a Transaction from/to a Supported Exchange/Wallet to/from an Unsupported One](#how-to-represent-a-transaction-fromto-a-supported-exchangewallet-tofrom-an-unsupported-one)
  * [What if the Spot Price Is Missing for Some Transactions?](#what-if-the-spot-price-is-missing-for-some-transactions)
  * [What if I and My Spouse File Taxes Jointly?](#what-if-i-and-my-spouse-file-taxes-jointly)
  * [What if a Transaction Is Generated Differently Than I Expect?](#what-if-a-transaction-is-generated-differently-than-i-expect)
  * [How to Report a DaLI Bug Without Sharing Personal Information?](#how-to-report-a-dali-bug-without-sharing-personal-information)
  * [What if I Don't Trust DaLI With My Crypto Data?](#what-if-i-dont-trust-rp2-with-my-crypto-data)
  * [Who is the Author of DaLI?](#who-is-the-author-of-dali)

* **[Tax Questions](#tax-questions)**
  * [What Events Are Taxable?](#what-events-are-taxable)
  * [Can I Avoid Paying Crypto Taxes?](#can-i-avoid-paying-crypto-taxes)
  * [Which Resources Can I Use to Learn About Crypto Taxes?](#which-resources-can-i-use-to-learn-about-crypto-taxes)

## General Questions

### What Is the Timestamp Format?
Timestamp format is [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) (see [examples](https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations) of timestamps in this format). Note that RP2 requires full timestamps, including date, time and timezone.

### Can I Avoid Writing a Config File from Scratch?
You can use the [test_config.ini](config/test_config.ini) as a starting point and the [configuration file](configuration_file.md) documentation as reference.


### How to Represent Transactions from Unsupported Exchanges and Wallets?
The [Manual data loader plugin](configuration_file.md#manual-section-csv) can be used for this purpose.

### How to Represent a Transaction from/to a Supported Exchange/Wallet to/from an Unsupported One?
The [Manual data loader plugin](configuration_file.md#manual-section-csv) can be used for this purpose.

### What if the Spot Price Is Missing for Some Transactions?
In some cases exchange reports don't have spot price information. In such situations you can retrieve historical price data from the Web by passing the `-s` option to DaLI.

### What if I and My Spouse File Taxes Jointly?
Suppose Alice and Bob are filing together and they both have a Coinbase account and a Trezor wallet. They can configure 4 plugin sections in the configuration file:
* Coinbase / Bob
* Coinbase / Alice
* Trezor / Bob
* Trezor / Alice
See the [configuration file](configuration_file.md) section of the documentation for more details.

### What if a Transaction Is Generated Differently Than I Expect?
In certain cases DaLI doesn't know the user's intentions and it needs hints to generate a transaction correctly. For example an out transaction could be represented as a partial intra transaction or as a normal out transaction describing a gift to another person: only the user knows the correct representation. In such cases the [transaction_hints](configuration_file.md#transaction-hints-section) section of the configuration file can be used to resolve the problem.

### How to Report a DaLI Bug Without Sharing Personal Information?
See the Reporting Bugs section in the [CONTRIBUTING](../CONTRIBUTING.md#reporting-bugs) document.

### What if I Don't Trust DaLI With My Crypto Data?
In other words, how to be sure DaLI is not malware/spyware? After all, Bitcoin's motto is *"don't trust, verify"*. DaLI is open-source and written in Python, so anybody with Python skills can inspect the code anytime: if DaLI were to try anything untoward, someone would likely notice. However if you don't have the time, patience or skill to verify the code and you don't trust others to do so for you, you can still use DaLI in an isolated environment (but this will limit functionality to CSV-based plugins because REST-based ones need networking):
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
A good starting point is the [Cryptocurrency Tax FAQ](https://www.reddit.com/r/CryptoTax/comments/re6jal/cryptocurrency_tax_faq/) on Reddit. Also read the question on [which tax forms to file](#which-crypto-tax-forms-to-file) and consult with your tax professional.

