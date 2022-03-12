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

# The DaLI Configuration File

## Table of Contents
* **[Introduction](#introduction)**
* **[Data Loader Plugin Sections](#data-loader-plugin-sections)**
  * [Coinbase Section (REST)](#coinbase-section-rest)
  * [Coinbase Section Pro (REST)](#coinbase-pro-section-rest)
  * [Trezor Section (CSV)](#trezor-section-csv)
  * [Trezor Old Section (CSV)](#trezor-old-section-csv)
  * [Manual Section (CSV)](#manual-section-csv)
* **[Builtin Sections](#builtin-sections)**
  * [Transaction Hints Section](#transaction-hints-section)
  * [Header Sections](#header-sections)

## Introduction

The configuration file is in [INI format](https://en.wikipedia.org/wiki/INI_file) and it is used to initialize data loader plugins and configure DaLI's behavior. It contains a sequence of configuration sections, which are of two types:
* plugin sections: they select data loader plugins (e.g. Coinbase REST, Trezor CSV, etc.) to run and contain their initialization parameters;
* builtin sections: they configure general DaLI behavior (format of the output ODS, hints on how to generate certain transactions, etc.).

An example of a configuration file can be found in [test_config.ini](../config/test_config.ini). For instructions on how to run the example read the [Running](../README.md#running) section of the README.md file.

The example shows several concepts described in this document (see sections below for more details):
* [transaction hints](#transaction-hints-section): a transaction is recast from intra to out in the `transaction_hints` section of test_config.ini;
* multiple instances of the same plugin: test_config.ini has two trezor sections with different qualifiers and parameters (this captures two different trezor wallets);
* multiple people filing together: test_config.ini has a section for Alice's Trezor and another for Bob's Trezor;
* unsupported exchanges/wallets: the [manual](#manual-section-csv) section of test_config.ini points to [test_manual_in.csv](../input/test_manual_in.csv) containing buy transactions on FTX (which is not yet supported directly by DaLI);
* completing partial intra transactions using `unique_id`: the manual section of test_config.ini points to [test_manual_intra.csv](../input/test_manual_intra.csv), which contains two FTX intra transactions from/to Trezor. The Trezor side of the transaction is generated automatically by DaLI (because Trezor is supported), the FTX side has to be filled in by the user via the manual plugin. The unique_id field is used to pair the partial FTX transaction to the Trezor one: the DaLI transaction resolver will use these two transaction parts to generate a single complete intra transaction between Trezor and FTX.

## Data Loader Plugin Sections

A data loader plugin has the purpose of reading crypto data from a native source (CSV file or REST-based service). It is initialized with parameters from a plugin-specific section of the INI file. This section has the following format:
<pre>
[dali.plugin.input.<em>&lt;type&gt;</em>.<em>&lt;plugin&gt;</em> <em>&lt;qualifiers&gt;</em>]
<em>&lt;parameter_1&gt;</em> = <em>&lt;value_1&gt;</em>
...
<em>&lt;parameter_n&gt;</em> = <em>&lt;value_n&gt;</em>
</pre>

Where:
* *`<type>`* is one of: `csv` or `rest`, depending on plugin type;
* *`<plugin>`* is the name of the plugin;
* *`<qualifiers>`* is an optional sequence of words with the purpose of distinguishing different sections initializing the same plugin (e.g. two different Trezor wallets belonging to the same user, or two Coinbase accounts belonging to two people filing together);
* *`<parameter>`* and *`<value>`* are plugin-specific name-value pairs used to initialize a specific instance of the plugin. They are described in the plugin-specific sections below.

DaLI comes with a few builtin plugins, but more are needed: help us make DaLI a robust open-source, community-driven crypto data loader by [contributing](../CONTRIBUTING.md#contributing-to-the-repository) plugins for exchanges and wallets!

### Coinbase Section (REST)
This plugin is REST-based and requires setting up API Keys in your Coinbase account settings (click on the API link).

**IMPORTANT NOTE**:
* When setting up API key/secret, only use read permissions (DaLI does NOT need write permissions).
* store your API key and secret safely and NEVER share it with anyone!

Initialize this plugin section as follows:
<pre>
[dali.plugin.input.rest.coinbase <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
api_key = <em>&lt;api_key&gt;</em>
api_secret = <em>&lt;api_secret&gt;</em>
</pre>

### Coinbase Pro Section (REST)
This plugin is REST-based and requires setting up API Keys in your Coinbase Pro account settings (click on the API link).

**IMPORTANT NOTE**:
* When setting up API key/secret/passphrase, only use read permissions (DaLI does NOT need write permissions).
* store your API key, secret and passphrase safely and NEVER share it with anyone!

Initialize this plugin section as follows:
<pre>
[dali.plugin.input.rest.coinbase_pro <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
api_key = <em>&lt;api_key&gt;</em>
api_secret = <em>&lt;api_secret&gt;</em>
api_passphrase = <em>&lt;api_passphrase&gt;</em>
</pre>

### Trezor Section (CSV)
This plugin is CSV-based and parses CSV files generated by Trezor Suite. Initialize it as follows:
<pre>
[dali.plugin.input.csv.trezor <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
account_nickname = <em>&lt;account_nickname&gt;</em>
currency = <em>&lt;currency&gt;</em>
timezone = <em>&lt;timezone&gt;</em>
csv_file = <em>&lt;csv_file&gt;</em>
</pre>

Notes:
* account_nickname is a user-selected identifier for the account;
* currency is the cryptocurrency the account is denominated in;
* timezone is the string representation of the timezone where the transactions occurred (Trezor doesn't provide this information in the CSV file), e.g.: `America/Los_Angeles` or `US/Pacific`.

### Trezor Old Section (CSV)
This plugin is CSV-based and parses older CSV files generated by the Trezor web interface (only use this if the regular Trezor plugin doesn't work). Initialize it as follows:
<pre>
[dali.plugin.input.csv.trezor_old <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
account_nickname = <em>&lt;account_nickname&gt;</em>
currency = <em>&lt;currency&gt;</em>
timezone = <em>&lt;timezone&gt;</em>
csv_file = <em>&lt;csv_file&gt;</em>
</pre>

Notes:
* account_nickname is a user-selected identifier for the account;
* currency is the cryptocurrency the account is denominated in;
* timezone is the string representation of the timezone where the transactions occurred (Trezor doesn't provide this information in the CSV file), e.g.: `America/Los_Angeles` or `US/Pacific`.

### Manual Section (CSV)
The manual CSV plugin is used to describe partial or complete transactions for exchanges and wallets that are not yet supported by DaLI (more on partial transactions below). Initialize it as follows:
<pre>
[dali.plugin.input.csv.manual <em>&lt;qualifiers&gt;</em>]
in_csv_file = <em>&lt;in_csv_file&gt;</em>
out_csv_file = <em>&lt;out_csv_file&gt;</em>
intra_csv_file = <em>&lt;intra_csv_file&gt;</em>
</pre>

The in_csv_file contains transactions describing crypto being acquired. Line 1 is considered a header line and it's ignored. Subsequent lines have the following format:
* unique_id (optional): unique identifier for the transaction. It's only useful to match partial intra transactions (see below) and it can be omitted in other cases;
* timestamp: time at which the transaction occurred. DaLI can parse most timestamp formats, but timestamps must always include: year, month, day, hour, minute, second and timezone (milliseconds are optional). E.g.: "2020-01-21 11:15:00+00:00";
* asset: which cryptocurrency was transacted (e.g. BTC, ETH, etc.);
* exchange: exchange or wallet on which the transaction occurred;
* holder: exchange account or wallet owner;
* transaction_type: AIRDROP, BUY, DONATE, GIFT, HARDFORK, INCOME, INTEREST, MINING, STAKING or WAGES;
* spot_price: value of 1 unit of the given cryptocurrency at the time the transaction occurred;
* crypto_in: how much of the given cryptocurrency was acquired with the transaction;
* fiat_fee: fiat value of the transaction fee;
* fiat_in_no_fee (optional): fiat value of the transaction without fee;
* fiat_in_with_fee (optional): fiat value of the transaction with fee;
* notes (optional): user-provided description of the transaction.

The out_csv_file contains transactions describing crypto being disposed of. Line 1 is considered a header line and it's ignored. Subsequent lines have the following format:
* unique_id (optional): unique identifier for the transaction. It's only useful to match partial intra transactions (see below) and it can be omitted in other cases;
* timestamp: time at which the transaction occurred. DaLI can parse most timestamp formats, but timestamps must always include: year, month, day, hour, minute, second and timezone (milliseconds are optional). E.g.: "2020-01-21 11:15:00+00:00";
* asset: which cryptocurrency was transacted (e.g. BTC, ETH, etc.);
* exchange: exchange or wallet on which the transaction occurred;
* holder: exchange account or wallet owner;
* transaction_type: DONATE, GIFT or SELL;
* spot_price: value of 1 unit of the given cryptocurrency at the time the transaction occurred;
* crypto_out_no_fee: how much of the given cryptocurrency was sold or sent with the transaction (excluding fee);
* crypto_fee: crypto value of the transaction fee;
* crypto_out_with_fee (optional): how much of the given cryptocurrency was sold or sent with the transaction (including fee);
* fiat_out_no_fee (optional): fiat value of the transaction without fee;
* fiat_fee (optional): fiat value of the transaction fee;
* notes (optional): user-provided description of the transaction.

The intra_csv_file contains transactions describing crypto being moved across accounts controlled by the same user (or people filing together). Line 1 is considered a header line and it's ignored. Subsequent lines have the following format:
* unique_id: unique identifier for the transaction. It's only useful to match partial intra transactions (see below) and it can be omitted in other cases;
* timestamp: time at which the transaction occurred. DaLI can parse most timestamp formats, but timestamps must always include: year, month, day, hour, minute, second and timezone (milliseconds are optional). E.g.: "2020-01-21 11:15:00+00:00";
* asset: which cryptocurrency was transacted (e.g. BTC, ETH, etc.);
* from_exchange (optional): exchange or wallet from which the transfer of cryptocurrency occurred;
* from_holder (optional): owner of the exchange account or wallet from which the transfer of cryptocurrency occurred;
* to_exchange (optional): exchange or wallet to which the transfer of cryptocurrency occurred;
* to_holder (optional): owner of the exchange account or wallet to which the transfer of cryptocurrency occurred;
* spot_price (optional): value of 1 unit of the given cryptocurrency at the time the transaction occurred;
* crypto_sent (optional): how much of the given cryptocurrency was sent with the transaction;
* crypto_received (optional): how much of the given cryptocurrency was received with the transaction;
* notes (optional): user-provided description of the transaction.

Note that empty (separator) lines are allowed in all three CSV files to increase readability.

The manual CSV plugin is typically used for two purposes:
* add complete in/out/intra transactions for exchanges or wallets that are not yet supported by DaLI;
* add partial in/out/intra transactions for exchanges or wallets that are not yet supported by DaLI.

Partial transactions are used when an intra transaction is sent from a wallet or exchange supported by DaLI and is received at a wallet or exchange that is not yet supported by DaLI or viceversa. This causes DaLI to generate an incomplete transaction.

For the case of supported origin and unsupported destination, DaLI generates:
1. a partial intra transaction with defined `from_exchange`/`from_holder`/`crypto_sent` and empty `to_exchange`/`to_holder`/`crypto_received` or
2. a full out transaction (modeling the source side of the intra transaction)

For the case of unsupported origin and supported destination, DaLI generates:
3. a partial intra transaction with empty `from_exchange`/`from_holder`/`crypto_sent` and defined `to_exchange`/`to_holder`/`crypto_received` or
4. a full in transaction (modeling the destination side of the intra transaction)

The manual CSV allows users to complete the partially transactions generated by DaLI:
* in case 1 the user adds to the *`intra_csv_file`* a partial intra transaction with empty `from_exchange`/`from_holder`/`crypto_sent` and defined `to_exchange`/`to_holder`/`crypto_received`;
* in case 2 the user adds an in_transaction to *`in_csv_file`* (modeling the destination side of the intra transaction). Transaction type can be omitted.
* in case 3 the user adds to the *`intra_csv_file`* a partial intra transaction with defined `from_exchange`/`from_holder`/`crypto_sent` and empty `to_exchange`/`to_holder`/`crypto_received` to the *`intra_csv_file`*;
* in case 4 the user adds an out_transaction to *`out_csv_file`* (modeling the source side of the intra transaction). Transaction type can be omitted.

In order to match up and join pairs of partial transactions, DaLI uses the `unique_id` field, which is critical to data merging: when adding a partial transaction to a manual CSV file, make sure its `unique_id` matches the one of the generated partial transaction you want to complete. Full transactions don't need `unique_id`.

**Example 1)**

If DaLI generates the following partial intra transaction:
unique_id   |timestamp                 |asset|from_exchange|from_holder|to_exchange|to_holder|spot_price|crypto_sent|crypto_received
------------|--------------------------|-----|-------------|-----------|-----------|---------|----------|-----------|---------------
389ded74b35f|2020-03-01 10:45:23 +00:00|BTC  |Coinbase     |Alice      |-          |-        |15100     |0.5        |-

The user can complete it by adding the following partial transaction to the *`intra_csv_file`* to model the missing half of the generated transaction (note that the two `unique_id` fields match):
unique_id   |timestamp                 |asset|from_exchange|from_holder|to_exchange|to_holder|spot_price|crypto_sent|crypto_received
------------|--------------------------|-----|-------------|-----------|-----------|---------|----------|-----------|---------------
389ded74b35f|2020-03-01 11:25:18 +00:00|BTC  |-            |-          |FTX        |Alice    |-         |-          |0.49

The next time DaLI is run with the new information in the *`intra_csv_file`*, it will join the two partial transactions and generate a full intra transaction:
unique_id   |timestamp                 |asset|from_exchange|from_holder|to_exchange|to_holder|spot_price|crypto_sent|crypto_received
------------|--------------------------|-----|-------------|-----------|-----------|---------|----------|-----------|---------------
389ded74b35f|2020-03-01 11:25:18 +00:00|BTC  |Coinbase     |Alice      |FTX        |Alice    |15100     |0.5        |0.49

**Example 2)**

If DaLI generates the following incomplete out transaction:
unique_id   |timestamp                 |asset|exchange|holder|transaction_type|spot_price|crypto_out_no_fee|crypto_fee
------------|--------------------------|-----|--------|------|----------------|----------|-----------------|----------
389ded74b35f|2020-03-01 10:45:23 +00:00|BTC  |Coinbase|Alice |-               |15100     |0.5              |0.01

The user can complete it by adding the following partial transaction to the *`in_csv_file`* to model the missing half of the generated transaction (note that the two `unique_id` fields match):
unique_id   |timestamp                 |asset|exchange|holder|transaction_type|spot_price|crypto_out_no_fee|crypto_fee
------------|--------------------------|-----|--------|------|----------------|----------|-----------------|----------
389ded74b35f|2020-03-01 11:25:18 +00:00|BTC  |FTX     |Alice |-               |15100     |0.49             |0.01

The next time DaLI is run with the new information in the *`in_csv_file`*, it will join the two partial transactions and generate a full intra transaction:
unique_id   |timestamp                 |asset|from_exchange|from_holder|to_exchange|to_holder|spot_price|crypto_sent|crypto_received
------------|--------------------------|-----|-------------|-----------|-----------|---------|----------|-----------|---------------
389ded74b35f|2020-03-01 11:25:18 +00:00|BTC  |Coinbase     |Alice      |FTX        |Alice    |15100     |0.5        |0.49

## Builtin Sections
Builtin sections are used as global configuration of DaLI's behavior.

### Transaction Hints Section
The transaction_hints section is optional and is used to force a transaction to be generated as in, out or intra. This is useful because DaLI doesn't know the user's intentions and in certain cases it needs hints from the user. For example an out transaction could be represented either as a partial intra transaction (as described above) or as a normal out transaction (perhaps a gift to another person): only the user knows the correct meaning of the transaction. In such cases a transaction_hint line can be used to clear the ambiguity.

The format of this section is as follows:
<pre>
[transaction_hints]
<em>&lt;unique_id_1&gt;</em> = <em>&lt;direction&gt;</em>:<em>&lt;transaction_type&gt;</em>:<em>&lt;notes&gt;</em>
...
<em>&lt;unique_id_n&gt;</em> = <em>&lt;direction&gt;</em>:<em>&lt;transaction_type&gt;</em>:<em>&lt;notes&gt;</em>
</pre>

Where:
* *`unique_id`* is the unique_id of the transaction that needs to be recast;
* *`direction`* denotes how to represent the transaction in generated file and it's one of: `in`, `out` or `intra`;
* *`transaction_type`* is the transaction type (see [Manual Section (CSV)](#manual-section-csv) for more details on this field);
* *`notes`* is an optional English comment.

### Header Sections
There are three header sections (all of which are optional):
* in_header
* out_header
* intra_header

They are used to describe the format of the generated output file and are populated with *`<field> = <column_index>`* assignments (the fields in each section are the same ones described in [Manual Section (CSV)](#trezor-section-csv)): this assigns a column index to each field in the output file. Note that two different fields cannot have the same column index in the same section.

For example:
<pre>
[in_header]
timestamp = 0
asset = 1
exchange = 2
holder = 3
transaction_type = 4
spot_price = 6
crypto_in = 7
fiat_fee = 8
fiat_in_no_fee = 9
fiat_in_with_fee = 10
unique_id = 12
notes = 13

[out_header]
timestamp = 0
asset = 1
exchange = 2
holder = 3
transaction_type = 4
spot_price = 6
crypto_out_no_fee = 7
crypto_fee = 8
crypto_out_with_fee = 9
fiat_out_no_fee = 10
fiat_fee = 11
unique_id = 12
notes = 13

[in_header]
timestamp = 0
asset = 1
from_exchange = 2
from_holder = 3
to_exchange = 4
to_holder = 5
spot_price = 6
crypto_sent = 7
crypto_received = 8
unique_id = 12
notes = 13
</pre>
