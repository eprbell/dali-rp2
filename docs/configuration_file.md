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
  * [Binance.com Section (REST)](#binance-com-section-rest)
  * [Bitbank Section (REST)](#bitbank-section-rest)
  * [Coinbase Section (REST)](#coinbase-section-rest)
  * [Coinbase Pro Section (REST)](#coinbase-pro-section-rest)
  * [Binance.com Supplemental Section (CSV)](#binance.com-supplemental-section-csv)
  * [Bitbank Supplemental Section (CSV)](#bitbank-supplemental-section-csv)
  * [Coincheck Supplemental Section (CSV)](#coincheck-supplemental-section-csv)
  * [Ledger Section (CSV)](#ledger-section-csv)
  * [Pionex Section (CSV)](#pionex-section-csv)
  * [Trezor Section (CSV)](#trezor-section-csv)
  * [Trezor Old Section (CSV)](#trezor-old-section-csv)
  * [Manual Section (CSV)](#manual-section-csv)
* **[Pair Converter Plugin Sections](#pair-converter-plugin-sections)**
  * [CCXT](#ccxt)
  * [Binance Locked CCXT](#binance-locked-ccxt)
  * [Kraken Locked CCXT](#kraken-locked-ccxt)
  * [Historic Crypto](#historic-crypto)
* **[Builtin Sections](#builtin-sections)**
  * [Transaction Hints Section](#transaction-hints-section)
  * [Header Sections](#header-sections)
  * [Historical Market Data Section](#historical-market-data-section)

## Introduction

The configuration file is in [INI format](https://en.wikipedia.org/wiki/INI_file) and it is used to initialize data loader and pair converter plugins and to configure DaLI's behavior. It contains a sequence of configuration sections, which are of the following types:
* builtin sections: they configure general DaLI behavior (format of the output ODS file, hints on how to generate certain transactions, etc.);
* data loader plugin sections: they select data loader plugins to run (e.g. Coinbase REST, Trezor CSV, etc.) and contain their initialization parameters;
* pair converter plugin sections: they are optional and select pair converter plugins to use for filling missing spot price and converting foreign fiat to native fiat (e.g. USD for US, JPY for Japan, etc.).

Look at [test_config.ini](../config/test_config.ini) for an example of a configuration file. For instructions on how to run the example read the [Running](../README.md#running) section of the README.md file.

The example shows several concepts described in this document (see sections below for more details):
* [transaction hints](#transaction-hints-section): a transaction is recast from intra to out in the `transaction_hints` section of test_config.ini;
* multiple instances of the same plugin: test_config.ini has two Trezor sections with different qualifiers and parameters (this captures two different Trezor wallets);
* multiple people filing together: test_config.ini has a section for Alice's Trezor and another for Bob's Trezor;
* unsupported exchanges/wallets: the [manual](#manual-section-csv) section of test_config.ini points to [test_manual_in.csv](../input/test_manual_in.csv) containing buy transactions on FTX (which is not yet supported directly by DaLI);
* completing partial intra transactions using `unique_id`: the manual section of test_config.ini points to [test_manual_intra.csv](../input/test_manual_intra.csv), which contains two FTX intra transactions from/to Trezor. The Trezor side of the transaction is generated automatically by DaLI (because Trezor is supported), the FTX side has to be filled in by the user via the manual plugin. The `unique_id` field is used to pair the partial FTX transaction to the Trezor one: the DaLI transaction resolver will use these two transaction parts to generate a single complete intra transaction between Trezor and FTX.

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

### Binance.com Section (REST)
This plugin is REST-based and requires setting up API Keys in your Binance.com account settings (click on the API Management link under your profile).

**IMPORTANT NOTE**:
* When setting up API key/secret, only use read permissions (DaLI does NOT need write permissions).
* store your API key and secret safely and NEVER share it with anyone!

Initialize this plugin section as follows:
<pre>
[dali.plugin.input.rest.binance_com <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
api_key = <em>&lt;api_key&gt;</em>
api_secret = <em>&lt;api_secret&gt;</em>
username = <em>&lt;username&gt;</em>
native_fiat = <em>&lt;native_fiat&gt;</em>
</pre>

Notes:
* The `username` parameter is optional and denotes the username used when connecting to the Binance.com mining pool. Only basic mining deposits (type 2) are currently supported.
* On May 8th, 2021, Binance.com implemented a new unified dividend endpoint. This endpoint returns the time when interest affected a user's wallet. However, the endpoint used to retrieve interest payments previously returns the time when the interest was delivered (but hadn't yet affected the user's wallet). [See this post for more details](https://dev.binance.vision/t/time-difference-between-sapi-v1-asset-assetdividend-and-sapi-v1-staking-stakingrecord/12346/2).
* Due to this discrepency, there may be duplicate locked savings/staked and flexible savings dividends around May 8th, 2021. Please review your payments around this time before processing your transactions with RP2.
* [Currently only dust 'dribblets' of 100 or less crypto assets can be retrieved at once](https://dev.binance.vision/t/pagination-for-dustlog-asset-dividend-record-swap-history-bswap/4963). If you dust more than 100 crypto assets at one time the REST API will not be able to process the transactions successfully.
* Due to the information not being available via REST, autoinvest trades and ETH to BETH conversions can not be processed with this plugin. Please download the CSV for these transactions and use the [Binance.com Supplemental CSV plugin](#binance.com-supplemental-section-csv). Note that some files are only available as .xlsx will need to be converted to the CSV format to be processed. BETH/ETH trades are not affected by this limitation and will be read via the REST plugin.

### Bitbank Section (REST)
This plugin is REST-based and requires setting up API Keys in your Bitbank account settings (click on the API link under your profile).

**IMPORTANT NOTE**:
* When setting up API key/secret, only use 参照 (reference) 権限 (authority) (DaLI does NOT need 取引 (transaction) or 出勤 (withdrawal) authority).
* store your API key and secret safely and NEVER share it with anyone!

Initialize this plugin section as follows:
<pre>
[dali.plugin.input.rest.binance_com <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
api_key = <em>&lt;api_key&gt;</em>
api_secret = <em>&lt;api_secret&gt;</em>
username = <em>&lt;username&gt;</em>
native_fiat = <em>&lt;native_fiat&gt;</em>
thread_count = <em>&lt;thread_count&gt;</em>
</pre>

Note: the `thread_count` parameter is optional and is currently not implemented.

Notes:
* At this time, the Bitbank REST API only supports trades. To process deposits and withdrawals please use the [Bitbank Supplemental CSV plugin](#bitbank-supplemental-section-csv).

### Coinbase Section (REST)
This plugin is REST-based and requires setting up API Keys in your Coinbase account settings (click on the API link).

**IMPORTANT NOTE**:
* when setting up API key/secret, only use read permissions (DaLI does NOT need write permissions);
* store your API key and secret safely and NEVER share it with anyone!

Initialize this plugin section as follows:
<pre>
[dali.plugin.input.rest.coinbase <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
api_key = <em>&lt;api_key&gt;</em>
api_secret = <em>&lt;api_secret&gt;</em>
thread_count = <em>&lt;thread_count&gt;</em>
</pre>

Note: the `thread_count` parameter is optional and denotes the number of parallel threads used to by the plugin to connect to the endpoint. The higher this number, the faster the execution, however if the number is too high the server may interrupt the connection with a rate-limit error.

### Coinbase Pro Section (REST)
This plugin is REST-based and requires setting up API Keys in your Coinbase Pro account settings (click on the API link).

**IMPORTANT NOTE**:
* when setting up API key/secret/passphrase, only use read permissions (DaLI does NOT need write permissions);
* store your API key, secret and passphrase safely and NEVER share it with anyone!

Initialize this plugin section as follows:
<pre>
[dali.plugin.input.rest.coinbase_pro <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
api_key = <em>&lt;api_key&gt;</em>
api_secret = <em>&lt;api_secret&gt;</em>
api_passphrase = <em>&lt;api_passphrase&gt;</em>
thread_count = <em>&lt;thread_count&gt;</em>
</pre>

Note: the `thread_count` parameter is optional and denotes the number of parallel threads used to by the plugin to connect to the endpoint. The higher this number, the faster the execution, however if the number is too high the server may interrupt the connection with a rate-limit error.

### Binance.com Supplemental Section (CSV)
This plugin is CSV-based and parses CSV files generated by Binance.com. It only supports autoinvest purchases and ETH to BETH conversions, which are not covered by the REST API. Initialize it as follows:
<pre>
[dali.plugin.input.csv.binance_com <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
autoinvest_csv_file = <em>&lt;autoinvest_csv_file&gt;</em>
betheth_csv_file = <em>&lt;betheth_csv_file&gt;</em>
native_fiat = <em>&lt;native_fiat&gt;</em>
</pre>

Notes:
* Both `autoinvest_csv_file` and `betheth_csv_file` are optional.
* `autoinvest_csv_file` can be retrieved by clicking [Wallet] -> [Earn] -> [History] -> [auto-invest] -> [export]
* `betheth_csv_file` can be retrieved by clicking [Wallet] -> [Earn] -> [History] -> [ETH 2.0 Staking] -> [export]
* You can currently only retrieve 6 months of data at one time, 5 times a month.

### Bitbank Supplemental Section (CSV)
This plugin is CSV-based and parses CSV files generated by Bitbank. It only supports withdrawals, which are not covered by the REST API. Initialize it as follows:
<pre>
[dali.plugin.input.csv.binance_com <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
withdrawals_csv_file = <em>&lt;withdrawals_csv_file&gt;</em>
withdrawal_code = <em>&lt;withdrawal_code&gt;</em>
deposits_csv_file = <em>&lt;deposits_csv_file&gt;</em>
deposits_code = <em>&lt;deposits_code&gt;</em>
native_fiat = <em>&lt;native_fiat&gt;</em>
</pre>

Notes:
* You do not have to declare both a withdrawal and deposit file. However, declaring a `withdrawals_csv_file` or `deposits_csv_file` without a corresponding code will trigger an exception.
* `withdrawals_csv_file` can be retrieved by clicking [出金 (withdrawals)] -> [出金 button to the right of the asset] -> [CSVドウンロード (CSV download)]
* `withdrawal_code` is the code of the crypto asset or fiat (JPY) that was withdrawn. The csv does not include any information about what asset was withdrawn.
* `deposits_csv_file` can be retrieved by clicking [入金 (deposits)] -> [入金 button to the right of the asset] -> [CSVドウンロード (CSV download)]
* `deposits_code` is the code of the crypto asset or fiat (JPY) that was deposited. The csv does not include any information about what asset was deposited.

### Coincheck Supplemental Section (CSV)
This plugin is CSV-based and parses CSV files generated by Coincheck. It only supports buys from the marketplace, which are not covered by the REST API. Initialize it as follows:
<pre>
[dali.plugin.input.csv.binance_com <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
buys_csv_file = <em>&lt;buys_csv_file&gt;</em>
native_fiat = <em>&lt;native_fiat&gt;</em>
</pre>

Notes:
* `buys_csv_file` can be retrieved by clicking [Marketplace (Buy)] -> (Scroll down to the bottom) -> Click on the [🔻] to the right of [Coin purchase history] -> Select [Export to CSV]
* Transfers are not exportable. They will have to be manually added to [transaction hints](https://github.com/eprbell/dali-rp2/blob/main/docs/configuration_file.md#transaction-hints-section)
* This plugin currently only supports Marketplace Buys. If you need support
for sells, please [open an issue](https://github.com/eprbell/dali-rp2/issues).
* The Coincheck REST API only supports BTC trades, withdrawals, and deposits and there are currently no plans to implement it.

### Ledger Section (CSV)
This plugin is CSV-based and parses CSV files generated by Ledger Live. Initialize it as follows:
<pre>
[dali.plugin.input.csv.ledger <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
account_nickname = <em>&lt;account_nickname&gt;</em>
csv_file = <em>&lt;csv_file&gt;</em>
</pre>

Notes:
* `account_nickname` is a user-selected identifier for the account;

### Nexo (CSV)
This plugin is CSV-based and parses CSV files generated by Nexo. Initialize it as follows:

```
[dali.plugin.input.csv.nexo <qualifiers>]
account_holder = <account_holder>
account_nickname = <account_nickname>
transaction_csv_file = <csv_file>
```
Notes:

* Locking or unlocking transactions are skipped. They are internal to Nexo.

### Pionex Section (CSV)
This plugin is CSV-based and parses CSV files generated by Pionex. It currently supports trades, deposits and withdrawals. Initialize it as follows:
<pre>
[dali.plugin.input.csv.binance_com <em>&lt;qualifiers&gt;</em>]
account_holder = <em>&lt;account_holder&gt;</em>
trades_csv_file = <em>&lt;trades_csv_file&gt;</em>
transfers_csv_file = <em>&lt;transfers_csv_file&gt;</em>
native_fiat = <em>&lt;native_fiat&gt;</em>
</pre>

Notes:
* The `trades_csv_file` and `transfers_csv_file` must be extracted from the .xlsx that is downloadable from the app. How to export transaction history is detailed [here](https://www.pionex.com/blog/how-to-export-pionex-transaction-history-statement%E3%80%90app-version%E3%80%91/).
* The sheet from the exported .xlsx labeled "for-cointracker" needs to be saved seperately as a CSV. This is used as the `trades_csv_file`.
* The sheet from the exported .xlsx labeled "depositwithdraw" needs to be saved seperately as a CSV. This is used as the `transfers_csv_file`.

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
* `account_nickname` is a user-selected identifier for the account;
* `currency` is the cryptocurrency the account is denominated in;
* `timezone` is the string representation of the timezone where the transactions occurred (Trezor doesn't provide this information in the CSV file), e.g.: `America/Los_Angeles` or `US/Pacific`.

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

The `in_csv_file` contains transactions describing crypto being acquired. Line 1 is considered a header line and it's ignored. Subsequent lines have the following format:
* `unique_id` (optional): unique identifier for the transaction. It's useful to match partial intra transactions (see below) and it can be omitted in other cases;
* `timestamp`: [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) format time at which the transaction occurred. Timestamps must always include: year, month, day, hour, minute, second and timezone (milliseconds are optional). E.g.: "2020-01-21 11:15:00+00:00";
* `asset`: which cryptocurrency was transacted (e.g. BTC, ETH, etc.);
* `exchange`: exchange or wallet on which the transaction occurred;
* `holder`: exchange account or wallet owner;
* `transaction_type`: AIRDROP, BUY, DONATE, GIFT, HARDFORK, INCOME, INTEREST, MINING, STAKING or WAGES;
* `spot_price`: value of 1 unit of the given cryptocurrency at the time the transaction occurred; If the value is unavailable, to direct DaLI to read it from Internet historical data, write in `__unknown` and use the `-s` command line switch;
* `crypto_in`: how much of the given cryptocurrency was acquired with the transaction (without fee);
* `crypto_fee` (optional): transaction fee (if it was paid in crypto). This is mutually exclusive with `fiat_fee`;
* `fiat_in_no_fee` (optional): fiat value of the transaction without fee;
* `fiat_in_with_fee` (optional): fiat value of the transaction with fee;
* `fiat_fee` (optional): transaction fee (if it was paid in fiat). This is mutually exclusive with `crypto_fee`;
* `notes` (optional): user-provided description of the transaction.

The `out_csv_file` contains transactions describing crypto being disposed of. Line 1 is considered a header line and it's ignored. Subsequent lines have the following format:
* `unique_id` (optional): unique identifier for the transaction. It's useful to match partial intra transactions (see below) and it can be omitted in other cases;
* `timestamp`: [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) format time at which the transaction occurred. Timestamps must always include: year, month, day, hour, minute, second and timezone (milliseconds are optional). E.g.: "2020-01-21 11:15:00+00:00";
* `asset`: which cryptocurrency was transacted (e.g. BTC, ETH, etc.);
* `exchange`: exchange or wallet on which the transaction occurred;
* `holder`: exchange account or wallet owner;
* `transaction_type`: DONATE, GIFT or SELL;
* `spot_price`: value of 1 unit of the given cryptocurrency at the time the transaction occurred; If the value is unavailable, to direct DaLI to read it from Internet historical data, write in `__unknown` and use the `-s` command line switch;
* `crypto_out_no_fee`: how much of the given cryptocurrency was sold or sent with the transaction (excluding fee);
* `crypto_fee`: crypto value of the transaction fee;
* `crypto_out_with_fee` (optional): how much of the given cryptocurrency was sold or sent with the transaction (including fee);
* `fiat_out_no_fee` (optional): fiat value of the transaction without fee;
* `fiat_fee` (optional): fiat value of the transaction fee;
* `notes` (optional): user-provided description of the transaction.

The `intra_csv_file` contains transactions describing crypto being moved across accounts controlled by the same user (or people filing together). Line 1 is considered a header line and it's ignored. Subsequent lines have the following format:
* `unique_id`: unique identifier for the transaction. It's useful to match partial intra transactions (see below) and it can be omitted in other cases;
* `timestamp`: [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) format time at which the transaction occurred. Timestamps must always include: year, month, day, hour, minute, second and timezone (milliseconds are optional). E.g.: "2020-01-21 11:15:00+00:00";
* `asset`: which cryptocurrency was transacted (e.g. BTC, ETH, etc.);
* `from_exchange` (optional): exchange or wallet from which the transfer of cryptocurrency occurred;
* `from_holder` (optional): owner of the exchange account or wallet from which the transfer of cryptocurrency occurred;
* `to_exchange` (optional): exchange or wallet to which the transfer of cryptocurrency occurred;
* `to_holder` (optional): owner of the exchange account or wallet to which the transfer of cryptocurrency occurred;
* `spot_price` (optional): value of 1 unit of the given cryptocurrency at the time the transaction occurred; If the value is unavailable, to direct DaLI to read it from Internet historical data, write in `__unknown` and use the `-s` command line switch;
* `crypto_sent` (optional): how much of the given cryptocurrency was sent with the transaction;
* `crypto_received` (optional): how much of the given cryptocurrency was received with the transaction;
* `notes` (optional): user-provided description of the transaction.

Note that empty (separator) lines are allowed in all three CSV files to increase readability.

#### Partial Transactions and Transaction Resolution
The manual CSV plugin is typically used for two purposes:
* add complete in/out/intra transactions for exchanges or wallets that are not yet supported by DaLI;
* add partial in/out/intra transactions for exchanges or wallets that are not yet supported by DaLI.

Partial transactions occur when an intra transaction is sent from a wallet or exchange supported by DaLI and is received at a wallet or exchange that is not yet supported by DaLI or viceversa. This causes DaLI to generate an partial transaction. Partial transactions can be identified in the generated ODS file, because they have some fields marked with `__unknown`.

For the case of supported origin and unsupported destination, DaLI models the source side of the transaction by generating either:
1. a partial intra transaction with defined `from_exchange`/`from_holder`/`crypto_sent` and empty `to_exchange`/`to_holder`/`crypto_received` or
2. an out transaction.

For the case of unsupported origin and supported destination, DaLI models the destination side of the transaction by generating either:
3. a partial intra transaction with empty `from_exchange`/`from_holder`/`crypto_sent` and defined `to_exchange`/`to_holder`/`crypto_received` or
4. a in transaction.

The manual CSV allows users to complete the partial transactions generated by DaLI:
* in case 1 the user adds to the *`intra_csv_file`* a partial intra transaction with empty `from_exchange`/`from_holder`/`crypto_sent` and defined `to_exchange`/`to_holder`/`crypto_received`;
* in case 2 the user adds an in_transaction to *`in_csv_file`* (modeling the destination side of the intra transaction). Transaction type can be omitted.
* in case 3 the user adds to the *`intra_csv_file`* a partial intra transaction with defined `from_exchange`/`from_holder`/`crypto_sent` and empty `to_exchange`/`to_holder`/`crypto_received` to the *`intra_csv_file`*;
* in case 4 the user adds an out_transaction to *`out_csv_file`* (modeling the source side of the intra transaction). Transaction type can be omitted.

In order to match up and join pairs of partial transactions, DaLI uses the `unique_id` field, which is critical to data merging: when adding a partial transaction to a manual CSV file, make sure its `unique_id` matches the one of the generated partial transaction you want to complete.

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

## Pair Converter Plugin Sections
A pair converter plugin has the purpose of converting a currency to another (both crypto and fiat) and it is used to fill missing spot price and convert foreign fiat to native fiat (e.g. USD for US, JPY for Japan, etc.). It is initialized with parameters from a plugin-specific section of the INI file. This section has the following format:
<pre>
[dali.plugin.pair_converter.<em>&lt;plugin&gt;</em>]
<em>&lt;parameter_1&gt;</em> = <em>&lt;value_1&gt;</em>
...
<em>&lt;parameter_n&gt;</em> = <em>&lt;value_n&gt;</em>
</pre>
Where:
* *`<parameter>`* and *`<value>`* are plugin-specific name-value pairs used to initialize a specific instance of the plugin. They are described in the plugin-specific sections below.

The order in which pair converter sections are defined in the configuration file denotes the priority used by DaLI when looking for price data: it starts by querying the first pair converter in the configuration file, if it doesn't find a result it queries the second, and so on.

Pair converters are optional: if they are missing from the configuration file DaLI uses a default pair converter list.

### CCXT
This plugin is based on the CCXT Python library.

Initialize this plugin section as follows:
<pre>
[dali.plugin.pair_converter.ccxt</em>]
historical_price_type = <em>&lt;historical_price_type&gt;</em>
default_exchange = <em>&lt;default_exchange&gt;</em>
fiat_priority = <em>&lt;fiat_priority&gt;</em>
google_api_key = <em>&lt;google_api_key&gt;</em>
</pre>

Where:
* `<historical_price_type>` is one of `open`, `high`, `low`, `close`, `nearest`. When DaLi downloads historical market data, it captures a `bar` of data surrounding the timestamp of the transaction. Each bar has a starting timestamp, an ending timestamp, and OHLC prices. You can choose which price to select for price lookups. The open, high, low, and close prices are self-explanatory. The `nearest` price is either the open price or the close price of the bar depending on whether the transaction time is nearer the bar starting time or the bar ending time.
* `default_exchange` is an optional string for the name of an exchange to use if the exchange listed in a transaction is not currently supported by the CCXT plugin. If no default is set, Kraken(US) is used. If you would like an exchange added please open an issue. The current available exchanges are "Binance.com", "Gate", "Huobi" and "Kraken".
* `fiat_priority` is an optional list of strings in JSON format (e.g. `["_1stpriority_", "_2ndpriority_"...]`) that ranks the priority of fiat in the routing system. If no `fiat_priority` is given, the default priority is USD, JPY, KRW, EUR, GBP, AUD, which is based on the volume of the fiat market paired with BTC (ie. BTC/USD has the highest worldwide volume, then BTC/JPY, etc.).
* `google_api_key` is an optional string for the Google API Key that is needed by some CSV readers, most notably the Kraken CSV reader. It is used to download the OHLCV files for a market. No data is ever sent to Google Drive. This is only used to retrieve data. To get a Google API Key, visit the [Google Console Page](https://console.developers.google.com/) and setup a new project. Be sure to enable the Google Drive API by clicking [+ ENABLE APIS AND SERVICES] and selecting the Google Drive API.

The CCXT pair converter plugin uses a routing system to find the shortest pricing path between a base asset and a quote asset (what the asset is priced in). It does this by assembling a graph of nodes made out of assets and edges made from markets with a preference for the exchange the asset was purchased on. Fiat exchange rates from the European Central Bank are also added to the graph to allow any fiat to be converted between each other.

For example, if a user needs the price of their BETH (Beaconed ETH) purchased on Binance.com in CHF (Swiss Francs). The router will first route the price through the available markets on the exchange:

1. BETH -> ETH - The only market for BETH on Binance.com
2. ETH -> EUR - There are no ETH markets for USD, JPY, and KRW on Binance.com, so we go to the next in the list of fiat priority - EUR.

Then, it will route the price through a fiat conversion to get the final price:

3. EUR -> CHF

Be aware that:
* Exchange rates for fiat transactions are based on the daily rate and not minute or hourly rates.
* If a market for the conversion exists on the exchange where the asset was purchased, no routing takes place. The plugin retrieves the price for the time period.
* The router uses the exchange listed in the transaction data to build the graph to calculate the route. If no exchange is listed, the current default is Kraken(US).
* `fiat_priority` determines what fiat the router will attempt to route through first while trying to find a path to your quote asset.
* Some exchanges, in particular Binance.com, might not be available in certain territories.


### Binance Locked CCXT
This plugin makes use of the CCXT plugin, but locks all routes to Binance.com.

Initialize this plugin section as follows:
<pre>
[dali.plugin.pair_converter.ccxt</em>]
historical_price_type = <em>&lt;historical_price_type&gt;</em>
fiat_priority = <em>&lt;fiat_priority&gt;</em>
</pre>

Where:
* `<historical_price_type>` is one of `open`, `high`, `low`, `close`, `nearest`. When DaLi downloads historical market data, it captures a `bar` of data surrounding the timestamp of the transaction. Each bar has a starting timestamp, an ending timestamp, and OHLC prices. You can choose which price to select for price lookups. The open, high, low, and close prices are self-explanatory. The `nearest` price is either the open price or the close price of the bar depending on whether the transaction time is nearer the bar starting time or the bar ending time.
* `fiat_priority` is an optional list of strings in JSON format (e.g. `["_1stpriority_", "_2ndpriority_"...]`) that ranks the priority of fiat in the routing system. If no `fiat_priority` is given, the default priority is USD, JPY, KRW, EUR, GBP, AUD, which is based on the volume of the fiat market paired with BTC (ie. BTC/USD has the highest worldwide volume, then BTC/JPY, etc.).

The Binance Locked CCXT plugin still makes use of fiat exchange rate routing. Pricing will resolve to any major fiat currency even if it doesn't have a market (ie. not used to trade with) on Binance.com.

Be aware that:
* Exchange rates for fiat transactions are based on the daily rate and not minute or hourly rates.
* The router only uses Binance.com and the fiat exchange rates to build the graph to calculate the route.
* `fiat_priority` determines what fiat the router will attempt to route through first while trying to find a path to your quote asset.
* Binance.com might not be available in certain territories.


### Kraken Locked CCXT
This plugin makes use of the CCXT plugin, but locks all routes to Kraken(US).

Initialize this plugin section as follows:
<pre>
[dali.plugin.pair_converter.ccxt</em>]
historical_price_type = <em>&lt;historical_price_type&gt;</em>
fiat_priority = <em>&lt;fiat_priority&gt;</em>
google_api_key = <em>&lt;google_api_key&gt;</em>
</pre>

Where:
* `<historical_price_type>` is one of `open`, `high`, `low`, `close`, `nearest`. When DaLi downloads historical market data, it captures a `bar` of data surrounding the timestamp of the transaction. Each bar has a starting timestamp, an ending timestamp, and OHLC prices. You can choose which price to select for price lookups. The open, high, low, and close prices are self-explanatory. The `nearest` price is either the open price or the close price of the bar depending on whether the transaction time is nearer the bar starting time or the bar ending time.
* `fiat_priority` is an optional list of strings in JSON format (e.g. `["_1stpriority_", "_2ndpriority_"...]`) that ranks the priority of fiat in the routing system. If no `fiat_priority` is given, the default priority is USD, JPY, KRW, EUR, GBP, AUD, which is based on the volume of the fiat market paired with BTC (ie. BTC/USD has the highest worldwide volume, then BTC/JPY, etc.).
* `google_api_key` is an optional string for the Google API Key that is needed by some CSV readers, most notably the Kraken CSV reader. It is used to download the OHLCV files for a market. No data is ever sent to Google Drive. This is only used to retrieve data. To get a Google API Key, visit the [Google Console Page](https://console.developers.google.com/) and setup a new project. Be sure to enable the Google Drive API by clicking [+ ENABLE APIS AND SERVICES] and selecting the Google Drive API.

The Kraken Locked CCXT plugin still makes use of fiat exchange rate routing. Pricing will resolve to any major fiat currency even if it doesn't have a market (ie. not used to trade with) on Kraken.

Be aware that:
* Exchange rates for fiat transactions are based on the daily rate and not minute or hourly rates.
* The router only uses Kraken and the fiat exchange rates to build the graph to calculate the route.
* `fiat_priority` determines what fiat the router will attempt to route through first while trying to find a path to your quote asset.


### Historic Crypto
This plugin is based on the Historic_Crypto Python library.

Initialize this plugin section as follows:
<pre>
[dali.plugin.pair_converter.historic_crypto</em>]
historical_price_type = <em>&lt;historical_price_type&gt;</em>
</pre>

Where:
* `<historical_price_type>` is one of `open`, `high`, `low`, `close`, `nearest`. When DaLI downloads historical market data, it captures a `bar` of data surrounding the timestamp of the transaction. Each bar has a starting timestamp, an ending timestamp, and OHLC prices. You can choose which price to select for price lookups. The open, high, low, and close prices are self-explanatory. The `nearest` price is either the open price or the close price of the bar depending on whether the transaction time is nearer the bar starting time or the bar ending time.

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
* *`direction`* denotes how to represent the transaction in the generated file and it's one of: `in`, `out` or `intra`;
* *`transaction_type`* is the transaction type (see [Manual Section (CSV)](#manual-section-csv) for more details on this field);
* *`notes`* is an optional English comment.

### Header Sections
There are three header sections (all of which are optional):
* `in_header`
* `out_header`
* `intra_header`

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
