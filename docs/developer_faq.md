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

# DaLI Frequently Asked Questions (Developer)

## Table of Contents
* **[General Questions](#general-questions)**
  * [What are the Contribution Guidelines?](#what-are-the-contribution-guidelines)
  * [What are the Design Guidelines?](#what-are-the-design-guidelines)
  * [What is the Best Way to Get Started on DaLI Development?](#what-is-the-best-way-to-get-started-on-dali-development)
  * [How to Develop a DaLI Data Loader Plugin?](#how-to-develop-a-dali-data-loader-plugin)
  * [How Does DaLI Merge Transactions Between Different Exchanges/Wallets?](#how-does-dali-merge-transactions-between-different-exchangeswallets)
  * [Why the Strange Directory Structure with Src?](#why-the-strange-directory-structure-with-src)

## General Questions

## What are the Contribution Guidelines?
Read the [contribution guidelines](../CONTRIBUTING.md#contributing-to-the-repository) section of the documentation.

## What are the Design Guidelines?
Read the [design guidelines](../README.dev.md#design-guidelines) section of the documentation.

## What is the Best Way to Get Started on DaLI Development?
Read the [contribution guidelines](../CONTRIBUTING.md#contributing-to-the-repository) and the [developer documentation](../README.dev.md) (especially the Internal Design section). Then look for an unassigned [issue](https://github.com/eprbell/dali-rp2/issues) that is marked as `good first issue`, or ask the project maintainers.

### How to Develop a DaLI Data Loader Plugin?
Read the [Internal Design](../README.dev.md#internal-design) section of the Developer Documentation (and in particular the Plugin subsections).

### How Does DaLI Merge Transactions Between Different Exchanges/Wallets?
Read about the [transaction_resolver](../src/dali/transaction_resolver.py) in the [Internal Design](../README.dev.md#internal-design) section of the Developer Documentation.

### Why the Strange Directory Structure with Src?
Because DaLI is a [src](https://bskinn.github.io/My-How-Why-Pyproject-Src/)-[based](https://hynek.me/articles/testing-packaging/) [project](https://blog.ionelmc.ro/2014/05/25/python-packaging/).

