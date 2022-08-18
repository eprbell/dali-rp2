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

# Contributing to DaLI

## Table of Contents
* **[Reporting Bugs](#reporting-bugs)**
* **[Contributing to the Repository](#contributing-to-the-repository)**
  * [Submitting Pull Requests](#submitting-pull-requests)
* **[Contributing with an Ecosystem Project](#contributing-with-an-ecosystem-project)**

## Reporting Bugs
Feel free to submit bugs via [Issue Tracker](https://github.com/eprbell/dali-rp2/issues), but **PLEASE READ THE FOLLOWING FIRST**: DaLI reads data from exchanges using REST APIs, which require API key, secret and / or passphrase. NEVER share your API key, secret and passphrase with anyone! DaLI stores the crypto data it reads locally on the user's machine and doesn't send this data elsewhere. Logs and outputs can be useful to reproduce a bug, so a user can decide (or not) to share them to help fix a problem. If you decide to share this information, be mindful of what you post or send out: stack traces are typically free of personal data, but DaLI logs and outputs, while very useful to reproduce an issue, may contain information that can identify you and your transactions. Before posting such data publicly or even sending it privately to the maintainers of DaLI, make sure that:
* the data is sanitized of personal information (although this may make it harder to reproduce the problem), or
* you're comfortable sharing your personal data.

Logs are stored in the `log/` directory and each file name is appended with a timestamp. Outputs are stored in the `output/` directory or where specified by the user with the `-o` option.

## Contributing to the Repository
Read the [developer guide](README.dev.md), which describes setup instructions, development workflow, design principles, source tree structure, plugin architecture, etc. In particular, if you're submitting a new plugin, be sure to read the [Plugin Development](README.dev.md#plugin-development) section.

### Submitting Pull Requests
Feel free to submit pull requests. Please follow these practices:
* follow the DaLI [design guidelines](README.dev.md#design-guidelines)
* follow the [PEP 8](https://www.python.org/dev/peps/pep-0008/) coding standard;
* add [unit tests](tests/) for any new code;
* ensure your commits are atomic (one feature per commit);
* write a clear log message for your commits.

## Contributing with an Ecosystem Project
Read about the [RP2 Ecosystem](https://github.com/eprbell/rp2/blob/main/README.md#rp2-ecosystem).
