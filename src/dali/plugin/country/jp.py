# Copyright 2023 Neal Chambers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from rp2.plugin.country.jp import JP

from dali.dali_main import dali_main


# JP-specific entry point
def dali_entry() -> None:
    dali_main(JP())

# This traditional entry point is used for debugging purposes only, dali_entry()
# is normally called through the use of an installed console script in setup.py.
if __name__ == '__main__':
    dali_entry()
