# Copyright 2022 Steve Davis
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

from typing import NamedTuple, Optional

import pytest

from dali.configuration import Keyword
from dali.intra_transaction import IntraTransaction
from dali.transaction_resolver import _resolve_intra_intra_transaction


class IntraTransactionNotesTestCase(NamedTuple):
    prior_notes: Optional[str]
    notes1: Optional[str]
    notes2: Optional[str]
    resolved_notes: Optional[str]


intra_transaction_notes_test_cases = [
    IntraTransactionNotesTestCase(None, None, None, None),  # resolver assignes empty string, IntraTransaction changes it to None
    IntraTransactionNotesTestCase(None, "xxx", None, "xxx;"),
    IntraTransactionNotesTestCase(None, None, "yyy", "yyy;"),
    IntraTransactionNotesTestCase(None, "xxx", "yyy", "xxx; yyy;"),
    IntraTransactionNotesTestCase(None, "xxx", "xxx", "xxx;"),  # do not concatentate identical notes
    IntraTransactionNotesTestCase("prior", None, None, "prior;"),
    IntraTransactionNotesTestCase("prior", "xxx", None, "prior; xxx;"),
    IntraTransactionNotesTestCase("prior", None, "yyy", "prior; yyy;"),
    IntraTransactionNotesTestCase("prior", "xxx", "yyy", "prior; xxx; yyy;"),
    IntraTransactionNotesTestCase("prior", "xxx", "xxx", "prior; xxx;"),  # do not concatentate identical notes
]


@pytest.mark.parametrize("prior_notes, notes1, notes2, resolved_notes", intra_transaction_notes_test_cases)
def test_resolve_intra_intra_transaction(prior_notes: Optional[str], notes1: Optional[str], notes2: Optional[str], resolved_notes: Optional[str]) -> None:
    """Verify resolved transaction matches expected values."""
    transaction1 = IntraTransaction(
        plugin="plugin",
        unique_id="unique_id",
        raw_data="raw_data1",
        asset="asset",
        timestamp="2022-01-01 00:00:00+00:00",
        from_exchange="from_exchange1",
        from_holder="from_holder1",
        to_exchange=Keyword.UNKNOWN.value,
        to_holder=Keyword.UNKNOWN.value,
        spot_price=None,
        crypto_sent="1.0",
        crypto_received="1.0",
        notes=notes1,
    )

    transaction2 = IntraTransaction(
        plugin="plugin",
        unique_id="unique_id",
        raw_data="raw_data2",
        asset="asset",
        timestamp="2022-01-01 00:00:00+00:00",
        from_exchange=Keyword.UNKNOWN.value,
        from_holder=Keyword.UNKNOWN.value,
        to_exchange="to_exchange2",
        to_holder="to_holder2",
        spot_price="1000.0",
        crypto_sent="1.0",
        crypto_received="1.0",
        notes=notes2,
    )

    resolved = _resolve_intra_intra_transaction(transaction1, transaction2, prior_notes)
    assert resolved.from_exchange == "from_exchange1"
    assert resolved.from_holder == "from_holder1"
    assert resolved.to_exchange == "to_exchange2"
    assert resolved.to_holder == "to_holder2"
    assert resolved.spot_price == "1000.0"
    assert resolved.notes == resolved_notes
