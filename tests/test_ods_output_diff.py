# Copyright 2022 eprbell
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

import os
import shutil
import unittest
from pathlib import Path
from subprocess import run
from typing import List

from ods_diff import ods_diff

from dali.cache import CACHE_DIR

ROOT_PATH: Path = Path(os.path.dirname(__file__)).parent.absolute()

CACHE_PATH: Path = ROOT_PATH / CACHE_DIR
CONFIG_PATH: Path = ROOT_PATH / Path("config")
INPUT_PATH: Path = ROOT_PATH / Path("input")
GOLDEN_PATH: Path = INPUT_PATH / Path("golden")
OUTPUT_PATH: Path = ROOT_PATH / Path("output")


class TestODSOutputDiff(unittest.TestCase):

    output_dir: Path

    @classmethod
    def setUpClass(cls) -> None:
        cls.output_dir = OUTPUT_PATH / Path(__file__[:-3]).name
        shutil.rmtree(cls.output_dir, ignore_errors=True)
        shutil.rmtree(CACHE_PATH, ignore_errors=True)

        cls._generate(cls.output_dir, "test", "test_config")

    def setUp(self) -> None:  # pylint: disable=invalid-name
        self.maxDiff = None  # pylint: disable=invalid-name

    @classmethod
    def _generate(
        cls,
        output_dir: Path,
        test_name: str,
        config: str,
    ) -> None:

        arguments: List[str] = [
            "dali_us",
            "-s",
            "-o",
            str(output_dir),
            "-p",
            f"{test_name}_",
            str(CONFIG_PATH / Path(f"{config}.ini")),
        ]
        run(arguments, check=True)

        # Temporarily removed due to https://github.com/eprbell/rp2/issues/79
        # for method in ["fifo", "lifo"]:
        for method in ["fifo"]:
            arguments = [
                "rp2_us",
                "-m",
                method,
                "-o",
                str(output_dir),
                "-p",
                f"{test_name}_",
                str(output_dir / Path(f"{test_name}_crypto_data.ini")),
                str(output_dir / Path(f"{test_name}_crypto_data.ods")),
            ]
            run(arguments, check=True)

    def test_crypto_data_ods(self) -> None:
        file_name: str = "test_crypto_data.ods"
        full_output_file_name: Path = self.output_dir / file_name
        full_golden_file_name: Path = GOLDEN_PATH / file_name
        diff = ods_diff(full_golden_file_name, full_output_file_name, generate_ascii_representation=True)
        self.assertFalse(diff, msg=diff)

    def test_fifo_tax_report_us_ods(self) -> None:
        file_name: str = "test_fifo_tax_report_us.ods"
        full_output_file_name: Path = self.output_dir / file_name
        full_golden_file_name: Path = GOLDEN_PATH / file_name
        diff = ods_diff(full_golden_file_name, full_output_file_name, generate_ascii_representation=True)
        self.assertFalse(diff, msg=diff)

    def test_fifo_rp2_full_report_ods(self) -> None:
        file_name: str = "test_fifo_rp2_full_report.ods"
        full_output_file_name: Path = self.output_dir / file_name
        full_golden_file_name: Path = GOLDEN_PATH / file_name
        diff = ods_diff(full_golden_file_name, full_output_file_name, generate_ascii_representation=True)
        self.assertFalse(diff, msg=diff)

    # Temporarily removed due to https://github.com/eprbell/rp2/issues/79
    # def test_lifo_tax_report_us_ods(self) -> None:
    #     file_name: str = "test_lifo_tax_report_us.ods"
    #     full_output_file_name: Path = self.output_dir / file_name
    #     full_golden_file_name: Path = GOLDEN_PATH / file_name
    #     diff = ods_diff(full_golden_file_name, full_output_file_name, generate_ascii_representation=True)
    #     self.assertFalse(diff, msg=diff)

    # def test_lifo_rp2_full_report_ods(self) -> None:
    #     file_name: str = "test_lifo_rp2_full_report.ods"
    #     full_output_file_name: Path = self.output_dir / file_name
    #     full_golden_file_name: Path = GOLDEN_PATH / file_name
    #     diff = ods_diff(full_golden_file_name, full_output_file_name, generate_ascii_representation=True)
    #     self.assertFalse(diff, msg=diff)


if __name__ == "__main__":
    unittest.main()
