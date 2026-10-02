# Copyright 2026 Neal Chambers
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

# Kraken publishes the complete OHLCVT history of every pair as one ~9 GB zip file, split into ~2 GB parts:
# https://support.kraken.com/hc/en-us/articles/360047124832-Downloadable-historical-OHLCVT-Open-High-Low-Close-Volume-Trades-data
# The end of a zip file lists where each file inside it starts, and Kraken's server supports HTTP range requests,
# so only the CSVs of the pairs being priced need to be downloaded, not the whole release.

import re
from bisect import bisect_right
from time import sleep
from typing import IO, Dict, List, Optional, Pattern, Tuple, cast
from zipfile import ZipFile

from requests import RequestException
from requests.sessions import Session
from rp2.rp2_error import RP2RuntimeError

KRAKEN_ASSETS_URL: str = "https://assets.kraken.com/marketing/institutions/"
# Lists the parts of the latest release, so new releases are picked up without code changes
KRAKEN_CHECKSUMS_URL: str = KRAKEN_ASSETS_URL + "OHLCVT_Full_PARTS_SHA256SUMS.txt"

# A line of the checksum list, e.g. "<sha256>  Kraken_OHLCVT_Full_2026Q2.zip.part00"
_PART_LINE: Pattern[str] = re.compile(r"[0-9a-fA-F]{64}\s+\*?(?P<part>(?P<release>\S+)\.zip\.part\d+)")

_TIMEOUT: int = 30
_RANGE_SIZE: int = 8 * 1024 * 1024  # Large files are downloaded 8 MB at a time, so a failure only repeats one request
_RETRIES: int = 3


# Read-only file made of the parts of a release, which downloads only the byte ranges that are read
class _RemoteParts:
    def __init__(self, part_urls: List[str], session: Session) -> None:
        self.__part_urls: List[str] = part_urls
        self.__session: Session = session
        self.__part_starts: List[int] = []
        self.__size: int = 0
        for url in part_urls:
            response = session.head(url, allow_redirects=True, timeout=_TIMEOUT)
            response.raise_for_status()
            self.__part_starts.append(self.__size)
            self.__size += int(response.headers["Content-Length"])
        self.__position: int = 0
        self.__prefetched_start: int = 0
        self.__prefetched: bytes = b""

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.__position

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 1:
            offset += self.__position
        elif whence == 2:
            offset += self.__size
        elif whence != 0:
            raise ValueError(f"Invalid whence: {whence}")
        if offset < 0:
            raise OSError(f"Invalid position: {offset}")
        self.__position = offset
        return offset

    def read(self, size: int = -1) -> bytes:
        end: int = self.__size if size < 0 else min(self.__position + size, self.__size)
        if end <= self.__position:
            return b""
        if self.__prefetched_start <= self.__position and end <= self.__prefetched_start + len(self.__prefetched):
            data: bytes = self.__prefetched[self.__position - self.__prefetched_start : end - self.__prefetched_start]
        else:
            data = self.__download(self.__position, end)
        self.__position = end
        return data

    # zipfile reads a file in small pieces, so the whole range is downloaded ahead of it in as few requests as possible
    def prefetch(self, start: int, end: int) -> None:
        self.__prefetched = b""
        self.__prefetched = self.__download(start, end)
        self.__prefetched_start = start

    def discard_prefetched(self) -> None:
        self.__prefetched = b""

    def __download(self, start: int, end: int) -> bytes:
        pieces: List[bytes] = []
        while start < end:
            part: int = bisect_right(self.__part_starts, start) - 1
            part_end: int = self.__part_starts[part + 1] if part + 1 < len(self.__part_starts) else self.__size
            piece_end: int = min(end, part_end, start + _RANGE_SIZE)
            pieces.append(self.__download_range(part, start - self.__part_starts[part], piece_end - self.__part_starts[part]))
            start = piece_end
        return b"".join(pieces)

    # Downloads bytes [start, end) of a part
    def __download_range(self, part: int, start: int, end: int) -> bytes:
        attempt: int = 0
        while True:
            try:
                response = self.__session.get(self.__part_urls[part], headers={"Range": f"bytes={start}-{end - 1}"}, timeout=_TIMEOUT)
                response.raise_for_status()
                if response.status_code != 206 or len(response.content) != end - start:
                    raise RP2RuntimeError(f"Kraken's server didn't return bytes {start}-{end - 1} of {self.__part_urls[part]} (HTTP {response.status_code})")
                return response.content
            except RequestException:
                attempt += 1
                if attempt == _RETRIES:
                    raise
                sleep(attempt)


# The latest release of Kraken's complete OHLCVT history, read from Kraken's server
class KrakenRelease:
    def __init__(self, session: Session) -> None:
        response = session.get(KRAKEN_CHECKSUMS_URL, timeout=_TIMEOUT)
        response.raise_for_status()
        parts: Dict[str, List[str]] = {}
        for line in response.text.splitlines():
            match = _PART_LINE.fullmatch(line.strip())
            if match:
                parts.setdefault(match.group("release"), []).append(match.group("part"))
        if not parts:
            raise RP2RuntimeError(f"No release found in {KRAKEN_CHECKSUMS_URL}")

        self.name: str = max(parts)
        self.__part_urls: List[str] = [KRAKEN_ASSETS_URL + part for part in sorted(parts[self.name])]
        self.__session: Session = session
        self.__file: Optional[_RemoteParts] = None
        self.__zip: Optional[ZipFile] = None
        # Byte range [start, end) of each file inside the release
        self.__spans: Dict[str, Tuple[int, int]] = {}

    def namelist(self) -> List[str]:
        return self.__open().namelist()

    def compressed_size(self, file_name: str) -> int:
        self.__open()
        start, end = self.__spans[file_name]
        return end - start

    # zipfile checks the CRC-32 of the file, so a corrupted download raises an error rather than returning wrong data
    def read(self, file_name: str) -> bytes:
        zip_file: ZipFile = self.__open()
        remote_file: _RemoteParts = cast(_RemoteParts, self.__file)
        remote_file.prefetch(*self.__spans[file_name])
        try:
            return zip_file.read(file_name)
        finally:
            remote_file.discard_prefetched()

    # The list of files at the end of the release is only downloaded once a file is needed
    def __open(self) -> ZipFile:
        if self.__zip is None:
            self.__file = _RemoteParts(self.__part_urls, self.__session)
            # Stays open to read the files of the release. The remote parts hold no OS resources, so nothing needs closing.
            self.__zip = ZipFile(cast(IO[bytes], self.__file))  # pylint: disable=consider-using-with
            starts: List[Tuple[int, str]] = sorted((info.header_offset, info.filename) for info in self.__zip.infolist())
            ends: List[int] = [start for start, _ in starts[1:]] + [self.__zip.start_dir]
            self.__spans = {file_name: (start, end) for (start, file_name), end in zip(starts, ends)}
        return self.__zip
