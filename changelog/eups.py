#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import concurrent.futures
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Iterable

import urllib3
from bs4 import BeautifulSoup

from .tag import ReleaseType, Tag, matches_release

log = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Data models
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class PackageEntry:
    package: str
    flavor: str
    version: str


@dataclass(frozen=True)
class Release:
    tag: Tag
    packages: List[PackageEntry]


@dataclass
class ReleaseCollection:
    releases: Dict[Tag, List[PackageEntry]]
    products: Set[str]


@dataclass(frozen=True)
class PackageDiff:
    added: Set[str]
    removed: Set[str]
    packages: Set[str]


@dataclass
class ReleaseResult:
    data: ReleaseCollection
    diffs: Dict[Tag, PackageDiff]


# -----------------------------------------------------------------------------
# Main API
# -----------------------------------------------------------------------------


class EupsData:
    """Retrieve EUPS release data."""

    BASE_URL = "https://eups.lsst.cloud/stack/src/tags/"

    def __init__(self, connections: int = 10):
        self._connections = connections
        self._http = urllib3.PoolManager(maxsize=connections)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _process_list(data: bytes) -> List[PackageEntry]:
        """Parse an EUPS `.list` file into package entries."""
        entries: List[PackageEntry] = []

        for line in data.splitlines():
            if not line or line.startswith((b"#", b"EUPS distribution")):
                continue

            parts = line.split()
            if len(parts) != 3:
                continue

            package, flavor, version = (p.decode("utf-8") for p in parts)
            entries.append(PackageEntry(package, flavor, version))

        return entries

    def _get_url_paths(self) -> List[str]:
        """Retrieve all `.list` file URLs from the EUPS server."""
        try:
            response = self._http.request(
                "GET",
                self.BASE_URL,
                timeout=urllib3.Timeout(connect=5.0, read=10.0),
            )
        except Exception:
            log.exception("Failed to fetch EUPS index page")
            return []

        if response.status != 200:
            log.error(
                "Unexpected HTTP status %s for %s",
                response.status,
                self.BASE_URL,
            )
            return []

        soup = BeautifulSoup(response.data, "html.parser")

        return [
            f"{self.BASE_URL.rstrip('/')}/{a['href'].split('/')[-1]}"
            for a in soup.find_all("a", href=True)
            if a["href"].endswith(".list")
        ]

    def _download(self, url: str) -> Optional[Release]:
        """Download and parse a single EUPS `.list` file."""
        try:
            response = self._http.request(
                "GET",
                url,
                timeout=urllib3.Timeout(connect=5.0, read=30.0),
            )
        except Exception:
            log.exception("Request failed for %s", url)
            return None

        if response.status != 200:
            log.warning("Non-200 response (%s) for %s", response.status, url)
            return None

        name = url.rsplit("/", 1)[-1].removesuffix(".list")
        tag = Tag(name)

        if not tag.is_valid():
            log.debug("Invalid tag skipped: %s", name)
            return None

        return Release(
            tag=tag,
            packages=self._process_list(response.data),
        )

    def get_release(self, release_type: ReleaseType) -> ReleaseCollection:
        """Retrieve all releases for a specific release type."""

        urls = self._get_url_paths()

        release_urls: List[str] = []
        for url in urls:
            name = url.rsplit("/", 1)[-1].removesuffix(".list")
            tag = Tag(name)

            if not tag.is_valid():
                continue
            if not matches_release(tag, release_type):
                continue

            release_urls.append(url)

        releases: Dict[Tag, List[PackageEntry]] = {}

        with ThreadPoolExecutor(max_workers=self._connections) as executor:
            futures = [executor.submit(self._download, url) for url in release_urls]

            for future in concurrent.futures.as_completed(futures):
                release = future.result()
                if release is not None:
                    releases[release.tag] = release.packages

        products: Set[str] = {
            entry.package
            for packages in releases.values()
            for entry in packages
        }

        return ReleaseCollection(releases=releases, products=products)

    @staticmethod
    def get_package_diff(collection: ReleaseCollection) -> Dict[Tag, PackageDiff]:
        """Compute added and removed packages between releases."""

        result: Dict[Tag, PackageDiff] = {}
        previous_packages: Optional[Set[str]] = None

        for tag in sorted(collection.releases):
            current_packages = {
                entry.package for entry in collection.releases[tag]
            }

            if previous_packages is not None:
                result[tag] = PackageDiff(
                    added=current_packages - previous_packages,
                    removed=previous_packages - current_packages,
                    packages=current_packages,
                )

            previous_packages = current_packages

        return result

    def get_releases(self, release_type: ReleaseType) -> ReleaseResult:
        """Retrieve releases and package diffs for a release type."""

        data = self.get_release(release_type)
        diffs = self.get_package_diff(data)
        return ReleaseResult(data=data, diffs=diffs)

    def get_all_releases(self) -> Dict[str, ReleaseResult]:
        """Retrieve weekly and regular releases."""

        return {
            "weekly": self.get_releases(ReleaseType.WEEKLY),
            "regular": self.get_releases(ReleaseType.REGULAR),
        }


