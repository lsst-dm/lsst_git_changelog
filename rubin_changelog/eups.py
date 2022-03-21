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

import concurrent
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List, Dict

import requests
import urllib3
from bs4 import BeautifulSoup
from sortedcontainers import SortedDict, SortedList

from .tag import ReleaseType, Tag, matches_release

log = logging.getLogger(__name__)


class EupsData:
    """class to describe data from the EUPS server"""
    def __init__(self, connections: int = 10):
        self._url = 'https://eups.lsst.codes/stack/src/tags/'
        self._connection_mgr = urllib3.PoolManager(maxsize=connections)
        self._connections = connections

    @staticmethod
    def _process_list(data) -> List[Dict[str, str]]:
        """

        Parameters
        ----------
        data : `Any`

        Returns
        -------
        eups data : `List[Dict[str, str]]`
            eups data for a release

        """
        lines = data.split(b"\n")
        result = list()
        for line in lines:
            if line.startswith(b"#") or \
                    line.startswith(b"EUPS distribution"):
                continue
            col = line.split()
            if len(col) != 3:
                continue
            package = col[0].decode('utf-8')
            flavor = col[1].decode('utf-8')
            version = col[2].decode('utf-8')
            result.append({'package': package,
                           'flavor': flavor,
                           'version': version,
                           })
        return result

    def _get_url_paths(self) -> List[str]:
        """retrieve list of .list files on the EUPS server

        Returns
        -------
        urls: `List`
            list of EUPS URLS

        """
        ext = '.list'
        params = {}
        response = requests.get(self._url, params=params)
        if response.ok:
            response_text = response.text
        else:
            return []
        soup = BeautifulSoup(response_text, 'html.parser')
        parent = [self._url + node.get('href')
                  for node in soup.find_all('a') if node.get('href').endswith(ext)]
        return parent

    def _download(self, url: str) -> SortedDict:
        """download an EUPS data file

        Parameters
        ----------
        url: `str`
            URL to download

        Returns
        -------
        content : `SortedDict`
            URL content

        """
        response = self._connection_mgr.request('GET', url)
        name = url.split('/')[-1].split('.')[0]
        rtag = Tag(name)
        if response.status == 200 and rtag.is_valid():
            return SortedDict({'name': rtag,
                               'data': self._process_list(response.data)
                               })
        else:
            return SortedDict()

    def get_releases(self, release: ReleaseType) -> SortedDict:
        """get all releases for a specific release type

        Parameters
        ----------
        release: `ReleaseType`
            release type WEEKLY or REGULAR

        Returns
        -------
        releases: `SortedDict`
            sorted dictionary with releases

        """
        urls = self._get_url_paths()
        result = SortedDict()
        release_list = SortedDict()
        product_list = SortedList()
        url_list = list()
        for url in urls:
            name = url.split('/')[-1]
            name = name.replace('.list', '')
            rtag = Tag(name)
            if not rtag.is_valid():
                continue
            if not matches_release(rtag, release):
                continue
            url_list.append(url)

        with ThreadPoolExecutor(
                max_workers=self._connections) as executor:
            futures = {executor.submit(self._download, url): url for url in url_list}
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                except Exception:
                    log.error("Failed")
                else:
                    if 'name' in data:
                        name = data['name']
                        release_list[name] = data['data']
        result["releases"] = release_list
        for r in release_list:
            for entry in release_list[r]:
                name = entry["package"]
                if name not in product_list:
                    product_list.add(name)
        result["products"] = product_list
        return result
