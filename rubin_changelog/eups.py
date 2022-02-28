import concurrent
from concurrent.futures.thread import ThreadPoolExecutor

import requests
import urllib3
from bs4 import BeautifulSoup
from sortedcontainers import SortedDict, SortedList

from .tag import *

import logging

log = logging.getLogger(__name__)


class EupsData:
    def __init__(self, connections: int = 10):
        self._url = f'https://eups.lsst.codes/stack/src/tags/'
        self._connection_mgr = urllib3.PoolManager(maxsize=connections)
        self._connections = connections

    @staticmethod
    def _process_list(data):
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

    def _get_url_paths(self):
        ext = '.list'
        params = {}
        response = requests.get(self._url, params=params)
        if response.ok:
            response_text = response.text
        else:
            return response.raise_for_status()
        soup = BeautifulSoup(response_text, 'html.parser')
        parent = [self._url + node.get('href')
                  for node in soup.find_all('a') if node.get('href').endswith(ext)]
        return parent

    def _download(self, url: str) -> SortedDict:
        response = self._connection_mgr.request('GET', url)
        name = url.split('/')[-1].split('.')[0]
        rtag = Tag(name)
        if response.status == 200 and rtag.is_valid():
            return SortedDict({'name': rtag,
                               'data': self._process_list(response.data)
                               })
        else:
            return SortedDict()

    def get_releases(self, release: ReleaseType):
        urls = self._get_url_paths()
        result = SortedDict()
        release_list = SortedDict()
        product_list = SortedList()
        url_list = list()
        for url in urls:
            name = url.split('/')[-1]
            name = name.replace('.list','')
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
                except Exception as exc:
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
