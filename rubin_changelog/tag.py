import re
from enum import Enum


class ReleaseType(Enum):
    WEEKLY = 1
    REGULAR = 2


class Tag:
    def __init__(self, name: str):
        self._name = name
        self._valid = False
        self._is_weekly = False
        self._is_main = False
        self._hash = -1
        if name.endswith("main"):
            self._hash = 9999999999
            self._valid = True
            self._is_main = True
            return
        if name.startswith('w'):
            self._weekly()
        else:
            self._regular()

    def name(self) -> str:
        return self._name

    def _weekly(self):
        match = re.search(r'^w[.|_](\d+)[_|.](\d+)$', self._name)
        if match is None:
            return
        g = list(match.groups())
        try:
            year = int(g[0])
            week = int(g[1])
        except ValueError:
            return
        self._is_weekly = True
        self._hash = year * 100 + week
        self._valid = True

    def is_weekly(self) -> bool:
        return self._is_main or self._is_weekly

    def is_release(self) -> bool:
        return not self._is_weekly or self._is_main

    def _regular(self):
        match = re.search(r'^[v]?(\d+)([_|.]\d+)([_|.]\d+)?([_|.]rc(\d+))?$', self._name)
        if not match:
            return
        g = list()
        for e in match.groups():
            g.append(e)
        g[1] = g[1].replace('.', '')
        g[1] = g[1].replace('_', '')
        if g[2] is None:
            g[2] = '0'
        else:
            g[2] = g[2].replace('.', '')
            g[2] = g[2].replace('_', '')
        if g[4] is None:
            g[4] = 99
        try:
            major = int(g[0])
            minor = int(g[1])
            patch = int(g[2])
            rc = int(g[4])
        except ValueError:
            return
        if major < 9 or major > 1000:
            return
        self._hash = major * 1000000 + minor * 10000 + patch * 100 + rc
        self._valid = True

    def rel_name(self) -> str:
        if self._is_main:
            return 'main'
        name = self._name
        if not self._is_weekly \
                and not name.startswith('v'):
            name = 'v' + name
        name = name.replace('.', '_')
        return name

    def hash(self) -> int:
        return self._hash

    def is_valid(self) -> bool:
        return self._valid

    def __eq__(self, other) -> bool:
        return self._hash == other.hash()

    def __ge__(self, other) -> bool:
        return self._hash >= other.hash()

    def __gt__(self, other) -> bool:
        return self._hash > other.hash()

    def __le__(self, other) -> bool:
        return self._hash <= other.hash()

    def __lt__(self, other) -> bool:
        return self._hash < other.hash()

    def __repr__(self) -> str:
        return self._name

    def __hash__(self) -> int:
        return self._hash


def matches_release(tag: Tag, release: ReleaseType) -> bool:
    if tag.is_weekly() and release == ReleaseType.WEEKLY:
        return True
    if not tag.is_weekly() and release == ReleaseType.REGULAR:
        return True
    return False
