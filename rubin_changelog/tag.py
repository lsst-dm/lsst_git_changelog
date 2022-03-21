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

import re
from enum import Enum


class ReleaseType(Enum):
    """Enum class to specify the release type"""
    WEEKLY = 1
    REGULAR = 2


class Tag:
    """Helper class to sort GitHub release tags"""

    def __init__(self, name: str):
        """

        Parameters
        ----------
        name: `str`
            name of tag

        """
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
        match = re.search(r'^w[.|_](\d{4})[_|.](\d{2})$', self._name)
        if match is None:
            return
        year = int(match[1])
        week = int(match[2])
        self._is_weekly = True
        self._hash = year * 100 + week
        self._valid = True

    def is_weekly(self) -> bool:
        """check for weekly release tag

        Returns
        -------
        weekly release : `bool`
            true if week release tag, false for main and regular release tag

        """
        return self._is_main or self._is_weekly

    def is_regular(self) -> bool:
        """check for regular release tag

        Returns
        -------
        regular release : `bool`
            true if a regular release tag or main

        """
        return not self._is_weekly or self._is_main

    def _regular(self):
        match = re.search(r'^[v]?(\d{1,2})([_|.]\d{1,2)([_|.]\d{1,2)?([_|.]rc(\d{1,2}))?$', self._name)
        if not match:
            return
        g = list(match.groups())
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
        """Get canonical release name

        Returns
        -------
        release name : `str`
            returns w_XXXX_XX for weekly tags
                     v_XX_XX[_XX}[_rcXX] fpr release tags

        """
        if self._is_main:
            return 'main'
        name = self._name
        if not self._is_weekly and not name.startswith('v'):
            name = 'v' + name
        name = name.replace('.', '_')
        return name

    def is_valid(self) -> bool:
        return self._valid

    def __eq__(self, other) -> bool:
        return self._hash == other.__hash__()

    def __ge__(self, other) -> bool:
        return self._hash >= other.__hash__()

    def __gt__(self, other) -> bool:
        return self._hash > other.__hash__()

    def __le__(self, other) -> bool:
        return self._hash <= other.__hash__()

    def __lt__(self, other) -> bool:
        return self._hash < other.__hash__()

    def __repr__(self) -> str:
        return self._name

    def __hash__(self) -> int:
        return self._hash


def matches_release(tag: Tag, release: ReleaseType) -> bool:
    """Check if a tag matches a given release type

    Parameters
    ----------
    tag: `Tag`
        tag class
    release: `Release Type`
        release type WEEKLY or REGULAR

    Returns
    -------
    matches release: `bool`
        returns true if tag matches the release type

    """
    if tag.is_weekly() and release == ReleaseType.WEEKLY:
        return True
    if not tag.is_regular() and release == ReleaseType.REGULAR:
        return True
    return False
