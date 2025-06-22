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
from conf import changelog_conf

discard_tag = []
first_tag = []


class ReleaseType(Enum):
    """Enum class to specify the release type"""
    WEEKLY = 1
    REGULAR = 2
    DAILY = 3
    ALL = 4


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
        self._is_daily = False
        self._is_main = False
        self._major = -1
        self._minor = -1
        self._patch = -1
        self._rc = -1
        self._week = -1
        self._year = -1
        self._month = -1
        self._day = -1
        if name.endswith("main"):
            self._valid = True
            self._is_main = True
            return
        if name.startswith('w'):
            self._weekly()
        elif name.startswith('d'):
            self._daily()
        else:
            self._regular()

    def name(self) -> str:
        return self._name

    def _weekly(self):
        match = re.search(r'^w[.|_](\d{4})[_|.](\d{2})$', self._name)
        if match is None:
            return
        self._year = int(match[1])
        self._week = int(match[2])
        self._is_weekly = True
        self._valid = True

    def _daily(self):
        match = re.search(r'^w[.|_](\d{4})[_|.](\d{2}[_|.]\d{2})$', self._name)
        if match is None:
            return
        self._year = int(match[1])
        self._month =  int(match[2])
        self._day = int(match[3])
        self._is_daily = True
        self._valid = True

    def is_weekly(self) -> bool:
        """check for weekly release tag

        Returns
        -------
        weekly release : `bool`
            true if week release tag, false for main and regular release tag

        """
        return self._is_main or self._is_weekly

    def is_weekly(self) - > bool:
        return self._is_main or self._is_daily

    def is_regular(self) -> bool:
        """check for regular release tag

        Returns
        -------
        regular release : `bool`
            true if a regular release tag or main

        """
        return not self._is_weekly or self._is_main

    def is_release(self) -> bool:
        """check for final release tag

        Returns
        -------
        final release : `bool`
            true if a final release tag

        """
        return self._rc == 99

    def _regular(self):
        match = re.search(r'^[v]?(\d+)([_|.]\d+)([_|.]\d+)?([_|.]rc(\d+))?$', self._name)
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
            self._major = int(g[0])
            self._minor = int(g[1])
            self._patch = int(g[2])
            self._rc = int(g[4])
        except ValueError:
            return
        if self._major < 9 or self._major > 1000:
            return
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
        return self._valid and self.name() not in discard_tag

    def __eq__(self, other) -> bool:
        return self.__hash__() == other.__hash__()

    def __ge__(self, other) -> bool:
        return self.__hash__() >= other.__hash__()

    def __gt__(self, other) -> bool:
        return self.__hash__() > other.__hash__()

    def __le__(self, other) -> bool:
        return self.__hash__() <= other.__hash__()

    def __lt__(self, other) -> bool:
        return self.__hash__() < other.__hash__()

    def __repr__(self) -> str:
        return repr(self._name)

    def __hash__(self) -> int:
        if self._is_main:
            return 9999999999
        if self.is_weekly():
            return self._year * 100 + self._week
        elif self.is_daily():
            return self._year * 10000 + self._month *10 + self._day
        else:
            return self._major * 1000000 + self._minor * 10000 + self._patch * 100 + self._rc

    def desc(self):
        if self.is_weekly():
            return 'weekly', [self._year, self._week]
        if self.is_daily():
            return 'daily', [self._year, self._month, self._day]
        if self.is_regular():
            return 'regular', [self._major, self._minor, self._patch, self._rc]
        return 'main', [9999999999]

    def tag_branch(self) -> str:
        """Tag branch string of a tag

        Returns
        -------
        -------
        tag_branch: `str`
            branch of tag <major>.<minor>.x

        """
        result = ""
        if self.is_regular():
            result = "%d.0.x" % self._major
        return result

    def base_name(self) -> str:
        if self.is_regular():
            return f'{self._major}.{self._minor}.{self._patch}'
        elif self.is_weekly():
            return f'w.{self._year:4}.{self._week:02}'
        elif self.is_daily():
            return f'd.{self._year:4}.{self._month:02}.{self._day:02}'
        else:
            return 'main'

    def same_major(self, other: str) -> bool:
        o = Tag(other).desc()[1]
        return self.is_regular and self._major == o[0] and self._minor == o[1]

    def first_name(self) -> str:
        if self.is_regular():
            return f'{self._major}.{self._minor}.0'
        elif self.is_weekly():
            return f'w.{self._year:4}.{self._week:02}'
        elif self.is_daily():
            return f'd.{self._year:4}.{self._month:02}.{self._day:02}'
        else:
            return 'main'

    def eups_tag(self) -> str:
        return self.base_name().replace('.', '_')

    def is_first_release_tag(self) -> bool:
        """Tag is the first tag in a release series

        Returns
        -------
        result: `bool`
            True for tags like 23.0.0.rc1

        """
        result = False
        if self.is_regular():
            result = (self._rc == 1 and self._patch == 0) or \
                     ((self.name() in first_tag) and (self.name() not in discard_tag))
        return result


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
    if tag.is_daily() and release == ReleaseType.DAILY:
        return True
    if tag.is_regular() and release == ReleaseType.REGULAR:
        return True
    return False


for tag in changelog_conf["discard_tag"]:
    discard_tag.append(Tag(tag).name())

for tag in changelog_conf["first_tag"]:
    first_tag.append(Tag(tag).name())
