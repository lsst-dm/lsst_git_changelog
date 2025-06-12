#!/usr/bin/env python
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

import logging
import argparse

from rubin_changelog import ChangeLog, ReleaseType

parser = argparse.ArgumentParser()
parser.add_argument('-n', type=int, default=5, dest='workers', help="Number of connection workers")
parser.add_argument('-d', dest='debug', default=False, action='store_true', help="Enable debug mode")
parser.add_argument('-u', dest='merge', default=True, action='store_false',
                    help="Do not merge release candidates")
parser.add_argument('-f', dest='first', default=True, action='store_false',
                    help="Do not merge first release RC")
parser.add_argument('-w', dest='weekly', default=False, action='store_true',
                    help="Create only weekly changelog")
parser.add_argument('-r', dest='release', default=False, action='store_true',
                    help="Create only regular release changelog")
parser.add_argument('-o', type=str, default='', dest='release_notes',
                    help="Generate ticket change release notes")
args = parser.parse_args()

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y-%m-%dT%H:%M:%S%z')
log = logging.getLogger("changelog")
log.setLevel(logging.INFO)
changelog = ChangeLog(args.workers)
if args.debug:
    changelog.set_debug()
release = ReleaseType.ALL
if args.release:
    release = ReleaseType.REGULAR
elif args.weekly:
    release = ReleaseType.WEEKLY
changelog.create_changelog(release, args.merge, args.first, args.release_notes)
