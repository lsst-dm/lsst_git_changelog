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
args = parser.parse_args()

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%Y-%m-%dT%H:%M:%S%z')
log = logging.getLogger("changelog")
log.setLevel(logging.INFO)
changelog = ChangeLog(args.workers)
changelog.set_debug()
changelog.create_changelog(ReleaseType.ALL)
