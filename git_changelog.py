#!/usr/bin/env python

import logging
import argparse

from rubin_changelog import ChangeLog
from rubin_changelog.tag import *

parser = argparse.ArgumentParser()
parser.add_argument('-n', type=int, default=5, dest='workers', help="Number of connection workers")
args = parser.parse_args()

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
log = logging.getLogger("changelog")
log.setLevel(logging.INFO)
changelog = ChangeLog(args.workers)
changelog.create_changelog(ReleaseType.WEEKLY)
changelog1 = ChangeLog(args.workers)
changelog1.create_changelog(ReleaseType.REGULAR)
