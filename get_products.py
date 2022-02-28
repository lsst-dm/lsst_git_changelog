#!/usr/bin/env python

import logging

from rubin_changelog.eups import EupsData
from rubin_changelog.tag import *

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
log = logging.getLogger("changelog")
log.setLevel(logging.INFO)
eups_data = EupsData()
for p in eups_data.get_releases(ReleaseType.WEEKLY)['products']:
    print(p)
