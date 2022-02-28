#!/usr/bin/env python

import logging
from pprint import pprint

from rubin_changelog.changelog import ChangeLog
from rubin_changelog.eups import EupsData
from rubin_changelog.tag import *

logging.basicConfig(format='[%(asctime)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
log = logging.getLogger("changelog")
log.setLevel(logging.INFO)
eups_data = EupsData()
eups_data = eups_data.get_releases(ReleaseType.REGULAR)
change_log = ChangeLog(eups_data)
diff = get_package_diff()
for d in diff:
    print(d.hash(), d.rel_name())
    pprint(diff[d])
