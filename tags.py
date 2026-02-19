from pprint import pprint

from changelog.releases import ReleaseData

release_data = ReleaseData()
releases = release_data.get_releases()
pprint(releases)
