# 25.0.1.rc1 was tagged from the wrong base tags
changelog_conf = {
    "discard_tag": ["25.0.1.rc1"],
    "first_tag": ["25.0.1.rc2"]
}

patch_merges = {
    # botched PR, merge succeeded but PR was closed from
    # the github web UI as the merge appeared stuck
    "https://github.com/lsst-sitcom/summit_utils/pull/26":
        {'committedDate': '2022-11-09T01:38:20Z',
         'oid': 'd68dba151e745583c2858637e14230aeea8506b2'},
    # testdata_deblender didn't seem to follow our github flow
    "https://github.com/lsst/testdata_deblender/pull/4":
        {'oid': '8af30cc6eeffc3554cc7b3aee2e66f46f21279de',
         "committedDate": "2020-01-16T16:54:00Z"},
}
