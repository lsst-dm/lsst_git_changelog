#!/usr/bin/env python

import textwrap
from pprint import pprint

from rubin_changelog import JiraData
jira = JiraData()
jira_data = jira.get_tickets()

row = list()

for j in jira_data:
    t = textwrap.wrap(jira_data[j], 50)
    row.append([j, t ])

pprint(row)


