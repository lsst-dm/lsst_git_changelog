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

from typing import Dict

import requests


class JiraData(object):
    """Class to retrieve JIRA ticket data"""

    def __init__(self):
        self._url = 'https://rubinobs.atlassian.net/rest/api/2/search'
        self._headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self._perPage = 1000

    def get_tickets(self) -> Dict[str, str]:
        dm = self.get_project_tickets("DM")
        sp = self.get_project_tickets("SP")
        return sp | dm

    def get_project_tickets(self, project: str) -> Dict[str, str]:
        """Get all tickets and summary messages or a given project

        Parameters
        ----------
        project : str
            JIRA project like DM or SP

        Returns
        -------
        tickets : `Dict[str, str]`
            returns a dictionary ticket: summary message

        """
        start_at = 0
        per_page = self._perPage
        url = self._url
        results = dict()
        while True:
            req_url = (f"{url}?jql=project={project}&startAt={start_at}"
                       f"&maxResults={per_page}"
                       "&fields=key,summary")
            res = requests.get(req_url, headers=self._headers)
            res_json = res.json()
            total = res_json['total']
            maxResults = res_json['maxResults']
            start_at = start_at + maxResults
            for r in res_json['issues']:
                results[r['key']] = r['fields']['summary']
            if start_at >= total:
                break
        return results
