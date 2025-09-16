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
from jira import JIRA


class JiraData(object):
    """Class to retrieve JIRA ticket data"""

    def __init__(self):
        pass

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
        JIRA_URL = "https://rubinobs.atlassian.net"

        jira = JIRA(
            server=JIRA_URL,
        )
        results = dict()
        for issue in jira.search_issues(f"project = {project}", maxResults=0, fields="summary"):
            results[issue.key] = issue.fields.summary
        return results
