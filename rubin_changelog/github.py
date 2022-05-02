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

from typing import List

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from sortedcontainers import SortedDict
import os


class GitHubData:
    """Query GitHub repo data"""

    def __init__(self):
        token = os.getenv('AUTH_TOKEN')
        headers = {"Authorization": f"Bearer {token}"}
        transport = AIOHTTPTransport(
            url='https://api.github.com/graphql', headers=headers)
        self._client = Client(transport=transport)
        self._cached_tags = None

    def _query(self, query: gql, what: List[str]) -> List:
        """Execute a gql query

        Parameters
        ----------
        query : `gql`
            gql query string

        what : `List[str]`
            list of nested query result keywords

        Returns
        -------
        query : `List`
            List of query results

        """
        result = list()
        next_cursor = None
        while True:
            res = self._client.execute(query, variable_values={'cursor': next_cursor})
            for w in what:
                if w in res:
                    res = res[w]
            for r in res["nodes"]:
                result.append(r)
            page_info = res["pageInfo"]
            next_cursor = page_info["endCursor"]
            if not page_info['hasNextPage']:
                break
        return result

    def get_pull_requests(self, repo: str) -> SortedDict:
        """Get all pull requests for a GitHub repo sorted byb merge date

        Parameters
        ----------
        repo : `str`
            repo name

        Returns
        -------
        pull requests : `SortedDict`
            Returns sorted dict mapping 'merge date' : 'merge title'

        """
        query = gql(
            """
            query pull_list($cursor: String) {
                repository(owner: "lsst", name: "%s") {
                    pullRequests(first: 100, after: $cursor) {
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        nodes {
                            baseRefName
                            title
                            mergedAt
                        }
                    }
                }
            }
            """ % repo)
        pull_requests = SortedDict()
        result = self._query(query, ["repository", "pullRequests"])
        branches = list()
        for r in result:
            branch = r['baseRefName']
            if branch == 'master':
                branch = 'main'
            if branch not in branches:
                branches.append(branch)
            if r["mergedAt"] is not None:
                pull_requests[r['mergedAt']] = {'title': r['title'], 'branch': branch}
        return pull_requests, branches

    def get_repos(self) -> List[str]:
        """Retrieve list of repos owned by lsst

        Parameters
        ----------

        Returns
        -------
        repos : `List[str]`
            list of lsst repos

        """
        result = list()
        query = gql(
            """
            query repo_list($cursor: String) {
              repositoryOwner(login: "lsst") {
                repositories(first: 100, after: $cursor) {
                  pageInfo {
                    hasNextPage
                    endCursor
                  }
                  nodes { name }
                }
              }
            }
            """)
        res = self._query(query, ["repositoryOwner", "repositories"])
        for r in res:
            result.append(r["name"].lower())
        return result

    def get_tags(self, repo: str) -> List[str]:
        """Retrieve list of all repo tags

        Parameters
        ----------
        repo : `str`
            repo name

        Returns
        -------
        tags : `List[str]`
            List of tag names

        """
        query = gql(
            """
            query tag_list($cursor: String)
            {
              repository(owner: "lsst", name: "%s") {
                refs(first: 90, after: $cursor, refPrefix: "refs/tags/") {
                  pageInfo {
                        hasNextPage
                        endCursor
                      }
                  nodes {
                    name
                    target {
                      ... on Tag {
                        tagger { date }
                        target { ... on Commit {committedDate}}
                      }
                      ... on Commit {committedDate}
                    }
                  }
                }
              }
            }
            """ % repo)
        tags = list()
        result = self._query(query, ["repository", "refs"])
        for r in result:
            tags.append(r)
        return tags
