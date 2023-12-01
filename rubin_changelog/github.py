#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

import os
import re
from typing import List, Tuple, Union, Any

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from sortedcontainers import SortedDict

import yaml
import requests
from conf import patch_merges


class GitHubData:
    """Query GitHub repo data"""

    def __init__(self):
        token = os.getenv('AUTH_TOKEN')
        headers = {"Authorization": f"Bearer {token}"}
        transport = AIOHTTPTransport(
            url='https://api.github.com/graphql', headers=headers)
        self._client = Client(transport=transport)

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

    def get_pull_requests(self, owner: str, repo: str) -> Tuple[SortedDict, List[Union[str, Any]]]:
        """Get all pull requests for a GitHub repo sorted byb merge date

        Parameters
        ----------
        owner : `str`
            repo owner
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
                repository(owner: "%s", name: "%s") {
                    pullRequests(first: 100, after: $cursor) {
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        nodes {
                            baseRefName
                            headRefName
                            title
                            mergedAt
                            url
                            mergeCommit {
                              oid
                              committedDate
                            }
                        }
                    }
                }
            }
            """ % (owner, repo))
        pull_requests = SortedDict()
        result = self._query(query, ["repository", "pullRequests"])
        branches = list()
        for r in result:
            branch = r['baseRefName']
            if branch == 'master':
                branch = 'main'
            if branch.startswith('v'):
                branch = branch.replace('v', '')
            if branch not in branches:
                branches.append(branch)
            if branch not in pull_requests:
                pull_requests[branch] = SortedDict()
            mergedAt = r['mergedAt']
            mergedBranch = r['headRefName']
            mergeCommit = r['mergeCommit']
            title = r['title']
            url = r['url']
            oid = None
            committedDate = mergedAt
            if mergeCommit:
                oid = mergeCommit['oid']
                committedDate = mergeCommit["committedDate"]
            if mergedBranch.startswith('tickets/'):
                mergedBranch = mergedBranch.split('/')[1]
            if url in patch_merges:
                oid = patch_merges[url]['oid']
                committedDate = patch_merges[url]['committedDate']
            if committedDate is not None:
                pull_requests[branch][committedDate] = mergedBranch, oid, url, title
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
            result.append(r["name"])
        return result

    def get_tags(self, owner: str, repo: str) -> List[str]:
        """Retrieve list of all repo tags

        Parameters
        ----------
        owner : `str`
            repo owner
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
              repository(owner: "%s", name: "%s") {
                refs(first: 90, after: $cursor, refPrefix: "refs/tags/") {
                  pageInfo {
                        hasNextPage
                        endCursor
                      }
                   nodes {
                    name
                    ... on Ref {
                      target {
                        ... on Commit {oid committedDate}
                      }
                    }
                    target {
                      ... on Tag {
                      tagger {date}
                        target {
                        ... on Commit {committedDate oid}
                        }
                      }
                    }
                  }
                }
              }
            }
            """ % (owner, repo))
        tags = list()
        result = self._query(query, ["repository", "refs"])

        for r in result:
            if 'target' not in r["target"]:
                oid = r["target"]["oid"]
                committedDate = r["target"]["committedDate"]
                r["target"].update({'tagger': {'date': committedDate}})
                r["target"]['target'] = {'oid': oid, 'committedDate': committedDate}
                r["target"].pop('oid')
                r['target'].pop('committedDate')
            tags.append(r)
        return tags

    @staticmethod
    def get_repo_yaml():
        result = dict()
        reverse_lookup = dict()
        url = 'https://raw.githubusercontent.com/lsst/repos/main/etc/repos.yaml'
        response = requests.get(url)
        content = response.content.decode("utf-8")
        repo_list = yaml.safe_load(content)
        for key, value in repo_list.items():
            url = value
            ref = 'main'
            lfs = False
            if isinstance(value, dict) and 'url' in value:
                url = value['url']
                if 'ref' in value:
                    ref = value['ref']
                if 'lfs' in value:
                    lfs = value['lfs']
            match = re.search(r"https://github.com/([\w\-]+)/([\w\-]+)(.git)*", url)
            org = match.groups()[0]
            repo = match.groups()[1]
            result[key] = (org, repo, ref, lfs)
            reverse_lookup[repo] = key
        return result, reverse_lookup
