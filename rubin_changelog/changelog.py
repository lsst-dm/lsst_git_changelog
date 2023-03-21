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

import concurrent
import json
import logging
import os
import re
from concurrent.futures.thread import ThreadPoolExecutor
from pprint import pprint
from typing import Dict
from .rst import RstRelease
from sortedcontainers import SortedDict, SortedList

from .eups import EupsData
from .github import GitHubData
from .jira import JiraData
from .tag import Tag, ReleaseType, matches_release

log = logging.getLogger("changelog")


def valid_branch(name):
    if name in ("main", "master"):
        return True
    match = re.search(r'v?\d+\.\d+\.x', name)
    return match is not None


def valid_ticket(name):
    match = re.search(r'.*DM-(\d+).*', name)
    if match is not None:
        return match.groups()[0]
    return None


def sort_tags(tags):
    result = list()
    sorted_list = SortedList()
    for tag in tags:
        sorted_list.add(Tag(tag))
    for tag in sorted_list:
        name = tag.name()
        if tag.is_release():
            name = name.replace('v', '')
        result.append(name)
    return result


class ChangeLogData:
    def __init__(self, github, jira_tickets):
        self.github = github
        self.jira_tickets = jira_tickets
        self.github['~untagged'] = dict()
        self.tags = github['tags']
        self.pulls = github['pulls']

    @staticmethod
    def _process_tags(tag_list, pull_list, release):
        tag_dict = dict()
        for t in tag_list:
            target = t['target']
            if 'target' not in target:
                continue
            name = t["name"]
            rel_tag = Tag(name)
            if not rel_tag.is_valid():
                continue
            if not matches_release(rel_tag, release):
                continue
            if target['target'] is None:
                continue
            if 'oid' not in target['target']:
                continue
            oid = target['target']['oid']
            committedDate = target['target']['committedDate']

            if oid not in tag_dict:
                tag_dict[oid] = list(([name], committedDate))
            else:
                tag_dict[oid][0].append(name)

        result = dict()
        for name, pull_branch in pull_list.items():
            if not valid_branch(name):
                continue
            if name not in result:
                result[name] = SortedDict()

            for pb in pull_branch.items():
                date = pb[0]
                ticket = pb[1][0]
                oid = pb[1][1]
                url = pb[1][2]
                tags = None
                if oid in tag_dict:
                    tags = tag_dict[oid]
                if tags is not None:
                    print(tags[0], sort_tags(tags[0]))
                result[name][date] = (ticket, tags, url)
        # pprint(result)
        return result

    def process(self, releaseType):
        results = dict()
        results['~untagged'] = dict()

        for pkg, tag in self.tags.items():
            pull = self.pulls[pkg]
            merges = self._process_tags(tag, pull, releaseType)
            for branch in merges.keys():
                current = '~untagged'
                merge = merges[branch]
                for merge_date in reversed(merge):
                    item = merge[merge_date]
                    ticket_nr = valid_ticket(item[0])
                    ticket_title = None
                    if f'DM-{ticket_nr}' in self.jira_tickets:
                        ticket_title = self.jira_tickets[f'DM-{ticket_nr}']
                    tags = item[1]
                    url = item[2]
                    first_tag = None
                    if tags is not None:
                        first_tag = tags[0][0]
                    if first_tag is not None:
                        current = first_tag
                        if current not in results:
                            results[current] = dict()
                    if ticket_nr not in results[current]:
                        results[current][ticket_nr] = dict()
                    if branch not in results[current][ticket_nr]:
                        results[current][ticket_nr][branch] = [None, SortedList(), "1970-01-01T00:00Z"]
                    results[current][ticket_nr][branch][0] = ticket_title
                    results[current][ticket_nr][branch][1].add((pkg, url))
                    if results[current][ticket_nr][branch][2] < merge_date:
                        results[current][ticket_nr][branch][2] = merge_date

        return results


class ChangeLog:
    """class to retrieve and store changelog data"""

    def __init__(self, max_workers: int = 5):
        """
        :param max_workers: `int`
            max number of parallel worker threads to query GitHub data
        """
        self._max_workers = max_workers
        self._debug = False

    @staticmethod
    def get_package_diff(release: ReleaseType) -> SortedDict:
        """Retrieve added/removed products

        Parameters
        ----------
        release: `ReleaseType`
            release type: WEEKLY or REGULAR

        Returns
        -------
        packages: `SortedDict`
            sorted dict of release name with lists of
            added and removed packages

        """
        eups = EupsData()
        eups_data = eups.get_releases(release)
        result = SortedDict()
        releases = eups_data['releases']
        last_release = None
        for r in releases:
            if last_release is not None:
                previous_pkgs = [sub['package'] for sub in last_release]
                pkgs = [sub['package'] for sub in releases[r]]
                removed = SortedList(set(previous_pkgs) - set(pkgs))
                added = SortedList(set(pkgs) - set(previous_pkgs))
                result[r] = {'added': added, 'removed': removed}
            last_release = releases[r]
        return result

    @staticmethod
    def _fetch(repo: str) -> Dict:
        """helper function to fetch repo data

        Parameters
        ----------
        repo: `str`
            name of GitHub repo

        Returns
        -------
        _fetch: `Dict`
            dictionary with pulls and tags of a given repo

        """
        log.info("Fetching %s", repo)
        retries = 5
        result = dict()
        for i in range(retries):
            try:
                gh = GitHubData()
                pulls, branches = gh.get_pull_requests(repo)
                tags = gh.get_tags(repo)
                result["repo"] = repo
                result["pulls"] = pulls
                result["tags"] = tags
                result['branches'] = branches
                del gh
                return result
            except:
                log.info("Fetch failed for %s -- retry %d", repo, i)
        return result

    def _get_package_repos(self, products: SortedList) -> dict:
        """retrieve repos for a list of products

        Parameters
        ----------
        products: `SortedList`
            sorted list of products

        Returns
        -------
        repos: `SortedDict`
            package repo data

        """
        result = dict()
        result['pulls'] = SortedDict()
        result["tags"] = SortedDict()
        result['branches'] = SortedDict()
        result["repos"] = list()

        if self._debug and os.path.exists("github_debug.json"):
            f = open('github_debug.json')
            data = json.load(f)
            result['pulls'] = SortedDict(data["pulls"])
            result["tags"] = SortedDict(data["tags"])
            result['branches'] = SortedDict(data["branches"])
            result["repos"] = list(data["repos"])
            return result
        gh = GitHubData()
        repos = gh.get_repos()
        result["repos"] = repos
        del gh
        repo_list = SortedList()
        # create a lowercase list for GitHub/EUPS package comparison
        # GitHub will be used for the package name
        products_lower = [k.lower() for k in products]
        for repo in repos:
            if repo.lower() in products_lower:
                repo_list.add(repo)
        with ThreadPoolExecutor(
                max_workers=self._max_workers) as executor:
            futures = {executor.submit(self._fetch, repo): repo for repo in repo_list}
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                except Exception:
                    log.error("Fetch failed")
                else:
                    repo = data["repo"]
                    result["pulls"][repo] = data["pulls"]
                    result["tags"][repo] = data["tags"]
                    result["branches"][repo] = data['branches']
        if self._debug:
            with open("github_debug.json", "w") as outfile:
                outfile.write(json.dumps(result))
        return result

    @staticmethod
    def _ticket_number(title: str) -> int:
        """helper function to map a JIRA ticket string to an integer

        Parameters
        ----------
        title: `str`
            JIRA ticket string, DM-XXXXXX

        Returns
        -------
            ticket number : `int`
                numeric part of DM-XXXXXX

        """
        match = re.search(r'DM[\s*|-](\d+)', title.upper())
        ticket = None
        if match:
            ticket = int(match[1])
        return ticket

    def set_debug(self):
        self._debug = True

    def create_changelog(self, release: ReleaseType) -> None:
        """Process data sources and Write RST changelog files

        Parameters
        ----------
        release: `ReleaseType`
            release type: WEEKLY or REGULAR

        Returns
        -------

        """
        log.info("Fetching JIRA data")
        jira = JiraData()
        jira_tickets = jira.get_tickets()
        log.info("Fetching EUPS data")
        eups = EupsData()
        eups_data, package_diff, products, all_products = eups.get_releases(ReleaseType.REGULAR)
        data = self._get_package_repos(all_products)
        changelog = ChangeLogData(data, jira_tickets)
        releases = changelog.process(ReleaseType.REGULAR)
        rst = RstRelease(ReleaseType.REGULAR, releases, products)
        rst.write()
