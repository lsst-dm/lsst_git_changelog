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
from typing import Dict

from dateutil.parser import parse
from sortedcontainers import SortedDict, SortedList, SortedSet

from .eups import EupsData
from .github import GitHubData
from .jira import JiraData
from .rst import Writer
from .tag import Tag, ReleaseType, matches_release

log = logging.getLogger("changelog")


def process_package_repos(products: SortedList, all_repos: dict, release: ReleaseType) -> SortedDict:
    """Retrieves tag and pull information from GitHub

    Parameters
    ----------
    products : `SortedList`
        list of GitHub repos
    all_repos : `dict`
        list of all repos from github
    release : `ReleaseType`
        release type WEEKLY or REGULAR

    Returns
    -------
    repos: `SortedDict`
        sorted dictionary with the release name
        as key containing all tags and pulls

    """
    cache = dict()
    cache["pulls"] = SortedDict()
    cache["branches"] = SortedDict()
    cache["tags"] = SortedDict()
    for p in products:
        if p in all_repos["pulls"]:
            cache["pulls"][p] = all_repos["pulls"][p]
            cache["branches"][p] = all_repos["branches"][p]
            cache["tags"][p] = all_repos["tags"][p]
    result = SortedDict()
    result["tags"] = SortedDict()
    result['pulls'] = cache['pulls']
    result['branches'] = cache['branches']
    repo_result = SortedDict()
    for repo in cache['tags']:
        repo_result[repo] = SortedDict()
        for tag in cache['tags'][repo]:
            rtag = Tag(tag["name"])
            if rtag.is_valid() and matches_release(rtag, release):
                target = tag["target"]
                if 'target' in target:
                    last_commit = target['target']['committedDate']
                else:
                    # handle special case like w.2016.34
                    last_commit = target['committedDate']
                tag_date = last_commit
                if 'tagger' in target:
                    tag_date = target['tagger']['date']
                repo_result[repo][rtag] = {'tag_date': tag_date, 'last_commit': last_commit}
    result["tags"] = repo_result
    return result


def _create_tag_list(repos: Dict):
    # find all tags
    result = SortedDict()
    tag_list = repos['tags']
    for p in tag_list:
        tags = tag_list[p]
        for tag in tags:
            if tag.is_regular():
                tag_base = tag.base_name()
                if tag_base not in result:
                    result[tag_base] = SortedSet()
                result[tag_base].add(tag)
    return result


class ChangeLog:
    """class to retrieve and store changelog data"""

    def __init__(self, max_workers: int = 5):
        """
        :param max_workers: `int`
            max number of parallel worker threads to query GitHub data
        """
        self._github_cache = None
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
        gh = GitHubData()
        result = dict()
        pulls, branches = gh.get_pull_requests(repo)
        tags = gh.get_tags(repo)
        result["repo"] = repo
        result["pulls"] = pulls
        result["tags"] = tags
        result['branches'] = branches
        del gh
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

    def get_merged_tickets(self, repos: Dict, package_diff: SortedDict) -> SortedDict:
        """Process all repo data and create a merged ticket dict

        Parameters
        ----------
        repos : `Dict`
            repo dictionary
        package_diff : `SortedDict`
            added/removed by release

        Returns
        -------
        merged tickets : `SortedDict`
            sorted dictionary of merged tickets

        """
        # find all tags
        result = SortedDict()
        releases = SortedDict()
        tag_list = repos['tags']
        for p in tag_list:
            tags = tag_list[p]
            for tag in tags:
                tag_base = tag.base_name()
                rtag = Tag(tag_base)
                if rtag not in releases:
                    releases[rtag] = dict()
                    releases[rtag]['included'] = SortedSet()
                    releases[rtag]['removed'] = SortedSet()
                    releases[rtag]['added'] = SortedSet()
                    releases[rtag]['branches'] = SortedDict()
                releases[rtag]['included'].add(tag)

        previous = None
        for rtag in releases:
            included = releases[rtag]['included']
            for tag in included:
                eups_tag = Tag(tag.eups_tag())
                if eups_tag in package_diff:
                    releases[rtag]['added'] |= package_diff[eups_tag]['added']
                    releases[rtag]['removed'] |= package_diff[eups_tag]['removed']
            last_tag = included[-1]
            first_tag = included[0]
            releases[rtag]["last_tag"] = last_tag
            tag_type, ver = rtag.desc()
            releases[rtag]['previous'] = previous
            if tag_type == 'weekly':
                releases[rtag]['branches']['main'] = [previous, last_tag]
            elif tag_type == 'regular':
                branch_name = rtag.tag_branch()
                previous_name = None
                if previous in releases:
                    previous_name = releases[previous]["last_tag"]
                # releases pre 23 don't branch
                if ver[0] < 23:
                    releases[rtag]['branches']['main'] = [previous_name, last_tag]
                else:
                    # releases 23 and higher
                    # first release with patch number = 0 (e.g. 23.0.0)
                    if ver[2] == 0:
                        base = Tag(previous.first_name())
                        pname = None
                        if base in releases:
                            pname = releases[base]['branches']['main'][-1]
                        # special case for release 23
                        if ver[0] == 23:
                            pname = previous_name
                        releases[rtag]['branches']['main'] = [pname, first_tag]
                        if (len(included)) > 1:
                            releases[rtag]['branches'][branch_name] = [first_tag, included[-1]]
                    # patch releases can have commits only in release branches
                    else:
                        releases[rtag]['branches'][branch_name] = [previous_name, included[-1]]
            previous = rtag

        tag_list = repos["tags"]
        pulls_list = repos["pulls"]
        for rtag in releases:
            log.info("Processing tag %s", rtag)
            branches = releases[rtag]['branches']
            last_tag = releases[rtag]['last_tag']  # this is the original EUPS tag name
            name = last_tag.name()
            if name not in result:
                result[name] = dict()
                result[name]['tickets'] = list()
                result[name]['date'] = None
            for branch in branches:
                if not (branch.endswith('.x') or branch == 'main'):
                    continue
                first, last = branches[branch]
                for pkg in tag_list:
                    tags = tag_list[pkg]
                    pulls = pulls_list[pkg]
                    if not (first in tags and last in tags):
                        continue
                    first_commit_date = parse(tags[first]['last_commit'])
                    last_commit_date = parse(tags[last]['last_commit'])
                    first_tag_date = tags[first]['tag_date']
                    last_tag_date = tags[last]['tag_date']
                    if last_tag_date is None:
                        last_tag_date = '2014-01-01T00:00:00:05:00Z'
                    result[name]['date'] = last_tag_date
                    if first_commit_date == last_commit_date:
                        continue
                    if branch not in pulls:
                        continue
                    for merge_date, title in pulls[branch].items():
                        ticket = self._ticket_number(title)
                        pull_date = parse(merge_date)
                        if (pull_date > parse(first_tag_date)) and (pull_date <= parse(last_tag_date)):
                            result[name]['tickets'].append({
                                'product': pkg, 'title': title,
                                'date': merge_date, 'ticket': ticket, 'branch': branch})
        return result

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
        log.info("Fetching EUPS data")
        releases = [release]
        if release == ReleaseType.ALL:
            releases = [ReleaseType.REGULAR, ReleaseType.WEEKLY]
        eups = EupsData()
        eups_data = dict()
        package_diff = dict()
        products = dict()
        all_products = set()
        for r in releases:
            eups_data[r] = eups.get_releases(r)
            package_diff[r] = self.get_package_diff(r)
            products[r] = eups_data[r]['products']
            all_products |= set(products[r])
        log.info("Fetching JIRA ticket data")
        jira = JiraData()
        jira_data = jira.get_tickets()
        log.info("Fetching GitHub repo data")
        all_repos = self._get_package_repos(SortedList(all_products))
        for r in releases:
            repos = process_package_repos(products[r], all_repos, r)
            log.info("Processing changelog data")
            # repo_data = self.get_merged_tickets_old(repos, package_diff[r])
            repo_data = self.get_merged_tickets(repos, package_diff[r])
            log.info("Writing RST files")
            outputdir = 'source/releases'
            if r == ReleaseType.WEEKLY:
                outputdir = 'source/weekly'
            writer = Writer(outputdir)
            writer.write_products(products[r])
            writer.write_products(products[r])
            writer.write_releases(jira_data, repo_data, package_diff[r])
