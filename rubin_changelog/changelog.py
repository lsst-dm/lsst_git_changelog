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
import datetime
import logging
import re
from concurrent.futures.thread import ThreadPoolExecutor
from copy import deepcopy
from typing import Dict

from dateutil.parser import parse
from sortedcontainers import SortedDict, SortedList

from .eups import EupsData
from .github import GitHubData
from .jira import JiraData
from .rst import Writer
from .tag import Tag, ReleaseType, matches_release

log = logging.getLogger("changelog")


class ChangeLog:
    """class to retrieve and store changelog data"""

    def __init__(self, max_workers: int = 5):
        """
        :param max_workers: `int`
            max number of parallel worker threads to query GitHub data
        """
        self._github_cache = None
        self._max_workers = max_workers

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
        pulls = gh.get_pull_requests(repo)
        tags = gh.get_tags(repo)
        result["repo"] = repo
        result["pulls"] = pulls
        result["tags"] = tags
        del gh
        return result

    def _get_package_repos(self, products: SortedList) -> SortedDict:
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
        result = SortedDict()
        result['pulls'] = SortedDict()
        result["tags"] = SortedDict()
        gh = GitHubData()
        repos = gh.get_repos()
        del gh
        repo_list = SortedList()
        for product in products:
            if product in repos:
                repo_list.add(product)
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
        return result

    def get_package_repos(self, products: SortedList, release: ReleaseType) -> SortedDict:
        """Retrieves tag and pull information from GitHub

        Parameters
        ----------
        products : `SortedList`
            list of GitHub repos
        release : `ReleaseType`
            release type WEEKLY or REGULAR

        Returns
        -------
        repos: `SortedDict`
            sorted dictionary with the release name
            as key containing all tags and pulls

        """
        if self._github_cache is None:
            self._github_cache = self._get_package_repos(products)
        else:
            log.info("Using cached github data")
        cache = self._github_cache
        result = SortedDict()
        result["tags"] = SortedDict()
        result['pulls'] = cache['pulls']
        repo_result = SortedDict()
        for repo in cache['tags']:
            repo_result[repo] = list()
            for tag in cache['tags'][repo]:
                rtag = Tag(tag["name"])
                if rtag.is_valid() and matches_release(rtag, release):
                    repo_result[repo].append(tag)
        result["tags"] = repo_result
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
        pull_list = repos['pulls']
        tag_list = repos['tags']
        result = SortedDict()
        last_tag_date = None
        for pkg in tag_list:
            log.info("Processing %s", pkg)
            pulls = pull_list[pkg]
            tags = tag_list[pkg]
            for tag in tags:
                rtag = Tag(tag['name'])
                name = rtag.rel_name()
                if name not in result:
                    result[name] = dict()
                    result[name]['tickets'] = list()
                target = tag["target"]
                if 'tagger' in target:
                    date = target['tagger']['date']
                else:
                    date = target['authoredDate']
                tag_date = parse(date)
                if last_tag_date is None or tag_date > last_tag_date:
                    last_tag_date = tag_date
                result[name]['date'] = date
                current_pulls = deepcopy(pulls)
                for merged_at in current_pulls:
                    pull_date = parse(merged_at)
                    title = pulls[merged_at]
                    if pull_date <= tag_date:
                        ticket = self._ticket_number(title)
                        del pulls[merged_at]
                        # skip all pulls before package was added
                        if not(rtag in package_diff and pkg in package_diff[rtag]["added"]):
                            result[name]['tickets'].append({
                                'product': pkg, 'title': title, 'date': merged_at, 'ticket': ticket
                            })
                    else:
                        break
            for merged_at in pulls:
                title = pulls[merged_at]
                date = datetime.datetime.now().isoformat()
                ticket = self._ticket_number(title)
                # use ~main for sorting to put it after any other tag
                if '~main' not in result:
                    result['~main'] = dict()
                    result['~main']['tickets'] = list()
                    result['~main']["date"] = date
                if parse(merged_at) > last_tag_date:
                    result['~main']['tickets'].append({
                        'product': pkg, 'title': title, 'date': merged_at, 'ticket': ticket
                    })
        return result

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
        eups = EupsData()
        eups_data = eups.get_releases(release)
        package_diff = self.get_package_diff(release)
        products = eups_data['products']
        log.info("Fetching JIRA ticket data")
        jira = JiraData()
        jira_data = jira.get_tickets()
        log.info("Fetching GitHub repo data")
        repos = self.get_package_repos(products, release)
        log.info("Processing changelog data")
        repo_data = self.get_merged_tickets(repos, package_diff)
        log.info("Writing RST files")
        outputdir = 'source/releases'
        if release == ReleaseType.WEEKLY:
            outputdir = 'source/weekly'
        writer = Writer(outputdir)
        writer.write_products(products)
        writer.write_releases(jira_data, repo_data, package_diff)
