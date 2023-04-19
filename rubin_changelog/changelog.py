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

import pytz
from dateutil import parser
from sortedcontainers import SortedDict, SortedList, SortedSet

from .eups import EupsData
from .github import GitHubData
from .jira import JiraData
from .rst import RstRelease
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

    @staticmethod
    def _process_tags(tag_list, pull_list, release):
        tag_dict = dict()
        tag_dates = dict()
        for t in tag_list:
            target = t['target']
            if 'target' not in target:
                continue
            name = t["name"]
            rtag = Tag(name)
            if rtag.is_regular() and name.startswith('v'):
                name = name.replace('v', '')
            tagDate = None
            if 'tagger' in target:
                tagDate = target['tagger']['date']
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
            if name not in tag_dates:
                tag_dates[name] = tagDate
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
                title = pb[1][3]
                tags = None
                if oid in tag_dict:
                    tags = tag_dict[oid]
                if tags is not None:
                    tags[0] = sort_tags(tags[0])
                result[name][date] = (ticket, tags, url, title)
        return result, tag_dates

    def process(self, releaseType):
        results = dict()
        results['~untagged'] = dict()
        tag_dates = dict()
        for pkg, tag in self.tags.items():
            pull = self.pulls[pkg]
            merges, dates = self._process_tags(tag, pull, releaseType)
            for rel, date_str in dates.items():
                date = parser.parse(date_str).astimezone(pytz.utc)
                if rel not in tag_dates:
                    tag_dates[rel] = (date, date.strftime("%Y-%m-%dT%H:%M:%SZ"))
                elif tag_dates[rel][0] < date:
                    tag_dates[rel] = (date, date.strftime("%Y-%m-%dT%H:%M:%SZ"))
            for branch in merges.keys():
                current = '~untagged'
                merge = merges[branch]
                for merge_date in reversed(merge):
                    item = merge[merge_date]
                    ticket_nr = valid_ticket(item[0])
                    title = item[3]
                    title_ticket_nr = self._ticket_number(title)
                    if ticket_nr is None and title_ticket_nr is not None:
                        ticket_nr = title_ticket_nr
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
                        results[current][ticket_nr][branch] = [
                            None, SortedList(), "1970-01-01T00:00Z", "1970-01-01T00:00Z"]
                    results[current][ticket_nr][branch][0] = ticket_title
                    results[current][ticket_nr][branch][1].add((pkg, url))
                    if results[current][ticket_nr][branch][2] < merge_date:
                        results[current][ticket_nr][branch][2] = merge_date
        return results, tag_dates


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
    def _fetch(owner, repo: str) -> Dict:
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
        log.info("Fetching %s/%s", owner, repo)
        retries = 5
        result = dict()
        for i in range(retries):
            try:
                gh = GitHubData()
                pulls, branches = gh.get_pull_requests(owner, repo)
                tags = gh.get_tags(owner, repo)
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
        result['repos'] = list()
        if self._debug and os.path.exists("github_debug.json"):
            f = open('github_debug.json')
            data = json.load(f)
            result['pulls'] = SortedDict(data["pulls"])
            result["tags"] = SortedDict(data["tags"])
            result['branches'] = SortedDict(data["branches"])
            result["repos"] = data["repos"]
            result["repo_map"] = data["repo_map"]
            return result
        gh = GitHubData()
        repos, reverse_lookup = gh.get_repo_yaml()
        result["repos"] = repos
        result["repo_map"] = reverse_lookup
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
            futures = {executor.submit(self._fetch, repos[repo][0], repos[repo][1]): repo for repo in repo_list}
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

    def set_debug(self):
        self._debug = True

    @staticmethod
    def merge_releases(release_data, package_diff):
        releases = SortedList()
        last_rel = dict()
        result = dict()
        packages = dict()
        for tag in release_data[1].keys():
            releases.add(Tag(tag))
        for rel in releases:
            rel_type, version = rel.desc()
            if rel_type != 'regular' or version[0] < 16:
                continue
            last_rel[rel.base_name()] = rel
        for tag, data in release_data[0].items():
            rtag = Tag(tag)
            if not rtag.is_valid() or rtag.desc()[1][0] < 16:
                continue
            if data is None:
                continue
            name = last_rel[rtag.base_name()]
            name = name.name()
            if name not in result:
                result[name] = dict()
            result[name].update(data)
        for tag, data in package_diff[ReleaseType.REGULAR].items():
            if not tag.is_valid() or tag.desc()[1][0] < 16:
                continue
            if tag.base_name() not in last_rel:
                continue
            name = last_rel[tag.base_name()]
            if name not in packages:
                packages[name] = {"pkgs": SortedSet(), "removed": SortedSet(), "added": SortedSet()}
            packages[name]["pkgs"].update(data["pkgs"])
            packages[name]["removed"].update(data["removed"])
            packages[name]["added"].update(data["added"])
        return result, packages

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
        releases = [release]
        if release == ReleaseType.ALL:
            releases = [ReleaseType.REGULAR, ReleaseType.WEEKLY]
        eups = EupsData()
        eups_data, package_diff, products, all_products = eups.get_releases(release)
        data = self._get_package_repos(all_products)
        repo_data = data['repos']
        repo_map = data['repo_map']
        changelog = ChangeLogData(data, jira_tickets)
        for r in releases:
            release_data = changelog.process(r)
            if r == ReleaseType.REGULAR:
                n = self.merge_releases(release_data, package_diff)
                release_data = n[0], release_data[1]
                package_diff[ReleaseType.REGULAR] = n[1]
            rst = RstRelease(r, release_data, repo_data, repo_map, products, package_diff)
            rst.write()
