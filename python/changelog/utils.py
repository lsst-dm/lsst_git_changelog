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
import requests
import yaml


def read_repo_yaml() -> dict[str, str]:
    """Fetch the LSST repository list from GitHub and parse it.

    Downloads ``lsst/repos/etc/repos.yaml`` from the ``main`` branch and
    flattens it into a name → URL mapping. Entries that are plain strings
    are kept as-is; dict entries with a ``url`` key have that value extracted.

    Returns
    -------
    dict[str, str]
        Mapping of package name to its git remote URL.

    Raises
    ------
    requests.HTTPError
        If the HTTP request fails.
    """
    url = "https://raw.githubusercontent.com/lsst/repos/refs/heads/main/etc/repos.yaml"
    response = requests.get(url)
    response.raise_for_status()

    # Parse YAML
    data = yaml.safe_load(response.text)

    # Print or use the data
    result = {}
    for key, value in data.items():
        if isinstance(value, dict) and "url" in value:
            result[key] = value["url"]
        else:
            result[key] = value
    return result


def get_repo_list() -> list[tuple[str, str]]:
    """Return all repositories as ``(owner, repo)`` tuples.

    Fetches the repo YAML via ``read_repo_yaml`` and splits each URL on
    ``"/"`` to extract the owner and repository name.

    Returns
    -------
    list[tuple[str, str]]
        List of ``(owner, repo_name)`` pairs derived from the remote URLs.
    """
    package_list = read_repo_yaml()

    repos = []
    for _, url in package_list.items():
        s = url.split("/")
        repos.append((s[-2], s[-1].removesuffix(".git")))
    return repos
