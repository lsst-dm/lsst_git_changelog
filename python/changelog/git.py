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
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from changelog.tag import Tag


@dataclass
class MergeEntry:
    """A single merge commit extracted from git log.

    Attributes
    ----------
    commit : str
        Full commit SHA of the merge commit.
    pr_number : int or None
        GitHub pull-request number parsed from the merge message, or
        ``None`` if not found.
    ticket_number : str or None
        Jira ticket identifier (e.g. ``"DM-12345"``) parsed from the
        merge message, or ``None`` if not found.
    branch : str or None
        Source branch name parsed from the merge message, or ``None``
        if not found.
    date : str or None
        Commit date in ``YYYY-MM-DD`` format, or ``None`` if unavailable.
    """

    commit: str
    pr_number: int | None
    ticket_number: str | None
    branch: str | None
    date: str | None = None


class Git:
    """Manage a collection of bare git repositories for changelog generation.

    Parameters
    ----------
    repo_path : str or Path
        Root directory under which bare repositories are stored.
    package_list : dict[str, str]
        Mapping of package name to its remote clone URL.
    repo_list : list[str]
        Ordered list of package names to operate on.
    max_workers : int, optional
        Maximum number of concurrent threads for parallel git operations.
        Default is 8.
    """

    def __init__(
        self,
        repo_path: str | Path,
        package_list: dict[str, str],
        repo_list: list[str],
        max_workers: int = 8,
    ) -> None:
        self.repo_path = Path(repo_path)
        self.package_list = package_list
        self.repo_list = repo_list
        self.max_workers = max_workers

    @staticmethod
    def _run_git(cmd: list[str], retries: int = 3, delay: int = 2) -> str:
        """Run a git command with automatic retry on transient failures.

        Parameters
        ----------
        cmd : list[str]
            Full command as a list of strings, e.g.
            ``["git", "-C", "/path", "fetch"]``.
        retries : int, optional
            Maximum number of attempts. Default is 3.
        delay : int, optional
            Base delay in seconds between attempts (multiplied by attempt
            number). Default is 2.

        Returns
        -------
        str
            Combined stdout of the completed command.

        Raises
        ------
        subprocess.CalledProcessError
            If the command fails with exit code 128 (fatal git error) or
            exhausts all retries.
        """
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                result = subprocess.run(
                    cmd,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                return result.stdout
            except subprocess.CalledProcessError as e:
                if e.returncode == 128:
                    raise
                last_exc = e
                if attempt == retries:
                    raise
                time.sleep(delay * attempt)
        raise last_exc

    def clone_or_update(self, name: str, repo_url: str, retries: int = 3) -> str:
        """Clone a bare repository or fetch updates if it already exists.

        Parameters
        ----------
        name : str
            Package name used as the subdirectory name under ``repo_path``.
        repo_url : str
            Remote URL to clone from.
        retries : int, optional
            Number of git retry attempts. Default is 3.

        Returns
        -------
        str
            ``"cloned"`` if a new bare repository was created, or
            ``"updated"`` if an existing repository was fetched.

        Raises
        ------
        RuntimeError
            If the target directory exists but is not a valid bare git
            repository.
        """
        repo_path = Path(self.repo_path, name)
        if not repo_path.exists():
            self._run_git(
                [
                    "git",
                    "clone",
                    "--bare",
                    "--filter=blob:none",
                    repo_url,
                    str(repo_path),
                ],
                retries=retries,
            )
            return "cloned"

        # sanity check: make sure it's a bare repo
        config = repo_path / "config"
        if not config.exists():
            raise RuntimeError(f"{repo_path} exists but is not a git repo")

        self._run_git(
            [
                "git",
                "-C",
                str(repo_path),
                "fetch",
                "--prune",
                "--filter=blob:none",
                "--tags",
                "origin",
            ],
            retries=retries,
        )
        return "updated"

    def clone_all_packages(self) -> None:
        """Clone or update all packages in ``repo_list`` in parallel.

        Only packages that have an entry in ``package_list`` are processed;
        others are silently skipped.
        """
        all_packages = self.repo_list

        def task(name):
            if name in self.package_list:
                self.clone_or_update(name, self.package_list[name])

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(task, all_packages)

    def tag(self, repo_name: str, tag_name: str, sha: str) -> list[str]:
        """Create or force-update a tag in a single repository.

        Parameters
        ----------
        repo_name : str
            Name of the package / repository subdirectory.
        tag_name : str
            Name of the tag to create or overwrite.
        sha : str
            Commit SHA the tag should point to.

        Returns
        -------
        list[str]
            Output lines from ``git tag -f``.

        Raises
        ------
        RuntimeError
            If the repository directory does not exist under ``repo_path``.
        """
        repo_path = Path(self.repo_path) / repo_name
        if not repo_path.exists():
            raise RuntimeError(f"Repository {repo_name} does not exist at {repo_path}")
        output = self._run_git(["git", "-C", str(repo_path), "tag", "-f", tag_name, sha])
        return output.splitlines()

    def tag_all(self, tags: dict, max_workers: int | None = None) -> None:
        """Create or force-update tags across all repositories in parallel.

        Parameters
        ----------
        tags : dict[Tag, list[tuple[str, str]]]
            Mapping of ``Tag`` to a list of ``(repo_name, sha)`` pairs.
        max_workers : int or None, optional
            Thread-pool size. ``None`` uses the Python default.
        """
        def task(args):
            tag, p = args
            self.tag(p[0], tag.git_name(), p[1])

        tasks = [(tag, p) for tag, pkg in tags.items() for p in pkg]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(task, tasks)

    def get_tags(self, repo_name: str) -> list[str]:
        """List all tags in a single repository.

        Parameters
        ----------
        repo_name : str
            Name of the package / repository subdirectory.

        Returns
        -------
        list[str]
            Tag names, one per element.

        Raises
        ------
        RuntimeError
            If the repository directory does not exist.
        """
        repo_path = Path(self.repo_path) / repo_name
        if not repo_path.exists():
            raise RuntimeError(f"Repository {repo_name} does not exist at {repo_path}")
        output = self._run_git(["git", "-C", str(repo_path), "tag"])
        return output.splitlines()

    def get_branches(self, repo_name: str) -> list[str]:
        """List all branches in a single repository.

        Parameters
        ----------
        repo_name : str
            Name of the package / repository subdirectory.

        Returns
        -------
        list[str]
            Branch names with ``origin/`` prefix stripped.

        Raises
        ------
        RuntimeError
            If the repository directory does not exist.
        """
        repo_path = Path(self.repo_path) / repo_name
        if not repo_path.exists():
            raise RuntimeError(f"Repository {repo_name} does not exist at {repo_path}")
        cmd = ["git", "-C", str(repo_path), "branch"]
        output = self._run_git(cmd)
        # clean up the branch names
        branches = [line.strip().replace("origin/", "") for line in output.splitlines()]
        return branches

    def get_all_tags(self, max_workers: int = 8) -> dict[str, list[str]]:
        """List tags for every repository in parallel.

        Parameters
        ----------
        max_workers : int, optional
            Thread-pool size. Default is 8.

        Returns
        -------
        dict[str, list[str]]
            Mapping of package name to its list of tag names. Repositories
            that raise an exception return an empty list.
        """
        repo_names = self.repo_list

        def task(name):
            try:
                return name, self.get_tags(name)
            except Exception:
                return name, []

        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for name, tags in executor.map(task, repo_names):
                results[name] = tags
        return results

    def get_all_branches(self) -> dict[str, list[str]]:
        """List branches for every repository in parallel.

        Returns
        -------
        dict[str, list[str]]
            Mapping of package name to its list of branch names. Repositories
            that raise an exception return an empty list.
        """
        repo_names = self.repo_list

        def task(name):
            try:
                return name, self.get_branches(name)
            except Exception:
                return name, []

        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for name, branches in executor.map(task, repo_names):
                results[name] = branches
        return results

    def get_tag_date(self, repo_name: str, tag_name: str) -> str | None:
        """Return the date of a tag as YYYY-MM-DD, or None if not found."""
        repo_path = Path(self.repo_path) / repo_name
        if not repo_path.exists():
            return None
        try:
            output = self._run_git([
                "git", "-C", str(repo_path),
                "for-each-ref", f"refs/tags/{tag_name}",
                "--format=%(creatordate:format:%Y-%m-%d)",
            ])
            return output.strip() or None
        except Exception:
            return None

    def update_head(self, repo_name: str, ref: str = "main") -> None:
        """Update the HEAD symbolic ref in a single bare repository.

        Parameters
        ----------
        repo_name : str
            Name of the package / repository subdirectory.
        ref : str, optional
            Branch name to point HEAD at. Default is ``"main"``.

        Raises
        ------
        RuntimeError
            If the repository directory does not exist.
        subprocess.CalledProcessError
            If the git command fails.
        """
        repo_path = Path(self.repo_path) / repo_name
        if not repo_path.exists():
            raise RuntimeError(f"Repository {repo_name} does not exist at {repo_path}")
        # Fetch only metadata (no blobs/files)
        self._run_git([
            "git", "-C", str(repo_path),
            "fetch", "--filter=blob:none", "origin", f"{ref}:refs/remotes/origin/{ref}",
        ])

        # Now update the refs
        self._run_git([
            "git", "-C", str(repo_path),
            "update-ref", f"refs/heads/{ref}", f"refs/remotes/origin/{ref}",
        ])

        self._run_git([
            "git", "-C", str(repo_path),
            "symbolic-ref", "HEAD", f"refs/heads/{ref}",
        ])



    def update_all_heads(
        self,
        default_branches: dict[str, str] | None = None,
        fallback: str = "main",
    ) -> dict[str, Exception | None]:
        """Update HEAD across all repositories in parallel.

        Parameters
        ----------
        default_branches : dict[str, str] or None, optional
            Per-repo branch names (e.g. from ``read_repo_default_branches``).
            When a repo is not present in the mapping, ``fallback`` is used.
        fallback : str, optional
            Branch name used when ``default_branches`` has no entry for a
            repo. Default is ``"main"``.

        Returns
        -------
        dict[str, Exception or None]
            Mapping of package name to ``None`` on success or the exception
            raised on failure.
        """
        branches = default_branches or {}

        def task(name):
            ref = branches.get(name, fallback)
            try:
                self.update_head(name, ref)
                return name, None
            except Exception as exc:
                return name, exc

        results: dict[str, Exception | None] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for name, exc in executor.map(task, self.repo_list):
                results[name] = exc
        return results

    def get_merges(self, name: str, begin_tag: str, end_tag: str) -> list[MergeEntry]:
        """Return all merge commits in a repository between two refs.

        Parameters
        ----------
        name : str
            Package / repository subdirectory name.
        begin_tag : str
            Starting ref (exclusive) — typically the previous release tag.
        end_tag : str
            Ending ref (inclusive) — typically the current release tag.

        Returns
        -------
        list[MergeEntry]
            Parsed merge entries in log order (most recent first).
        """
        git_cmd = [
            "git",
            "-C",
            str(self.repo_path / name),
            "log",
            f"{begin_tag}..{end_tag}",
            "--merges",
            "--format=%H\t%cI\t%s",
        ]
        output = self._run_git(git_cmd)

        entries: list[MergeEntry] = []
        for line in output.strip().splitlines():
            parts = line.split("\t", 2)
            commit = parts[0]
            raw_date = parts[1] if len(parts) > 1 else None
            date_str = None
            if raw_date:
                try:
                    dt = datetime.fromisoformat(raw_date)
                    date_str = dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    date_str = raw_date
            message = parts[2] if len(parts) > 2 else ""
            pr_match = re.search(r"Merge pull request #(\d+)", message)
            pr_number = int(pr_match.group(1)) if pr_match else None
            ticket_match = re.search(r"(?:DM|SP|RSO)-(\d+)", message)
            ticket_number = f"{ticket_match.group(0)}" if ticket_match else None
            branch = None
            from_match = re.search(r"from [^/]+/(\S+)", message)
            if from_match:
                branch = from_match.group(1)
            branch_match = re.search(r"Merge branch '([^']+)'", message)
            if branch_match:
                branch = branch_match.group(1)

            entries.append(MergeEntry(commit, pr_number, ticket_number, branch, date_str))

        return entries

    def get_all_merges(self, packages, begin_tag: str, end_tag: str) -> dict[str, list[MergeEntry]]:
        """Return merge commits for every repository in parallel.

        Parameters
        ----------
        begin_tag : str
            Starting ref (exclusive) passed to ``get_merges``.
        end_tag : str
            Ending ref (inclusive) passed to ``get_merges``.

        Returns
        -------
        dict[str, list[MergeEntry]]
            Mapping of package name to its merge entries. Repositories that
            raise an exception or have no merges are omitted.
        """
        repo_names = packages

        def task(name):
            try:
                result =  self.get_merges(name, begin_tag, end_tag)
            except Exception:
                result = None
            if not result and Tag(begin_tag).is_release():
                # retry for releases with a leading 'v'
                try:
                    result = self.get_merges(name, f"v{begin_tag}", f"v{end_tag}")
                except Exception:
                    result = None
            return name, result

        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for name, branches in executor.map(task, repo_names):
                if branches is None:
                    continue
                results[name] = branches
        return results
