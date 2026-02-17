from concurrent.futures import ThreadPoolExecutor

import subprocess
import time
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional
import re

@dataclass
class MergeEntry:
    commit: str
    pr_number: Optional[int]
    ticket_number: Optional[int]
    branch: Optional[str]

class Git:
    def __init__(self, repo_path, package_list, repo_list, max_workers=8):
        self.repo_path = Path(repo_path)
        self.package_list = package_list
        self.repo_list = repo_list
        self.max_workers = max_workers

    @staticmethod
    def _run_git(cmd, retries=3, delay=2):
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

    def clone_or_update(self, repo_url, retries=3):
        repo_path = Path(self. repo_path)

        if not repo_path.exists():
            # fresh clone
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

        # update existing clone
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


    def clone_all_packages(self):
        all_packages = self.repo_list
        def task(name):
            if name in self.package_list:
                self.clone_or_update(self.package_list[name], f"{self.repo_path}/{name}")

        # You can adjust max_workers depending on your system and network
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(task, all_packages)

    def get_tags(self, repo_name):
        repo_path = Path(self.repo_path) / repo_name
        if not repo_path.exists():
            raise RuntimeError(f"Repository {repo_name} does not exist at {repo_path}")
        print(repo_path)
        output = self._run_git(["git", "-C", str(repo_path), "tag"])
        print(output)
        return output.splitlines()

    def get_branches(self, repo_name):
        repo_path = Path(self.repo_path) / repo_name
        if not repo_path.exists():
            raise RuntimeError(f"Repository {repo_name} does not exist at {repo_path}")
        cmd = ["git", "-C", str(repo_path), "branch"]
        output = self._run_git(cmd)
        # clean up the branch names
        branches = [line.strip().replace("origin/", "") for line in output.splitlines()]
        return branches

    def get_all_tags(self, max_workers=8):
        repo_names = self.repo_list

        def task(name):
            try:
                return name, self.get_tags(name)
            except Exception as e:
                return name, []

        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for name, tags in executor.map(task, repo_names):
                results[name] = tags
        return results

    def get_all_branches(self):
        repo_names = self.repo_list

        def task(name):
            try:
                return name, self.get_branches(name)
            except Exception as e:
                return name, []

        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for name, branches in executor.map(task, repo_names):
                results[name] = branches
        return results


    def get_merges(
            self, name: str, begin_tag: str, end_tag: str
    ) -> List[MergeEntry]:
        git_cmd = [
            "git",
            "-C",
            str(self.repo_path / name),
            "log",
            f"{begin_tag}..{end_tag}",
            "--merges",
            "--oneline"
        ]
        output = self._run_git(git_cmd)

        entries: List[MergeEntry] = []
        for line in output.strip().splitlines():
            commit = line[:8]
            message = line[9:]  # rest of line after hash + space
            pr_match = re.search(r'Merge pull request #(\d+)', message)
            pr_number = int(pr_match.group(1)) if pr_match else None
            ticket_match = re.search(r'DM-(\d+)', message)
            ticket_number = int(ticket_match.group(1)) if ticket_match else None
            branch = None
            from_match = re.search(r'from [^/]+/(\S+)', message)
            if from_match:
                branch = from_match.group(1)
            branch_match = re.search(r"Merge branch '([^']+)'", message)
            if branch_match:
                branch = branch_match.group(1)

            entries.append(MergeEntry(commit, pr_number, ticket_number, branch))

        return entries

    def get_all_merges(self, begin_tag: str, end_tag: str) -> List[MergeEntry]:
        repo_names = self.repo_list

        def task(name):
            print(name)
            try:
                return name, self.get_merges(name, begin_tag, end_tag)
            except Exception as e:
                return name, None

        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for name, branches in executor.map(task, repo_names):
                if branches is None:
                    continue
                results[name] = branches
        return results



