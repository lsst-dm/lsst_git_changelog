import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PullRequest:
    """A GitHub pull request with merge metadata.

    Attributes
    ----------
    branch : str
        Base branch the PR was merged into (``"master"`` normalised to
        ``"main"``; leading ``"v"`` stripped from version branches).
    head_branch : str
        Source branch the PR was opened from.
    title : str
        Pull-request title.
    url : str
        HTML URL of the pull request on GitHub.
    merged_at : str or None
        ISO 8601 timestamp of the merge, or ``None`` for open/closed-unmerged
        PRs.
    """

    branch: str
    head_branch: str
    title: str
    url: str
    merged_at: str


def _load_cache(cache_file: Path) -> dict[str, dict[str, PullRequest]]:
    """Load the pull-request JSON cache from disk.

    Parameters
    ----------
    cache_file : Path
        Path to the JSON cache file. Returns an empty dict if the file does
        not exist.

    Returns
    -------
    dict[str, dict[str, PullRequest]]
        Mapping of ``repo_name → commit_sha → PullRequest``.
    """
    if not cache_file.exists():
        return {}

    with open(cache_file) as f:
        raw = json.load(f)

    if not raw:
        return {}

    result: dict[str, dict[str, PullRequest]] = {}
    for repo, sha_map in raw.items():
        result[repo] = {sha: PullRequest(**data) for sha, data in sha_map.items()}

    return result


def _save_cache(cache_file: Path, data: dict[str, dict[str, PullRequest]]) -> None:
    """Persist the pull-request cache to disk as JSON.

    Parameters
    ----------
    cache_file : Path
        Destination file path. Parent directories are created if absent.
    data : dict[str, dict[str, PullRequest]]
        Cache data in ``repo_name → commit_sha → PullRequest`` form.
    """
    serializable = {repo: {sha: pr.__dict__ for sha, pr in sha_map.items()} for repo, sha_map in data.items()}

    cache_file.parent.mkdir(parents=True, exist_ok=True)

    with open(cache_file, "w") as f:
        json.dump(serializable, f, indent=2)


def _fetch_repo(owner: str, repo: str) -> dict[str, dict[str, PullRequest]] | None:
    """Fetch all pull requests for a repository from the GitHub REST API.

    Reads the ``AUTH_TOKEN`` environment variable for authentication and
    paginates through all PR states.

    Parameters
    ----------
    owner : str
        GitHub organisation or user name.
    repo : str
        Repository name (without ``.git`` suffix).

    Returns
    -------
    dict[str, dict[str, PullRequest]] or None
        Single-key dict ``{repo: {commit_sha: PullRequest}}``, or ``None``
        if the request fails.

    Raises
    ------
    RuntimeError
        If the ``AUTH_TOKEN`` environment variable is not set.
    """
    token = os.getenv("AUTH_TOKEN")
    if not token:
        raise RuntimeError("AUTH_TOKEN environment variable not set")

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    base_url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
    per_page = 100
    page = 1
    pr_by_sha: dict[str, PullRequest] = {}

    while True:
        params = {"state": "all", "per_page": per_page, "page": page}
        response = requests.get(base_url, headers=headers, params=params)

        if response.status_code != 200:
            logger.warning(f"Failed to fetch {owner}/{repo}: {response.status_code} {response.text}")
            return None

        prs = response.json()
        if not prs:
            break

        for r in prs:
            merge_sha = r.get("merge_commit_sha")
            if not merge_sha:
                continue

            branch = r["base"]["ref"]
            if branch == "master":
                branch = "main"
            if branch.startswith("v"):
                branch = branch[1:]

            pr_by_sha[merge_sha] = PullRequest(
                branch=branch,
                head_branch=r["head"]["ref"],
                title=r["title"],
                url=r["html_url"],
                merged_at=r.get("merged_at"),
            )

        if "Link" in response.headers and 'rel="next"' in response.headers["Link"]:
            page += 1
        else:
            break

    return {repo: pr_by_sha}


def get_pull_requests_multi(
    repo_list: list[tuple[str, str]],
    cache_file: str | Path,
) -> dict[str, dict[str, PullRequest]]:
    """Fetch pull requests for multiple repositories, with caching.

    If the cache file already exists the data is loaded from it directly.
    Otherwise all repositories are fetched in parallel and the result is
    saved to the cache file.

    Parameters
    ----------
    repo_list : list[tuple[str, str]]
        List of ``(owner, repo)`` pairs to fetch.
    cache_file : str or Path
        Path to the JSON cache file.

    Returns
    -------
    dict[str, dict[str, PullRequest]]
        Mapping of ``repo_name → commit_sha → PullRequest`` for all
        successfully fetched repositories.
    """
    cache_path = Path(cache_file)

    if cache_path.exists():
        logger.info("Cache exists. Loading from cache.")
        return _load_cache(cache_path)
    with ThreadPoolExecutor(max_workers=8) as executor:
        # Map each repo to a future
        future_to_repo = {
            executor.submit(_fetch_repo, owner, repo): (owner, repo) for owner, repo in repo_list
        }
        combined: dict[str, dict[str, PullRequest]] = {}
        for future in as_completed(future_to_repo):
            owner, repo = future_to_repo[future]
            try:
                repo_result = future.result()
                if repo_result:
                    combined.update(repo_result)
                    logger.info(f"Fetched PRs for {owner}/{repo}.")
            except Exception as e:
                logger.error(f"Failed fetching PRs for {owner}/{repo}: {e}")

    _save_cache(cache_path, combined)
    return combined
