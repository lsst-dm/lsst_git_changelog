import os
import json
import asyncio
from pathlib import Path
from typing import List, Dict, Union, Tuple
from dataclasses import dataclass

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport


@dataclass(frozen=True)
class PullRequest:
    branch: str
    head_branch: str
    title: str
    url: str
    merged_at: str


def _load_cache(cache_file: Path) -> Dict[str, Dict[str, PullRequest]]:
    """
    Loads cache.

    Supports:
    OLD format: SHA -> PR
    NEW format: repo -> SHA -> PR
    """

    if not cache_file.exists():
        return {}

    with open(cache_file, "r") as f:
        raw = json.load(f)

    if not raw:
        return {}

    result: Dict[str, Dict[str, PullRequest]] = {}

    for repo, sha_map in raw.items():
        result[repo] = {
            sha: PullRequest(**data)
            for sha, data in sha_map.items()
        }

    return result


def _save_cache(
        cache_file: Path,
    data: Dict[str, Dict[str, PullRequest]],
):
    serializable = {
        repo: {sha: pr.__dict__ for sha, pr in sha_map.items()}
        for repo, sha_map in data.items()
    }

    cache_file.parent.mkdir(parents=True, exist_ok=True)

    with open(cache_file, "w") as f:
        json.dump(serializable, f, indent=2)


async def _query(
        session,
    query,
    variables: Dict,
    what: List[str],
) -> List[Dict]:

    result = []
    next_cursor = None

    while True:
        variables["cursor"] = next_cursor
        res = await session.execute(query, variable_values=variables)

        for w in what:
            res = res[w]

        result.extend(res["nodes"])

        page_info = res["pageInfo"]
        next_cursor = page_info["endCursor"]

        if not page_info["hasNextPage"]:
            break

    return result


async def _fetch_repo(
        session,
    owner: str,
    repo: str,
) -> Union[Dict[str, Dict[str, PullRequest]], None]:

    query = gql(
        """
        query pull_list($cursor: String) {
            repository(owner: "%s", name: "%s") {
                pullRequests(
                    first: 100,
                    after: $cursor,
                    states: MERGED,
                    orderBy: {field: UPDATED_AT, direction: ASC}
                ) {
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
                        }
                    }
                }
            }
        }
        """
        % (owner, repo)
    )

    try:
        result = await _query(
            session,
            query,
            variables={"cursor": None},
            what=["repository", "pullRequests"],
        )
    except Exception:
        print("failed", owner, repo)
        return None

    pr_by_sha: Dict[str, PullRequest] = {}

    for r in result:
        if not r["mergeCommit"]:
            continue

        merge_sha = r["mergeCommit"]["oid"]

        branch = r["baseRefName"]
        if branch == "master":
            branch = "main"
        if branch.startswith("v"):
            branch = branch[1:]

        pr_by_sha[merge_sha] = PullRequest(
            branch=branch,
            head_branch=r["headRefName"],
            title=r["title"],
            url=r["url"],
            merged_at=r["mergedAt"],
        )

    return {repo: pr_by_sha}


class GitHubData:
    """Query GitHub repo data with JSON caching."""

    def __init__(self):
        token = os.getenv("AUTH_TOKEN")
        if not token:
            raise RuntimeError("AUTH_TOKEN environment variable not set")

        headers = {"Authorization": f"Bearer {token}"}
        transport = AIOHTTPTransport(
            url="https://api.github.com/graphql",
            headers=headers,
            ssl=True,
        )

        self._client = Client(
            transport=transport,
            fetch_schema_from_transport=True,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_pull_requests_multi(
        self,
        repo_list: List[Tuple[str, str]],
        cache_file: Union[str, Path],
    ) -> Dict[str, Dict[str, PullRequest]]:

        cache_path = Path(cache_file)

        if cache_path.exists():
            print("Cache exists. Skipping GitHub query.")
            return _load_cache(cache_path)

        async with self._client as session:

            tasks = [
                _fetch_repo(session, owner, repo)
                for owner, repo in repo_list
            ]

            results = await asyncio.gather(*tasks)

        combined: Dict[str, Dict[str, PullRequest]] = {}

        for repo_result in results:
            if repo_result is None:
                continue

            for repo_name, sha_map in repo_result.items():
                combined[repo_name] = sha_map

        _save_cache(cache_path, combined)

        return combined


if __name__ == "__main__":
    from changelog.utils import read_repo_yaml

    package_list = read_repo_yaml()

    repos = []
    for _, url in package_list.items():
        s = url.split("/")
        repos.append((s[-2], s[-1].removesuffix(".git")))

    github = GitHubData()

    all_prs = asyncio.run(
        github.get_pull_requests_multi(
            repo_list=repos,
            cache_file="combined_cache.json",
        )
    )
