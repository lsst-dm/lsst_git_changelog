from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from sortedcontainers import SortedDict
import os


class GitHubData:
    def __init__(self):
        token = os.getenv('GITHUB_TOKEN')
        self._headers = {"Authorization": f"Bearer {token}"}
        self._transport = AIOHTTPTransport(
            url='https://api.github.com/graphql', headers=self._headers)
        self._client = Client(transport=self._transport)
        self._cached_tags = None

    def _query(self, query: gql, w1: str, w2: str) -> list:
        result = list()
        next_cursor = None
        while True:
            res = self._client.execute(query, variable_values={'cursor': next_cursor})
            for r in res[w1][w2]["nodes"]:
                result.append(r)
            page_info = res[w1][w2]["pageInfo"]
            next_cursor = page_info["endCursor"]
            if not page_info['hasNextPage']:
                break
        return result

    def get_pull_requests(self, repo: str) -> SortedDict:
        query = gql(
            """
            query pull_list($cursor: String) {
                repository(owner: "lsst", name: "%s") {
                    pullRequests(first: 100, after: $cursor) {
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                        nodes {
                            baseRefName
                            title
                            mergedAt
                        }
                    }
                }
            }
            """ % repo)
        pull_requests = SortedDict()
        result = self._query(query, "repository", "pullRequests")
        for r in result:
            if r["mergedAt"] is not None \
                    and r['baseRefName'] in ["main", "master"]:
                pull_requests[r['mergedAt']] = r['title']
        return pull_requests

    def get_repos(self) -> list:
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
        res = self._query(query, "repositoryOwner", "repositories")
        for r in res:
            result.append(r["name"].lower())
        return result

    def get_tags(self, repo: str) -> list:
        query = gql(
            """
            query tag_list($cursor: String)
            {
              repository(owner: "lsst", name: "%s") {
                refs(first: 90, after: $cursor, refPrefix: "refs/tags/") {
                  pageInfo {
                        hasNextPage
                        endCursor
                      }
                  nodes {
                    name
                    target {
                      ... on Tag {
                        tagger {
                          date
                        }
                      }
                      ... on Commit {
                          authoredDate
                      }
                    }
                  }
                }
              }
            }
            """ % repo)
        tags = list()
        result = self._query(query, "repository", "refs")
        for r in result:
            tags.append(r)
        return tags
