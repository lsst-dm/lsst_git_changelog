from dataclasses import dataclass

from changelog.git import Git
from changelog.releases import ReleaseData
from changelog.tag import Tag


@dataclass
class Package:
    """A single package referenced in a Jira ticket.

    Attributes
    ----------
    url : str
        Full URL or bare PR number for the associated pull request.
    merge_branch : str
        Branch name that was merged for this package change.
    """

    url: str
    merge_branch: str


@dataclass
class Ticket:
    """A Jira ticket with its associated package changes.

    Attributes
    ----------
    packages : dict[str, Package]
        Mapping of package name to its pull-request details.
    summary : str or None
        Ticket summary text fetched from Jira, or ``None`` if unavailable.
    latest_date : str or None
        Date (YYYY-MM-DD) of the most recent merge commit for this ticket,
        across all packages.
    """

    packages: dict[str, Package]
    summary: str | None
    latest_date: str | None = None


@dataclass
class ReleaseConfig:
    """Configuration for generating a single release changelog.

    Attributes
    ----------
    start_ref : str
        Git ref (tag or commit SHA) marking the start of the range (exclusive).
    end_ref : str
        Git ref (tag or commit SHA) marking the end of the range (inclusive).
    eups_tag : Tag
        Parsed EUPS ``Tag`` object representing this release.
    """

    start_ref: str
    end_ref: str
    eups_tag: Tag

@dataclass
class Config:
    """Configuration for generating a single release changelog.

    Attributes
    ----------
    cache_dir : str, optional
        Directory used for git clone caches.
        Default is ``"./cache/git"``.
    jira_db : str, optional
        Path to the SQLite Jira cache database.
        Default is ``"./cache/jira.sqlite"``.
    pr_cache : str, optional
        Path to the combined PR cache JSON file.
        Default is ``"./cache/combined_cache.json"``.
    """

    cache_dir: str = "./cache/git"
    jira_db: str = "./cache/jira.sqlite"
    pr_cache: str = "./cache/combined_cache.json"

@dataclass
class ChangelogData:
    """Shared data that can be reused across multiple changelog generations."""

    release_data: ReleaseData
    git: Git
    jira_tickets: dict
    all_prs: dict
    package_list: dict
