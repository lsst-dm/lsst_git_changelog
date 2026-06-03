import logging

from changelog.git import Git, MergeEntry
from changelog.github import PullRequest, get_pull_requests_multi
from changelog.jira import JiraData, JiraTicket
from changelog.models import ChangelogData, Config, Package, ReleaseConfig, Ticket
from changelog.releases import ReleaseData
from changelog.tag import ReleaseType
from changelog.utils import get_repo_list, read_repo_default_branches, read_repo_yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def find_pr_by_branch(ticket_branch: str, pr_data: dict[str, PullRequest]) -> str | None:
    """Find a pull-request URL by matching the source branch name.

    Parameters
    ----------
    ticket_branch : str
        Branch name to search for.
    pr_data : dict[str, PullRequest]
        Mapping of commit SHA to ``PullRequest`` for a single repository.

    Returns
    -------
    str or None
        URL of the first matching pull request, or ``None`` if not found.
    """
    for pr_sha, pr in pr_data.items():
        if ticket_branch == pr.head_branch:
            return pr.url
    return None


def find_pr_url(pr_number: str, pr_data: dict[str, PullRequest]) -> str | None:
    """Find a pull-request URL by PR number.

    Parameters
    ----------
    pr_number : str
        GitHub PR number (matched against the trailing path segment of
        ``PullRequest.url``).
    pr_data : dict[str, PullRequest]
        Mapping of commit SHA to ``PullRequest`` for a single repository.

    Returns
    -------
    str or None
        URL of the matching pull request, or ``None`` if not found.
    """
    for pr_sha, pr in pr_data.items():
        if str(pr_number) == pr.url.split("/")[-1]:
            return pr.url
    return None


def resolve_pr_url(
    merge_info: MergeEntry, pkg: str, all_prs: dict[str, dict[str, PullRequest]]
) -> tuple[str | None, str]:
    """Resolve the best available pull-request URL for a merge commit.

    Tries, in order: PR number match → commit SHA match → branch name match →
    fall back to the raw PR number.

    Parameters
    ----------
    merge_info : MergeEntry
        Parsed merge-commit data (commit SHA, PR number, branch name).
    pkg : str
        Package name used to look up the per-repository PR data.
    all_prs : dict[str, dict[str, PullRequest]]
        Full PR cache in ``repo_name → commit_sha → PullRequest`` form.

    Returns
    -------
    tuple[str or None, str]
        A 2-tuple of ``(url, branch)`` where *url* is the resolved PR URL
        (or raw PR number as a fallback, or ``None``), and *branch* is the
        merge target / source branch.
    """
    pr_data = all_prs.get(pkg)
    if pr_data is None:
        return None, merge_info.branch

    branch = merge_info.branch
    pr_number = merge_info.pr_number
    url = None

    # Try to find PR by number first
    if pr_number is not None:
        url = find_pr_url(pr_number, pr_data)
        if url:
            return url, branch

    # Try to find PR by commit SHA
    if merge_info.commit in pr_data:
        pr_entry = pr_data[merge_info.commit]
        return pr_entry.url, pr_entry.branch

    # Try to find PR by branch name
    url = find_pr_by_branch(branch, pr_data)
    if url is None:
        # logging.warning(
        #    f"Package: {pkg}, ticket={merge_info.ticket_number} "
        #    f"branch={branch} not found in GitHub PRs"
        # )
        # Fall back to PR number if available

        return pr_number, branch
    return url, branch


def process_merges_to_tickets(
    merges: dict[str, list[MergeEntry]],
    all_prs: dict[str, dict[str, PullRequest]],
    jira_tickets: dict[str, JiraTicket],
    release_packages: set[str],
    packages: dict[str, Package]
) -> dict[str, Ticket]:
    """Aggregate merge commits into per-ticket summaries with PR links.

    Parameters
    ----------
    merges : dict[str, list[MergeEntry]]
        Mapping of package name to its list of merge commits for the release
        range.
    all_prs : dict[str, dict[str, PullRequest]]
        Full PR cache in ``repo_name → commit_sha → PullRequest`` form.
    jira_tickets : dict[str, JiraTicket]
        Mapping of ticket identifier to ``JiraTicket`` (used for summaries).
    release_packages : set[str]
        Package names included in this release; merges from other packages
        are ignored.

    Returns
    -------
    dict[str, Ticket]
        Mapping of Jira ticket identifier to ``Ticket`` containing the
        summary and per-package ``Package`` entries.
    """
    tickets: dict[str, Ticket] = {}

    for pkg, merge_list in merges.items():
        # Skip packages not in this release
        if pkg not in release_packages:
            continue

        for merge_info in merge_list:
            ticket_number = merge_info.ticket_number

            # Skip merges without ticket numbers
            if ticket_number is None:
                # logging.warning(
                #    f"No ticket found for package {pkg}, "
                #    f"commit: {merge_info.commit}, "
                #    f"branch: {merge_info.branch}, "
                #    f"pr_number: {merge_info.pr_number}"
                # )
                continue

            # Skip tickets not in Jira
            if ticket_number not in jira_tickets:
                logging.warning(f"Ticket {ticket_number} not found in Jira")
                continue
            # Resolve PR URL
            pr_url, branch = resolve_pr_url(merge_info, pkg, all_prs)
            pr_url = pr_url.rsplit('/', 1)[-1] if isinstance(pr_url, str) else pr_url
            p = packages[pkg].removesuffix(".git")
            pr_url = f"{p}/pull/{pr_url}" if pr_url else None
            if ticket_number not in tickets:
                tickets[ticket_number] = Ticket(summary=jira_tickets[ticket_number].summary, packages={})

            if pkg not in tickets[ticket_number].packages:
                tickets[ticket_number].packages[pkg] = Package(url=pr_url, merge_branch=branch)

            if merge_info.date is not None:
                current = tickets[ticket_number].latest_date
                if current is None or merge_info.date > current:
                    tickets[ticket_number].latest_date = merge_info.date

    return tickets


def generate_changelog(config: ReleaseConfig, shared_data: ChangelogData) -> dict[str, Ticket]:
    """Generate a complete changelog for a single release range.

    Orchestrates the full pipeline: clones/updates all git repositories,
    fetches Jira tickets and GitHub PRs, collects merge commits, and
    assembles them into a ticket-keyed mapping.

    Parameters
    ----------
    config : ReleaseConfig
        Release configuration specifying the git range, EUPS tag, and
        cache locations.
    shared_data : SharedChangelogData, optional
        Pre-loaded shared data (release data, packages, Jira tickets, PRs).
        If None, all data will be fetched fresh.

    Returns
    -------
    dict[str, Ticket]
        Mapping of Jira ticket identifier to ``Ticket`` for all tickets
        touched in the release range.
    """
    release_packages = shared_data.release_data.eups_tags[config.eups_tag]
    logging.info(f"Getting merge information from {config.start_ref} to {config.end_ref}")
    merges = shared_data.git.get_all_merges(release_packages, config.start_ref, config.end_ref)

    logging.info(f"Release includes {len(release_packages)} packages")

    logging.info("Processing merges into tickets")
    tickets = process_merges_to_tickets(
        merges=merges,
        all_prs=shared_data.all_prs,
        jira_tickets=shared_data.jira_tickets,
        release_packages=release_packages,
        packages=shared_data.package_list
    )

    logging.info(f"Processed {len(tickets)} tickets")

    return tickets





def fetch_changelog_data(config: Config) -> ChangelogData:
    """Load all shared data needed for changelog generation.

    This function loads data that is common across all releases:
    - Git repositories
    - Jira tickets
    - GitHub pull requests
    - Release/package metadata

    Parameters
    ----------
    config : Config
        Configuration (used for cache paths and DB locations).

    Returns
    -------
    ChangelogData
        Container with all pre-loaded shared data.
    """
    logging.info("Loading release data")
    release_data = ReleaseData().get_releases()

    logging.info("Loading package list")
    package_list = read_repo_yaml()

    logging.info("Initializing Git repositories")
    git = Git(config.cache_dir, package_list, release_data.all_packages)
    git.clone_all_packages()
    git.update_all_heads(read_repo_default_branches())
    git.tag_all(release_data.tag_list[ReleaseType.DAILY])

    logging.info("Getting branches from Git repositories")
    branches = git.get_all_branches()
    logging.info(f"Retrieved branches for {len(branches)} packages")

    logging.info(f"Initializing Jira data from {config.jira_db}")
    jira = JiraData(db_path=config.jira_db)
    logging.info("Fetching Jira tickets")
    jira_tickets = jira.get_tickets()
    logging.info(f"Retrieved {len(jira_tickets)} Jira tickets")

    logging.info("Loading repository list")
    repos = get_repo_list()
    logging.info(f"Retrieved {len(repos)} repositories")

    logging.info("Fetching pull requests from GitHub")
    all_prs = get_pull_requests_multi(
        repo_list=repos,
        cache_file=config.pr_cache,
    )
    logging.info(f"Retrieved pull requests for {len(all_prs)} repositories")

    return ChangelogData(
        release_data=release_data,
        git=git,
        jira_tickets=jira_tickets,
        all_prs=all_prs,
        package_list=package_list,
    )
