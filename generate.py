from datetime import UTC, datetime
from textwrap import fill

from changelog.models import Config, Package, ReleaseConfig, Ticket
from changelog.processing import fetch_changelog_data, generate_changelog
from changelog.tag import Tag


def format_packages(packages: dict[str, Package], width: int = 30) -> str:
    """Format package names as comma-separated list with line breaks.
    Packages with URLs are shown as Markdown links.
    Line breaks are calculated using only the package name length.
    """
    lines = []
    current_line = []
    current_length = 0  # Tracks length using only package names

    for pkg_name, package in packages.items():
        # Create display text (Markdown link if URL exists)
        display_text = f"[{pkg_name}]({package.url})" if package.url else pkg_name

        # Calculate space needed (using only package name length)
        needed_length = len(pkg_name) + (2 if current_line else 0)  # +2 for ", "

        if current_line and (current_length + needed_length > width):
            lines.append(", ".join(current_line))
            current_line = [display_text]
            current_length = len(pkg_name)
        else:
            current_line.append(display_text)
            current_length += needed_length if current_length else len(pkg_name)

    if current_line:
        lines.append(", ".join(current_line))

    return "<br>".join(lines)


def create_markdown_table(data: dict[str, Ticket], width: int = 50) -> str:
    """Generate a Markdown table from the ticket data"""
    headers = ["Ticket", "Summary", "Last Merge Date", "Packages"]
    rows = []

    for ticket_id, ticket in data.items():
        ticket_link = f"[{ticket_id}](https://ls.st/{ticket_id})"
        wrapped_summary = fill(ticket.summary, width=width).replace("\n", "<br>")
        formatted_packages = format_packages(ticket.packages)
        date = f'<div style="min-width:200px">{ticket.latest_date}</div>' if ticket.latest_date else ""

        rows.append([ticket_link, wrapped_summary, date, formatted_packages])

    # Create the table
    markdown = "| " + " | ".join(headers) + " |\n"
    markdown += "|" + "|".join(["---"] * len(headers)) + "|\n"

    for row in rows:
        markdown += "| " + " | ".join(str(cell) for cell in row) + " |\n"

    return markdown


def get_entries(entries_dict):
    result = dict()
    previous_release_tag = None
    previous_major_release_tag = None

    for name, entries in entries_dict.items():
        if not entries:
            continue

        for entry in entries:
            current_last_tag = entry.last_tag

            if current_last_tag.is_release():
                if previous_release_tag is not None:
                    if current_last_tag.is_major_release():
                        lower_bound = previous_major_release_tag \
                                      if previous_major_release_tag is not None else previous_release_tag
                    else:
                        lower_bound = previous_release_tag

                    result[current_last_tag] = (lower_bound, current_last_tag)

                previous_release_tag = current_last_tag

                # Update previous major release if this is a major release
                if current_last_tag.is_major_release():
                    previous_major_release_tag = current_last_tag
            elif current_last_tag.is_daily() or current_last_tag.is_weekly():
                if previous_release_tag is not None:
                    result[current_last_tag] = (previous_release_tag, current_last_tag)
                previous_release_tag = current_last_tag
    return result

def get_release_tags(entries_dict):
    result = dict()
    for name, entries in entries_dict.items():
        if not entries:
            continue

        # Start with the first entry
        combined_first = entries[0].first_tag
        combined_last = entries[0].last_tag

        for entry in entries[1:]:
            current_first_tag = entry.first_tag
            current_last_tag = entry.last_tag
            combined_last_tag = combined_last

            current_last = current_last_tag

            if not combined_last_tag.is_release():
                combined_last = current_last
            else:
                result[combined_last] = (combined_first, combined_last)
                combined_first = current_first_tag
                combined_last = current_last
        result[combined_last] = (combined_first, combined_last)
    return result

def format_package_diff(start: Tag, end: Tag, eups_tags: dict) -> str:
    start_pkgs = set(eups_tags.get(start, []))
    end_pkgs = set(eups_tags.get(end, []))
    added = sorted(end_pkgs - start_pkgs)
    removed = sorted(start_pkgs - end_pkgs)

    if not added and not removed:
        return ""

    lines = []
    if added:
        lines.append(f"**Packages added ({len(added)}):** {', '.join(added)}")
    if removed:
        lines.append(f"**Packages removed ({len(removed)}):** {', '.join(removed)}")
    return "\n\n".join(lines) + "\n\n"


def get_release_date(end, eups_tags: dict, git) -> str | None:
    for pkg in eups_tags.get(end, []):
        if end.is_daily():
            return None
        date = git.get_tag_date(pkg, end.git_name())
        if date:
            return date
    return None


def write_doc(release, shared_data, filename, include_head_diff=False):
    with open(filename, "w") as f:
        generated_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"*Last updated: {generated_at}*\n\n", file=f)
        result = get_entries(release)
        first = True

        if include_head_diff and result:
            last_key = max(result.keys())
            rel_config = ReleaseConfig(start_ref=last_key.git_name(), end_ref="HEAD", eups_tag=last_key)
            tickets = generate_changelog(rel_config, shared_data)
            print("## HEAD\n\n", file=f)
            print(f"**Tickets merged:** {len(tickets)}\n\n", file=f)
            print(create_markdown_table(tickets), file=f)
            first = False

        for key, (start, end) in reversed(result.items()):
            if not first:
                print("---\n\n", file=f)
            first = False
            rel_config = ReleaseConfig(start_ref=start.git_name(), end_ref=end.git_name(), eups_tag=key)
            tickets = generate_changelog(rel_config, shared_data)
            release_date = get_release_date(end, shared_data.release_data.eups_tags, shared_data.git)
            heading = f"## {key.name()}"
            print(f"{heading}\n\n", file=f)
            if release_date:
                print(f"**Release date:** {release_date}\n\n", file=f)
            print(f"**Tickets merged:** {len(tickets)}\n\n", file=f)
            pkg_diff = format_package_diff(start, end, shared_data.release_data.eups_tags)
            if pkg_diff:
                print(pkg_diff, file=f)
            print(create_markdown_table(tickets), file=f)

config = Config()
shared_data = fetch_changelog_data(config)
releases = shared_data.release_data
write_doc(releases.releases, shared_data, 'docs/releases/index.md')
write_doc(releases.weekly_releases, shared_data, 'docs/weekly/index.md', include_head_diff=True)
write_doc(releases.daily_releases, shared_data, 'docs/daily/index.md', include_head_diff=True)
