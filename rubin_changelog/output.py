from datetime import datetime, timezone
from typing import Mapping, Set

from .eups import EupsTag
from .jira import JiraCache
from .typing import Changelog

def print_tag(
    tag: EupsTag, added: Set[str], dropped: Set[str], tickets: Mapping[str, Set[str]]
):
    print(f"<h2 id=\"{tag.name}\">{tag.name}</h2>")
    if tag.name != "master":
        print(f"Released {tag.date.strftime('%Y-%m-%d')}.")
    if not added and not dropped and not tickets:
        print("No changes in this version.")
    if added:
        print("<h3>Products added</h3>")
        print("<ul>")
        for product_name in sorted(added):
            print(f"<li>{product_name}</li>")
        print("</ul>")
    if dropped:
        print("<h3>Products removed</h3>")
        print("<ul>")
        for product_name in sorted(dropped):
            print(f"<li>{product_name}</li>")
        print("</ul>")
    if tickets:
        jira = JiraCache()
        print("<h3>Tickets merged</h3>")
        print("<ul>")
        for ticket_id, product_names in sorted(tickets.items(), key=lambda item: int(item[0][3:])):
            print(
                f"<li><a href=https://jira.lsstcorp.org/browse/"
                f"{ticket_id}>{ticket_id}</a>: {jira[ticket_id]} [{', '.join(sorted(product_names))}]</li>"
            )
        print("</ul>")


def print_changelog(changelog: Changelog, product_names: Set[str]):
    print("<html>")
    print("<head>")
    print("<title>Rubin Science Pipelines Changelog</title>")
    print("<style>.old-date {color: red;}</style>")
    gen_date = datetime.now(timezone.utc)
    print("<script>")
    print("const MAX_DIFF = 1.0;")  # days
    # Javascript may fail to parse date unless it exactly conforms to ISO
    print(f"const TIMESTAMP = Date.parse('{gen_date.strftime('%Y-%m-%dT%H:%M:%SZ')}');")
    print("function checkDate() {")
    print("  const MS_PER_DAY = 24 * 3600 * 1000.0;")
    print("  let load_time = Date.now();")
    print("  if (load_time - TIMESTAMP > MAX_DIFF * MS_PER_DAY) {")
    print("      document.getElementById('timestamp').className = 'old-date';")
    print("  }")
    print("}")
    print("</script>")
    print("</head>")
    print("<body onload=\"checkDate();\">")
    print("<h1>Rubin Science Pipelines Changelog</h1>")
    print(f"<p id=\"timestamp\">Generated at {gen_date.strftime('%Y-%m-%d %H:%M +00:00')}.</p>")

    for tag, values in changelog.items():
        print_tag(tag, **values)

    print(
        f"<p>Generated by considering {', '.join(sorted(product_names))}.</p>"
    )
    print("</body>")
    print("</html>")
