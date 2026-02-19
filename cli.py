from pprint import pprint

from changelog.models import *
from changelog.processing import generate_changelog
from changelog.tag import *


def main() -> Dict[str, Ticket]:
    """Main entry point - example usage."""
    config = ReleaseConfig(start_ref="v30.0.0.rc1", end_ref="30.0.6", eups_tag=Tag("v30_0_6"))

    # Generate changelog
    tickets = generate_changelog(config)

    # Output results
    pprint(tickets)

    return tickets


if __name__ == "__main__":
    tickets = main()
