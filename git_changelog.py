import argparse
import logging

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Set

from rubin_changelog.eups import Eups, EupsTag
from rubin_changelog.output import print_changelog
from rubin_changelog.products import products
from rubin_changelog.typing import Changelog


def get_merges_for_product(
    product_name: str, old_tag_name: str, new_tag_name: str
) -> Set[str]:
    merged = set()
    try:
        product = products[product_name]
    except KeyError:
        logging.debug(f"Skipping ticket list on {product_name} (probably skiplisted)")
    else:
        old_ref_name = f"refs/tags/{old_tag_name}"
        new_ref_name = (
            f"refs/tags/{new_tag_name}"
            if new_tag_name != "main"
            else product.branch_name
        )
        merges = product.merges_between(old_ref_name, new_ref_name)
        for sha in merges:
            ticket = product.ticket(product.message(sha))
            if ticket:
                merged.add(ticket)
    return merged


def generate_changelog(eups: Eups) -> Changelog:
    tags = sorted(eups.values(), reverse=True)
    tags.insert(
        0,
        EupsTag("main", datetime(1, 1, 1), [(p, "dummy") for p in tags[0].products]),
    )
    changelog: Changelog = {}
    for new_tag, old_tag in zip(tags, tags[1:]):
        added = set(new_tag.products) - set(old_tag.products)
        dropped = set(old_tag.products) - set(new_tag.products)
        tickets = defaultdict(set)

        with ThreadPoolExecutor() as executor:
            future_to_merges = {
                executor.submit(
                    get_merges_for_product, product_name, old_tag.name, new_tag.name
                ): product_name
                for product_name in set(new_tag.products) & set(old_tag.products)
            }
            for future in as_completed(future_to_merges):
                product_name = future_to_merges[future]
                for merge in future.result():
                    tickets[merge].add(product_name)

        changelog[new_tag] = {"added": added, "dropped": dropped, "tickets": tickets}
    return changelog

def parse_args():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--weekly', action='store_const', const=r"w_20", dest='tag_prefix')
    group.add_argument('--release', action='store_const', const=r"v\d\d", dest='tag_prefix')
    group.add_argument('--tag-prefix')
    parser.add_argument('--debug', action='store_true')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    eups = Eups(pattern=args.tag_prefix)
    print_changelog(generate_changelog(eups), eups.all_products)
