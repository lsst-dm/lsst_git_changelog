import importlib.util
import os
from dataclasses import dataclass
from typing import Any

from changelog.eups import EupsData
from changelog.tag import ReleaseType


def load_changelog_conf(conf_path="./conf.py"):
    """Load changelog configuration from a Python config file.

    Parameters
    ----------
    conf_path : str, optional
        Path to the configuration file. Default is ``"./conf.py"``.

    Returns
    -------
    dict
        The ``changelog_conf`` dictionary defined in the config file.

    Raises
    ------
    FileNotFoundError
        If no file exists at ``conf_path``.
    AttributeError
        If the file does not define a ``changelog_conf`` attribute.
    """
    if not os.path.exists(conf_path):
        raise FileNotFoundError(f"conf.py not found at: {conf_path}")

    spec = importlib.util.spec_from_file_location("conf", conf_path)
    conf = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(conf)

    if hasattr(conf, 'changelog_conf'):
        return conf.changelog_conf
    else:
        raise AttributeError("changelog_conf not found in conf.py")

changelog_conf = load_changelog_conf()

@dataclass
class ReleaseEntry:
    """A changelog range spanning two release tags.

    Attributes
    ----------
    first_tag : str
        Tag marking the start of the range (exclusive).
    last_tag : str
        Tag marking the end of the range (inclusive).
    """

    first_tag: str
    last_tag: str


@dataclass(frozen=True)
class Releases:
    """Aggregated release data across all release types.

    Attributes
    ----------
    all_packages : set[str]
        All package names seen across every release type.
    eups_tags : dict[Tag, list[str]]
        Mapping of each ``Tag`` to the package names it contains.
    tag_list : dict[ReleaseType, dict[Tag, list[tuple[str, str]]]]
        Mapping of ``ReleaseType`` to a dict of ``Tag`` → list of
        ``(package, version)`` pairs.
    release_tags : dict
        Nested ``first_name → base_name → list[Tag]`` structure for
        regular releases, as built by ``_build_release_tags``.
    releases : dict[str, list[ReleaseEntry]]
        Changelog entries keyed by version name (e.g. ``"v26_0_0"``).
    daily_tags : list[Tag]
        Sorted list of daily release ``Tag`` objects.
    daily_releases : dict[str, list[ReleaseEntry]]
        Sequential changelog entries for daily releases.
    weekly_tags : list[Tag]
        Sorted list of weekly release ``Tag`` objects.
    weekly_releases : dict[str, list[ReleaseEntry]]
        Sequential changelog entries for weekly releases.
    """

    all_packages: Any
    eups_tags: Any
    tag_list: dict[Any, dict]
    release_tags: Any
    releases: Any
    daily_tags: list[str]
    daily_releases: Any
    weekly_tags: list[str]
    weekly_releases: Any


def _build_releases(rel_tags: dict) -> dict[str, list[ReleaseEntry]]:
    """Build release entries from regular release tags.

    Parameters
    ----------
    rel_tags : dict
        Nested ``first_name → base_name → list[Tag]`` structure as returned
        by ``_build_release_tags``.

    Returns
    -------
    dict[str, list[ReleaseEntry]]
        Mapping of version name (e.g. ``"v26_0_0"``) to a list of
        ``ReleaseEntry`` objects covering all RC tags and the boundary from
        the previous release.
    """
    releases = {}
    prev_key = None
    prev_value = None

    for key, value in rel_tags.items():
        if prev_key is not None:
            base_name = "v" + value[key][0].base_name().replace(".", "_")
            releases[base_name] = []

            last_prev_tags = list(prev_value.values())[-1]
            releases[base_name].append(ReleaseEntry(first_tag=last_prev_tags[-1], last_tag=value[key][0]))

            for _, tags in value.items():
                releases[base_name].append(ReleaseEntry(first_tag=tags[0], last_tag=tags[-1]))

        prev_key = key
        prev_value = value

    return releases


def _build_sequential_releases(tags: list, release_name: str | None = None) -> dict[str, list[ReleaseEntry]]:
    """Build release entries from sequential daily or weekly tags.

    Parameters
    ----------
    tags : list[Tag]
        Ordered list of ``Tag`` objects (daily or weekly).
    release_name : str or None, optional
        Key to use for every entry. If ``None``, the string representation
        of each tag is used as the key.

    Returns
    -------
    dict[str, list[ReleaseEntry]]
        Mapping of release name to a single-element list containing a
        ``ReleaseEntry`` spanning each pair of consecutive tags.
    """
    releases = {}
    prev_tag = None

    for tag in tags:
        if prev_tag is not None:
            key = release_name if release_name else str(tag)
            releases[key] = [ReleaseEntry(first_tag=prev_tag, last_tag=tag)]
        prev_tag = tag

    return releases


def _build_release_tags(tag_list: dict) -> dict:
    """Build a nested release-tag structure from the raw tag list.

    Parameters
    ----------
    tag_list : dict[ReleaseType, dict[Tag, list]]
        Mapping of ``ReleaseType`` to tag data as returned by
        ``_get_eups_data``.

    Returns
    -------
    dict
        Nested ``first_name → base_name → sorted list[Tag]`` mapping
        for all regular release tags.
    """
    rel_tags = {}

    for tag in sorted(tag_list[ReleaseType.REGULAR].keys()):
        first = tag.first_name()
        base = tag.base_name()

        if first not in rel_tags:
            rel_tags[first] = {}
        if base not in rel_tags[first]:
            rel_tags[first][base] = []

        rel_tags[first][base].append(tag)

    return rel_tags


class ReleaseData:
    """Fetch and organize EUPS release data.

    Parameters
    ----------
    max_workers : int, optional
        Maximum number of concurrent HTTP connections used when downloading
        EUPS ``.list`` files. Default is 8.
    """

    def __init__(self, max_workers=8):
        self.max_workers = max_workers
        self.eups = EupsData(connections=max_workers)

    def _get_eups_data(self) -> tuple[set[str], dict, dict]:
        """Load EUPS data and collect all packages and tags.

        Returns
        -------
        tuple[set[str], dict, dict]
            A 3-tuple of:

            all_packages : set[str]
                All package names found across every release type.
            eups_tags : dict[Tag, list[str]]
                Mapping of each ``Tag`` to the package names it contains.
            tag_list : dict[ReleaseType, dict[Tag, list[tuple[str, str]]]]
                Mapping of ``ReleaseType`` to tag → list of
                ``(package, version)`` pairs.
        """
        eups = self.eups
        all_packages = set()
        eups_tags = {}
        tag_list = {
            release_type: {}
            for release_type in [
                ReleaseType.WEEKLY,
                ReleaseType.REGULAR,
                ReleaseType.DAILY,
            ]
        }

        for release_type in [
            ReleaseType.WEEKLY,
            ReleaseType.REGULAR,
            ReleaseType.DAILY,
        ]:
            result = eups.get_releases(release_type)

            for tag, packages in sorted(result.data.releases.items()):
                if tag.is_regular() and tag.desc()[1][0] < 11:
                    continue

                if tag.name() in changelog_conf["discard_tags"]:
                    continue

                if tag not in eups_tags:
                    eups_tags[tag] = []

                tag_packages = []
                for package in packages:
                    version = package.version.split("+")[0][1:]
                    all_packages.add(package.package)
                    eups_tags[tag].append(package.package)
                    tag_packages.append((package.package, version))

                tag_list[release_type][tag] = tag_packages

        return all_packages, eups_tags, tag_list

    def get_releases(self) -> Releases:
        """Fetch all EUPS release data and build structured release entries.

        Returns
        -------
        Releases
            Fully populated ``Releases`` dataclass containing data for all
            release types (regular, weekly, and daily).
        """
        all_packages, eups_tags, tag_list = self._get_eups_data()

        release_tags = _build_release_tags(tag_list)
        releases = _build_releases(release_tags)

        daily_tags = sorted(tag_list[ReleaseType.DAILY].keys())
        daily_releases = _build_sequential_releases(daily_tags)

        weekly_tags = sorted(tag_list[ReleaseType.WEEKLY].keys())
        weekly_releases = _build_sequential_releases(weekly_tags)

        return Releases(
            all_packages,
            eups_tags,
            tag_list,
            release_tags,
            releases,
            daily_tags,
            daily_releases,
            weekly_tags,
            weekly_releases,
        )
