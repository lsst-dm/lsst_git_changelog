#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime

from jira import JIRA


@dataclass(frozen=True)
class JiraTicket:
    """A cached Jira issue.

    Attributes
    ----------
    key : int
        Numeric part of the issue key (e.g. ``12345`` for ``DM-12345``).
    project : str
        Project prefix (e.g. ``"DM"`` or ``"SP"``).
    summary : str
        Issue summary (title) text.
    updated : datetime
        Timestamp of the last update as recorded in Jira.
    """

    key: int
    project: str
    summary: str
    updated: datetime


class JiraData:
    """Fetch and locally cache Jira tickets using an SQLite database.

    Parameters
    ----------
    db_path : str, optional
        Path to the SQLite cache file. Created if it does not exist.
        Default is ``"jira_cache.sqlite"``.
    """

    JIRA_URL = "https://rubinobs.atlassian.net"

    def __init__(self, db_path: str = "jira_cache.sqlite"):
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.jira = JIRA(server=self.JIRA_URL)

    def _init_db(self) -> None:
        """Create the ``tickets`` table and index if they do not exist."""
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                                                   key INTEGER NOT NULL,
                                                   project TEXT NOT NULL,
                                                   summary TEXT NOT NULL,
                                                   updated TEXT NOT NULL,
                                                   PRIMARY KEY(key, project)
                )
            """
        )
        self.db.execute("CREATE INDEX IF NOT EXISTS idx_project ON tickets(project)")
        self.db.commit()

    # ---------- public API ----------

    def get_tickets(self) -> dict[str, JiraTicket]:
        """Return all tickets for the DM, SP and RSO projects.

        Syncs any new issues from Jira before returning cached data.

        Returns
        -------
        dict[str, JiraTicket]
            Mapping of ticket identifier (e.g. ``"DM-12345"``) to
            ``JiraTicket``.
        """
        tickets: list[JiraTicket] = self.get_projects_tickets(["DM", "SP", "RSO"])
        # keys as "SP-1", "DM-1" to avoid collisions
        return {f"{t.project}-{t.key}": t for t in tickets}

    def get_projects_tickets(self, projects: list[str]) -> list[JiraTicket]:
        """Sync and return tickets for the specified Jira projects.

        Parameters
        ----------
        projects : list[str]
            Project keys to fetch, e.g. ``["DM", "SP"]``.

        Returns
        -------
        list[JiraTicket]
            All cached tickets across the requested projects, including any
            newly synced from Jira.
        """
        # compute last_updated for each project
        last_updated_map = {p: self._last_updated(p) for p in projects}
        for project in projects:
            self._sync_project(project, last_updated_map[project])
        # load all cached tickets
        tickets: list[JiraTicket] = []
        for project in projects:
            tickets.extend(self._load_cached(project))
        return tickets

    def _sync_project(self, project: str, since: int | None) -> None:
        """Fetch new issues from Jira and upsert them into the local cache.

        Parameters
        ----------
        project : str
            Jira project key (e.g. ``"DM"``).
        since : int or None
            Highest cached issue number for this project. If provided, only
            issues with a higher key number are fetched. ``None`` fetches all.
        """
        jql = f"project = {project}"
        if since:
            jql += f" AND key > '{project}-{since}'"
        # Use enhanced_search_issues instead of search_issues
        issues = self.jira.enhanced_search_issues(
            jql,
            maxResults=False,
            fields="summary,updated",
        )
        rows = []
        for issue in issues:
            # extract numeric part of the key
            match = re.search(r"(\d+)$", issue.key)
            if not match:
                continue
            key_number = int(match.group(1))
            rows.append(
                (
                    key_number,
                    project,
                    issue.fields.summary,
                    issue.fields.updated,
                )
            )

        if rows:
            self.db.executemany(
                """
                INSERT INTO tickets (key, project, summary, updated)
                VALUES (?, ?, ?, ?)
                    ON CONFLICT(key, project) DO UPDATE SET
                    summary = excluded.summary,
                                                     updated = excluded.updated
                """,
                rows,
            )
            self.db.commit()

    # ---------- cache helpers ----------

    def _last_updated(self, project: str) -> int | None:
        """Return the highest cached issue number for a project.

        Parameters
        ----------
        project : str
            Jira project key.

        Returns
        -------
        int or None
            Maximum cached key number, or ``None`` if no tickets are cached.
        """
        row = self.db.execute(
            "SELECT MAX(key) AS updated FROM tickets WHERE project = ?",
            (project,),
        ).fetchone()

        if row and row["updated"]:
            return row["updated"]
        return None

    def _load_cached(self, project: str) -> list[JiraTicket]:
        """Load all cached tickets for a project from SQLite.

        Parameters
        ----------
        project : str
            Jira project key.

        Returns
        -------
        list[JiraTicket]
            All tickets stored in the local cache for this project.
        """
        rows = self.db.execute(
            "SELECT key, project, summary, updated FROM tickets WHERE project = ?",
            (project,),
        ).fetchall()

        return [
            JiraTicket(
                key=row["key"],
                project=row["project"],
                summary=row["summary"],
                updated=datetime.fromisoformat(row["updated"]),
            )
            for row in rows
        ]
