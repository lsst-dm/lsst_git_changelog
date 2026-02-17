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

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List
from jira import JIRA
import sqlite3

@dataclass(frozen=True)
class JiraTicket:
    key: str
    project: str
    summary: str
    updated: datetime

class JiraData:
    JIRA_URL = "https://rubinobs.atlassian.net"

    def __init__(self, db_path: str = "jira_cache.sqlite"):
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self._init_db()
        self.jira = JIRA(server=self.JIRA_URL)

    def _init_db(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                key TEXT PRIMARY KEY,
                project TEXT NOT NULL,
                summary TEXT NOT NULL,
                updated TEXT NOT NULL
            )
            """
        )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_project ON tickets(project)"
        )
        self.db.commit()

    # ---------- public API ----------

    def get_tickets(self) -> Dict[str, JiraTicket]:
        tickets = []
        for project in ("DM", "SP"):
            tickets.extend(self.get_project_tickets(project))
        return {t.key: t for t in tickets}

    def get_project_tickets(self, project: str) -> List[JiraTicket]:
        last_updated = self._last_updated(project)
        self._sync_project(project, last_updated)
        return self._load_cached(project)

    # ---------- sync logic ----------

    def _sync_project(self, project: str, since: datetime | None) -> None:
        jql = f"project = {project}"
        if since:
            jql += f" AND updated > '{since.isoformat()}'"

        issues = self.jira.search_issues(
            jql,
            maxResults=0,
            fields="summary,updated",
        )

        rows = []
        for issue in issues:
            rows.append(
                (
                    issue.key,
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
                ON CONFLICT(key) DO UPDATE SET
                    summary = excluded.summary,
                    updated = excluded.updated
                """,
                rows,
            )
            self.db.commit()

    # ---------- cache helpers ----------

    def _last_updated(self, project: str) -> datetime | None:
        row = self.db.execute(
            "SELECT MAX(updated) AS updated FROM tickets WHERE project = ?",
            (project,),
        ).fetchone()

        if row["updated"]:
            return datetime.fromisoformat(row["updated"])
        return None

    def _load_cached(self, project: str) -> List[JiraTicket]:
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

