from __future__ import annotations
from typing import Optional, TYPE_CHECKING, Dict, List

if TYPE_CHECKING:
    from faexport_db.db import Database


class Website:
    def __init__(
            self,
            website_id: str,
            full_name: str,
            link: str
    ) -> None:
        self.website_id = website_id
        self.full_name = full_name
        self.link = link

    def count_user_snapshots(self, db: Database) -> int:
        count_rows = db.select(
            "SELECT COUNT(*) FROM user_snapshots WHERE website_id = %s",
            (self.website_id,)
        )
        if count_rows:
            return count_rows[0][0]
        return 0

    def count_submission_snapshots(self, db: Database) -> int:
        count_rows = db.select(
            "SELECT COUNT(*) FROM submission_snapshots WHERE website_id = %s",
            (self.website_id,)
        )
        if count_rows:
            return count_rows[0][0]
        return 0

    def to_web_json(self, db: Database) -> Dict:
        return {
            "website_id": self.website_id,
            "full_name": self.full_name,
            "link": self.link,
            "num_user_snapshots": self.count_user_snapshots(db),
            "num_submission_snapshots": self.count_submission_snapshots(db),
        }

    def save(self, db: Database) -> None:
        if self.from_database(db, self.website_id):
            return
        self._create(db)

    def _create(self, db: Database) -> None:
        db.update(
            "INSERT INTO websites (website_id, full_name, link) VALUES (%s, %s, %s)",
            (self.website_id, self.full_name, self.link)
        )

    @classmethod
    def from_database(cls, db: Database, website_id: str) -> Optional["Website"]:
        website_rows = db.select(
            "SELECT full_name, link FROM websites WHERE website_id = %s",
            (website_id,)
        )
        if not website_rows:
            return None
        full_name, link = website_rows[0]
        return cls(
            website_id,
            full_name,
            link
        )
    
    @classmethod
    def list_all(cls, db: Database) -> List["Website"]:
        website_rows = db.select(
            "SELECT website_id, full_name, link FROM websites",
            tuple()
        )
        websites = []
        for website_row in website_rows:
            website_id, full_name, link = website_row
            websites.append(Website(website_id, full_name, link))
        return websites
