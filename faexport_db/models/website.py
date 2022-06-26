from __future__ import annotations
from typing import Optional, TYPE_CHECKING

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
