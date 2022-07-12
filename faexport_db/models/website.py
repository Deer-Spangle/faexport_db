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

    def to_web_json(self) -> Dict:
        return {
            "website_id": self.website_id,
            "full_name": self.full_name,
            "link": self.link,
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
            "SELECT website_id, full_name, link FROM websites"
        )
        websites = []
        for website_row in website_rows:
            website_id, full_name, link = website_row
            websites.append(Website(website_id, full_name, link))
        return websites
