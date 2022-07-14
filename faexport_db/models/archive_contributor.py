from typing import List, Dict, Optional

from faexport_db.db import Database


class ArchiveContributor:

    def __init__(self, name: str, *, contributor_id: int = None, api_key: str = None) -> None:
        self.name = name
        self.contributor_id = contributor_id
        self.api_key = api_key

    def count_user_snapshots(self, db: Database) -> int:
        count_rows = db.select(
            "SELECT COUNT(*) FROM user_snapshots WHERE archive_contributor_id = %s",
            (self.contributor_id,)
        )
        if count_rows:
            return count_rows[0][0]
        return 0

    def count_submission_snapshots(self, db: Database) -> int:
        count_rows = db.select(
            "SELECT COUNT(*) FROM submission_snapshots WHERE archive_contributor_id = %s",
            (self.contributor_id,)
        )
        if count_rows:
            return count_rows[0][0]
        return 0

    def to_web_json(self, db: Optional[Database] = None) -> Dict:
        data = {
            "contributor_id": self.contributor_id,
            "name": self.name,
        }
        if db:
            data["num_user_snapshots"] = self.count_user_snapshots(db)
            data["num_submission_snapshots"] = self.count_submission_snapshots(db)
        return data

    def save(self, db: Database) -> None:
        if self.contributor_id is None:
            contributor_rows = db.insert(
                "WITH e AS ( "
                "INSERT INTO archive_contributors (name, api_key) VALUES (%s, %s) "
                "ON CONFLICT (name) DO NOTHING "
                "RETURNING contributor_id ) "
                "SELECT * FROM e "
                "UNION SELECT contributor_id FROM archive_contributors WHERE name = %s",
                (self.name, self.api_key, self.name)
            )
            if not contributor_rows:
                contributor_rows = db.select(
                    "SELECT contributor_id FROM archive_contributors WHERE name = %s",
                    (self.name,)
                )
            self.contributor_id = contributor_rows[0][0]

    @classmethod
    def list_all(cls, db: Database) -> List["ArchiveContributor"]:
        contributor_rows = db.select(
            "SELECT contributor_id, name, api_key FROM archive_contributors",
            tuple()
        )
        contributors = []
        for contributor_row in contributor_rows:
            contributor_id, name, api_key = contributor_row
            contributors.append(cls(
                name,
                contributor_id=contributor_id,
                api_key=api_key
            ))
        return contributors

    @classmethod
    def from_database_by_api_key(cls, db: Database, api_key: str) -> Optional["ArchiveContributor"]:
        contributor_rows = db.select(
            "SELECT contributor_id, name FROM archive_contributors WHERE api_key = %s",
            (api_key,)
        )
        if not contributor_rows:
            return None
        contributor_id, name = contributor_rows[0]
        return cls(
            name,
            contributor_id=contributor_id,
            api_key=api_key
        )
