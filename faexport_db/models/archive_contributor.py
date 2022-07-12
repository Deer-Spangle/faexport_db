from typing import List, Dict

from faexport_db.db import Database


class ArchiveContributor:

    def __init__(self, name: str, *, contributor_id: int = None) -> None:
        self.name = name
        self.contributor_id = contributor_id

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

    def to_web_json(self, db: Database) -> Dict:
        return {
            "contributor_id": self.contributor_id,
            "name": self.name,
            "num_user_snapshots": self.count_user_snapshots(db),
            "num_submission_snapshots": self.count_submission_snapshots(db),
        }

    def save(self, db: Database) -> None:
        if self.contributor_id is None:
            contributor_rows = db.insert(
                "WITH e AS ( "
                "INSERT INTO archive_contributors (name) VALUES (%s) ON CONFLICT (name) DO NOTHING "
                "RETURNING contributor_id ) "
                "SELECT * FROM e "
                "UNION SELECT contributor_id FROM archive_contributors WHERE name = %s",
                (self.name, self.name)
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
            "SELECT contributor_id, name FROM archive_contributors",
            tuple()
        )
        contributors = []
        for contributor_row in contributor_rows:
            contributor_id, name = contributor_row
            contributors.append(cls(
                name,
                contributor_id=contributor_id
            ))
        return contributors
