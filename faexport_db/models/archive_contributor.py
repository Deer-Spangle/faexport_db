from faexport_db.db import Database


class ArchiveContributor:

    def __init__(self, name: str, *, contributor_id: int = None) -> None:
        self.name = name
        self.contributor_id = contributor_id

    def save(self, db: Database) -> None:
        if self.contributor_id is None:
            self.contributor_id = db.insert(
                "INSERT INTO archive_contributor (name) VALUES (%s) RETURNING contributor_id",
                (self.name,)
            )
            return
