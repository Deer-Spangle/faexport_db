from typing import Optional, List

from faexport_db.db import Database


class SubmissionKeyword:
    def __init__(
            self,
            keyword: str,
            *,
            submission_snapshot_id: int = None,
            keyword_id: int = None,
            ordinal: Optional[int] = None
    ):
        self.keyword = keyword
        self.submission_snapshot_id = submission_snapshot_id
        self.keyword_id = keyword_id
        self.ordinal = ordinal
    
    def to_web_json(self) -> Dict:
        return {
            "keyword": self.keyword,
            "ordinal": self.ordinal
        }

    def create_snapshot(self, db: "Database") -> None:
        keyword_rows = db.insert(
            "INSERT INTO submission_snapshot_keywords "
            "(submission_snapshot_id, keyword, ordinal) "
            "VALUES (%s, %s, %s) "
            "RETURNING keyword_id",
            (self.submission_snapshot_id, self.keyword, self.ordinal)
        )
        self.keyword_id = keyword_rows[0][0]

    def save(self, db: "Database", submission_snapshot_id: int) -> None:
        self.submission_snapshot_id = submission_snapshot_id
        if self.keyword_id is None:
            self.create_snapshot(db)

    @classmethod
    def list_for_submission_snapshot(cls, db: Database, submission_snapshot_id: int) -> List["SubmissionKeyword"]:
        keyword_rows = db.select(
            "SELECT keyword_id, keyword, ordinal "
            "FROM submission_snapshot_keywords "
            "WHERE submission_snapshot_id = %s",
            (submission_snapshot_id,)
        )
        keywords = []
        for keyword_row in keyword_rows:
            keyword_id, keyword, ordinal = keyword_row
            keywords.append(SubmissionKeyword(
                keyword,
                submission_snapshot_id=submission_snapshot_id,
                keyword_id=keyword_id,
                ordinal=ordinal
            ))
        return keywords
