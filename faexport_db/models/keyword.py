from typing import Optional, List, Dict

from faexport_db.db import Database, chunks


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

    @classmethod
    def from_web_json(cls, web_data: Dict) -> "SubmissionKeyword":
        return cls(
            web_data["keyword"],
            ordinal=web_data.get("ordinal"),
        )

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
    def save_batch(cls, db: Database, keywords: List["SubmissionKeyword"], submission_snapshot_id: int) -> None:
        chunk_size = 100
        keywords = [k for k in keywords if k.keyword_id is None]
        for keywords_chunk in chunks(keywords, chunk_size):
            keyword_vals = [
                [submission_snapshot_id, keyword.keyword, keyword.ordinal]
                for keyword in keywords_chunk
            ]
            keyword_val_tuple = tuple(sum(keyword_vals, start=[]))
            keyword_rows = db.insert(
                "INSERT INTO submission_snapshot_keywords (submission_snapshot_id, keyword, ordinal) VALUES " +
                ", ".join("(%s, %s, %s)" for _ in keywords_chunk) + " RETURNING keyword_id",
                keyword_val_tuple
            )
            for keyword, keyword_row in zip(keywords_chunk, keyword_rows):
                keyword.keyword_id = keyword_row[0]
                keyword.submission_snapshot_id = submission_snapshot_id

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
    
    @classmethod
    def list_from_ordered_keywords(cls, ordered_keywords: List[str]) -> List["SubmissionKeyword"]:
        return [
            cls(keyword, ordinal=ordinal) for ordinal, keyword in enumerate(ordered_keywords)
        ]
    
    @classmethod
    def list_from_unordered_keywords(cls, unordered_keywords: List[str]) -> List["SubmissionKeyword"]:
        return [
            SubmissionKeyword(keyword) for keyword in unordered_keywords
        ]
