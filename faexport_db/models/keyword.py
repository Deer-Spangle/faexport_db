from typing import Optional, Dict, List, Set, Iterable

from faexport_db.db import Database, UNSET, unset_to_null


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


# TODO: remove everything below here
class SubmissionKeywordsList:

    def __init__(self, submission_id: int, keywords: List[SubmissionKeyword]) -> None:
        self.submission_id = submission_id
        self.keywords = keywords

    def to_iterable(self) -> Optional[Iterable[str]]:
        if not self.keywords:
            return None
        if self.keywords[0].ordinal is None:
            return {keyword.keyword for keyword in self.keywords}
        return [keyword.keyword for keyword in sorted(self.keywords, key=lambda k: k.ordinal)]

    @property
    def by_ordinal(self) -> Dict[int, SubmissionKeyword]:
        return {keyword.ordinal: keyword for keyword in self.keywords if keyword.ordinal is not None}

    @property
    def unordered_keywords(self) -> Set[SubmissionKeyword]:
        return {keyword for keyword in self.keywords if keyword.ordinal is None}

    @property
    def by_keyword(self) -> Dict[str, SubmissionKeyword]:
        return {keyword.keyword: keyword for keyword in self.keywords}

    @classmethod
    def from_database(cls, db: "Database", submission_id: int) -> "SubmissionKeywordsList":
        submission_keywords = db.select(
            "SELECT keyword_id, keyword, ordinal FROM submission_keywords WHERE submission_id = %s",
            (submission_id,)
        )
        return cls(
            submission_id,
            [
                SubmissionKeyword(keyword_id, submission_id, keyword, ordinal)
                for keyword_id, keyword, ordinal in submission_keywords
            ]
        )

    def delete(self, db: Database) -> None:
        db.update("DELETE FROM submission_keywords WHERE submission_id = %s", (self.submission_id,))


class SubmissionKeywordUpdate:
    def __init__(
            self,
            keyword: str,
            ordinal: Optional[int] = UNSET
    ):
        self.keyword = keyword
        self.ordinal = ordinal

    def create_keyword(self, db: "Database", submission_id: int) -> SubmissionKeyword:
        keyword_rows = db.insert(
            "INSERT INTO submission_keywords "
            "(submission_id, keyword, ordinal) "
            "VALUES (%s, %s, %s) RETURNING keyword_id",
            (
                submission_id, self.keyword, unset_to_null(self.ordinal)
            )
        )
        keyword_id = keyword_rows[0][0]
        return SubmissionKeyword(
            keyword_id,
            submission_id,
            self.keyword,
            unset_to_null(self.ordinal)
        )


class SubmissionKeywordsListUpdate:
    def __init__(self, keyword_updates: List[SubmissionKeywordUpdate]) -> None:
        self.keyword_updates = keyword_updates

    def to_iterable(self) -> Optional[Iterable[str]]:
        if not self.keyword_updates:
            return None
        if self.keyword_updates[0].ordinal is None:
            return {keyword.keyword for keyword in self.keyword_updates}
        return [keyword.keyword for keyword in sorted(self.keyword_updates, key=lambda k: k.ordinal)]

    def create_keywords(self, db: Database, submission_id: int) -> SubmissionKeywordsList:
        keywords = [
            keyword_update.create_keyword(db, submission_id) for keyword_update in self.keyword_updates
        ]
        return SubmissionKeywordsList(submission_id, keywords)

    def save(self, db: "Database", submission_id: int) -> SubmissionKeywordsList:
        keyword_list = SubmissionKeywordsList.from_database(db, submission_id)
        if keyword_list is not None:
            if keyword_list.to_iterable() == self.to_iterable():
                return keyword_list
            keyword_list.delete(db)
        return self.create_keywords(db, submission_id)

    @classmethod
    def from_ordered_keywords(cls, keywords: List[str]) -> "SubmissionKeywordsListUpdate":
        return cls([
            SubmissionKeywordUpdate(keyword, ordinal) for ordinal, keyword in enumerate(keywords)
        ])

    @classmethod
    def from_unordered_keywords(cls, keywords: Iterable[str]) -> "SubmissionKeywordsListUpdate":
        return cls([
            SubmissionKeywordUpdate(keyword) for keyword in keywords
        ])
