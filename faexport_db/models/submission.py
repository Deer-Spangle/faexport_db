from __future__ import annotations
import datetime
from typing import Optional, Dict, Any, List, Iterable

from faexport_db.db import (
    merge_dicts,
    Database,
    json_to_db,
    parse_datetime,
)
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.file import File, HashAlgo
from faexport_db.models.keyword import SubmissionKeyword


class Submission:
    def __init__(
        self,
        website_id: str,
        site_submission_id: str,
        snapshots: List["SubmissionSnapshot"],
    ):
        self.website_id = website_id
        self.site_submission_id = site_submission_id
        self.snapshots = snapshots
    
    @property
    def sorted_snapshots(self) -> List[SubmissionSnapshot]:
        return sorted(self.snapshots, key=lambda s: s.scan_datetime, reverse=True)

    @property
    def is_deleted(self) -> bool:
        return self.sorted_snapshots[0].is_deleted

    @property
    def first_scanned(self) -> datetime.datetime:
        return self.sorted_snapshots[-1].scan_datetime

    @property
    def latest_update(self) -> datetime.datetime:
        return self.sorted_snapshots[0].scan_datetime
    
    @property
    def uploader_site_user_id(self) -> Optional[str]:
        for snapshot in self.sorted_snapshots:
            if snapshot.uploader_site_user_id is not None:
                return snapshot.uploader_site_user_id
        return None
    
    @property
    def title(self) -> Optional[str]:
        for snapshot in self.sorted_snapshots:
            if snapshot.title is not None:
                return snapshot.title
        return None
    
    @property
    def description(self) -> Optional[str]:
        for snapshot in self.sorted_snapshots:
            if snapshot.description is not None:
                return snapshot.description
        return None
    
    @property
    def datetime_posted(self) -> Optional[datetime.datetime]:
        for snapshot in self.sorted_snapshots:
            if snapshot.datetime_posted is not None:
                return snapshot.datetime_posted
        return None

    @property
    def extra_data(self) -> Dict[str, Any]:
        extra_data = {}
        for snapshot in self.sorted_snapshots[::-1]:
            if snapshot.extra_data is not None:
                extra_data = merge_dicts(extra_data, snapshot.extra_data)
        return extra_data
    
    @property
    def keywords(self) -> List[SubmissionKeyword]:
        for snapshot in self.sorted_snapshots:
            if snapshot.keywords_recorded:
                return sorted(
                    snapshot.keywords,
                    key=lambda keyword: (keyword.ordinal, keyword.keyword)
                )
        return []
    
    @property
    def files(self) -> Dict[Optional[str], File]:
        files = {}
        for snapshot in self.sorted_snapshots[::-1]:
            if snapshot.files is None:
                continue
            for file in snapshot.files:
                current_file = files.get(file.site_file_id)
                if current_file is None:
                    files[file.site_file_id] = file
                    continue
                if current_file.is_clashing(file):
                    files[file.site_file_id] = file
                    continue
                current_file.add_update(file)
        return files

    def to_web_json(self) -> Dict:
        return {
            "website_id": self.website_id,
            "site_submission_id": self.site_submission_id,
            "cache_data": {
                "snapshot_count": len(self.snapshots),
                "first_scanned": self.first_scanned,
                "latest_update": self.latest_update,
            },
            "submission_data": {
                "is_deleted": self.is_deleted,
                "uploader_site_user_id": self.uploader_site_user_id,
                "title": self.title,
                "description": self.description,
                "datetime_posted": self.datetime_posted.isoformat() if self.datetime_posted is not None else None,
                "keywords": [keyword.to_web_json() for keyword in self.keywords],
                "files": [file.to_web_json() for file in self.files.values()],
                "extra_data": self.extra_data,
            }
        }

    def to_web_snapshots_json(self) -> Dict:
        return {
            "website_id": self.website_id,
            "site_submission_id": self.site_submission_id,
            "snapshot_count": len(self.snapshots),
            "snapshots": [snapshot.to_web_json() for snapshot in self.sorted_snapshots]
        }

    @classmethod
    def from_database(
        cls, db: "Database", website_id: str, site_submission_id: str
    ) -> Optional["Submission"]:
        snapshot_rows = db.select(
            "SELECT s.submission_snapshot_id, s.scan_datetime, s.archive_contributor_id, a.name as contributor_name, "
            "s.ingest_datetime, s.uploader_site_user_id, s.is_deleted, s.title, s.description, s.datetime_posted, "
            "s.extra_data "
            "FROM submission_snapshots s "
            "LEFT JOIN archive_contributors a ON s.archive_contributor_id = a.contributor_id "
            "WHERE website_id = %s AND site_submission_id = %s",
            (website_id, site_submission_id)
        )
        snapshots = []
        contributors = {}
        snapshot_rows = list(snapshot_rows)
        snapshot_ids = [row[0] for row in snapshot_rows]
        all_keywords = SubmissionKeyword.list_for_submission_snapshots_batch(db, snapshot_ids)
        all_files = File.list_for_submission_snapshots_batch(db, snapshot_ids)
        for row in snapshot_rows:
            (
                submission_snapshot_id, scan_datetime, contributor_id, contributor_name, ingest_datetime,
                uploader_site_user_id, is_deleted, title, description, datetime_posted, extra_data
            ) = row
            contributor = contributors.get(contributor_id)
            if contributor is None:
                contributor = ArchiveContributor(contributor_name, contributor_id=contributor_id)
                contributors[contributor_id] = contributor
            # Load keywords
            keywords = [keyword for keyword in all_keywords if keyword.submission_snapshot_id == submission_snapshot_id]
            # Load files
            files = [file for file in all_files if file.submission_snapshot_id == submission_snapshot_id]
            snapshots.append(SubmissionSnapshot(
                website_id,
                site_submission_id,
                contributor,
                scan_datetime,
                submission_snapshot_id=submission_snapshot_id,
                ingest_datetime=ingest_datetime,
                uploader_site_user_id=uploader_site_user_id,
                is_deleted=is_deleted,
                title=title,
                description=description,
                datetime_posted=datetime_posted,
                extra_data=extra_data,
                keywords=keywords,
                files=files,
            ))
        if not snapshots:
            return None
        return Submission(
            website_id,
            site_submission_id,
            snapshots
        )

    @classmethod
    def list_unique_site_ids(cls, db: Database, website_id: str) -> Iterable[str]:
        submission_rows = db.select_iter(
            "SELECT DISTINCT site_submission_id FROM submission_snapshots WHERE website_id = %s",
            (website_id,)
        )
        for submission_row in submission_rows:
            yield submission_row[0]


class SubmissionSnapshot:
    def __init__(
        self,
        website_id: str,
        site_submission_id: str,
        contributor: ArchiveContributor,
        scan_datetime: datetime.datetime,
        *,
        submission_snapshot_id: int = None,
        ingest_datetime: datetime.datetime = None,
        uploader_site_user_id: str = None,
        is_deleted: bool = False,
        title: str = None,
        description: str = None,
        datetime_posted: datetime.datetime = None,
        extra_data: Dict[str, Any] = None,
        keywords: List[SubmissionKeyword] = None,
        ordered_keywords: List[str] = None,
        unordered_keywords: List[str] = None,
        files: List[File] = None,
    ):
        self.website_id = website_id
        self.site_submission_id = site_submission_id
        self.contributor = contributor
        self.scan_datetime = scan_datetime
        self.submission_snapshot_id = submission_snapshot_id
        self.ingest_datetime = ingest_datetime or datetime.datetime.now(datetime.timezone.utc)
        self.uploader_site_user_id = uploader_site_user_id
        self.is_deleted = is_deleted
        self.title = title
        self.description = description
        self.datetime_posted = datetime_posted
        self.extra_data = extra_data
        self.keywords: Optional[List[SubmissionKeyword]] = keywords
        if keywords is not None:
            # Check keyword ordinals are unique
            ordinals = set()
            for keyword in keywords:
                if keyword.ordinal is None:
                    continue
                if keyword.ordinal in ordinals:
                    raise ValueError("Duplicate ordinal in keywords list")
                ordinals.add(keyword.ordinal)
        if ordered_keywords is not None:
            self.keywords = SubmissionKeyword.list_from_ordered_keywords(ordered_keywords)
        if unordered_keywords is not None:
            self.keywords = SubmissionKeyword.list_from_unordered_keywords(unordered_keywords)
        self.files: Optional[List[File]] = files
    
    @property
    def keywords_recorded(self) -> bool:
        return self.keywords is not None
    
    def to_web_json(self) -> Dict:
        keywords = None
        if self.keywords_recorded:
            keywords = [keyword.to_web_json() for keyword in self.keywords]
        return {
            "submission_snapshot_id": self.submission_snapshot_id,
            "website_id": self.website_id,
            "site_submission_id": self.site_submission_id,
            "cache_data": {
                "scan_datetime": self.scan_datetime,
                "archive_contributor": self.contributor.to_web_json(),
                "ingest_datetime": self.ingest_datetime,
            },
            "submission_data": {
                "uploader_site_user_id": self.uploader_site_user_id,
                "is_deleted": self.is_deleted,
                "title": self.title,
                "description": self.description,
                "datetime_posted": self.datetime_posted,
                "keywords": keywords,
                "files": [file.to_web_json() for file in self.files],
                "extra_data": self.extra_data,
            },
        }
    
    @classmethod
    def from_web_json(cls, web_data: Dict, contributor: ArchiveContributor) -> "SubmissionSnapshot":
        keywords = None
        if "keywords" in web_data:
            keywords = [SubmissionKeyword.from_web_json(keyword_data) for keyword_data in web_data["keywords"]]
        if "ordered_keywords" in web_data:
            keywords = SubmissionKeyword.list_from_ordered_keywords(web_data["ordered_keywords"])
        if "unordered_keywords" in web_data:
            keywords = SubmissionKeyword.list_from_unordered_keywords(web_data["unordered_keywords"])
        files = None
        if "files" in web_data:
            files = [File.from_web_json(file_data) for file_data in web_data["files"]]
        return SubmissionSnapshot(
            web_data["website_id"],
            web_data["site_submission_id"],
            contributor,
            parse_datetime(web_data.get("scan_datetime")),
            uploader_site_user_id=web_data.get("uploader_site_user_id"),
            is_deleted=web_data.get("is_deleted", False),
            title=web_data.get("title"),
            description=web_data.get("description"),
            datetime_posted=parse_datetime(web_data.get("datetime_posted")),
            extra_data=web_data.get("extra_data"),
            keywords=keywords,
            files=files,
        )

    def create_snapshot(self, db: "Database") -> None:
        snapshot_rows = db.insert(
            "INSERT INTO submission_snapshots "
            "(website_id, site_submission_id, scan_datetime, archive_contributor_id, ingest_datetime, "
            "uploader_site_user_id, is_deleted, title, description, datetime_posted, keywords_recorded, extra_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "RETURNING submission_snapshot_id ",
            (
                self.website_id, self.site_submission_id, self.scan_datetime, self.contributor.contributor_id,
                self.ingest_datetime, self.uploader_site_user_id, self.is_deleted, self.title, self.description,
                self.datetime_posted, self.keywords_recorded, json_to_db(self.extra_data),
            )
        )
        self.submission_snapshot_id = snapshot_rows[0][0]
        # Save keywords
        if self.keywords is not None:
            SubmissionKeyword.save_batch(db, self.keywords, self.submission_snapshot_id)
        # Save files
        if self.files is not None:
            File.save_batch(db, self.files, self.submission_snapshot_id)

    def save(self, db: "Database") -> None:
        if self.submission_snapshot_id is None:
            self.create_snapshot(db)

    @classmethod
    def save_batch(cls, db: Database, snapshots: List["SubmissionSnapshot"]) -> None:
        unsaved = [snapshot for snapshot in snapshots if snapshot.submission_snapshot_id is None]
        snapshot_ids = db.bulk_insert(
            "submission_snapshots",
            (
                "website_id", "site_submission_id", "scan_datetime", "archive_contributor_id", "ingest_datetime",
                "uploader_site_user_id", "is_deleted", "title", "description", "datetime_posted", "keywords_recorded",
                "extra_data"
            ),
            [
                (
                    snapshot.website_id, snapshot.site_submission_id, snapshot.scan_datetime,
                    snapshot.contributor.contributor_id, snapshot.ingest_datetime, snapshot.uploader_site_user_id,
                    snapshot.is_deleted, snapshot.title, snapshot.description, snapshot.datetime_posted,
                    snapshot.keywords_recorded, json_to_db(snapshot.extra_data))
                for snapshot in unsaved
            ],
            "submission_snapshot_id"
        )
        for snapshot, snapshot_id in zip(unsaved, snapshot_ids):
            snapshot.submission_snapshot_id = snapshot_id
            if snapshot.keywords is not None:
                for keyword in snapshot.keywords:
                    keyword.submission_snapshot_id = snapshot_id
            if snapshot.files is not None:
                for file in snapshot.files:
                    file.submission_snapshot_id = snapshot_id
        # Save keywords
        keywords = sum([snapshot.keywords for snapshot in snapshots if snapshot.keywords is not None], start=[])
        SubmissionKeyword.save_batch(db, keywords, None)
        # Save files
        files = sum([snapshot.files for snapshot in snapshots if snapshot.files is not None], start=[])
        File.save_batch(db, files, None)

    @classmethod
    def list_all(cls, db: Database, website_id: str) -> Iterable["SubmissionSnapshot"]:
        contributors = {contributor.contributor_id: contributor for contributor in ArchiveContributor.list_all(db)}
        snapshot_rows = db.select_iter(
            "SELECT submission_snapshot_id, website_id, site_submission_id, scan_datetime, archive_contributor_id, "
            "ingest_datetime, uploader_site_user_id, is_deleted, title, description, datetime_posted, "
            "keywords_recorded, extra_data "
            "FROM submission_snapshots WHERE website_id = %s",
            (website_id,)
        )
        for snapshot_row in snapshot_rows:
            snapshot_id, website_id, site_submission_id, scan_datetime, archive_contributor_id, ingest_datetime, uploader_site_user_id, is_deleted, title, description, datetime_posted, keywords_recorded, extra_data = snapshot_row
            contributor = contributors[archive_contributor_id]
            keywords = SubmissionKeyword.list_for_submission_snapshot(db, snapshot_id)
            files = File.list_for_submission_snapshot(db, snapshot_id)
            yield cls(
                website_id,
                site_submission_id,
                contributor,
                scan_datetime,
                submission_snapshot_id=site_submission_id,
                ingest_datetime=ingest_datetime,
                uploader_site_user_id=uploader_site_user_id,
                is_deleted=is_deleted,
                title=title,
                description=description,
                datetime_posted=datetime_posted,
                extra_data=extra_data,
                keywords=keywords,
                files=files,
            )

    @classmethod
    def search_by_file_hash(cls, db: Database, hash_algo: HashAlgo, hash_value: bytes) -> List[SubmissionSnapshot]:
        snapshot_rows = db.select(
            "SELECT s.submission_snapshot_id, s.website_id, s.site_submission_id, s.scan_datetime, "
            "s.archive_contributor_id, a.name as contributor_name, s.ingest_datetime, s.uploader_site_user_id, "
            "s.is_deleted, s.title, s.description, s.datetime_posted, s.extra_data "
            "FROM submission_snapshot_file_hashes hashes "
            "LEFT JOIN submission_snapshot_files files on files.file_id = hashes.file_id "
            "LEFT JOIN submission_snapshots s ON files.submission_snapshot_id = s.submission_snapshot_id "
            "LEFT JOIN archive_contributors a ON s.archive_contributor_id = a.contributor_id "
            "WHERE algo_id = %s AND hash_value = %s",
            (hash_algo.algo_id, hash_value)
        )
        contributors = {}
        snapshots = []
        snapshot_rows = list(snapshot_rows)
        snapshot_ids = [row[0] for row in snapshot_rows]
        all_keywords = SubmissionKeyword.list_for_submission_snapshots_batch(db, snapshot_ids)
        all_files = File.list_for_submission_snapshots_batch(db, snapshot_ids)
        for snapshot_row in snapshot_rows:
            (
                submission_snapshot_id, website_id, site_submission_id, scan_datetime, contributor_id, contributor_name,
                ingest_datetime, uploader_site_user_id, is_deleted, title, description, datetime_posted, extra_data
            ) = snapshot_row
            contributor = contributors.get(contributor_id)
            if contributor is None:
                contributor = ArchiveContributor(contributor_name, contributor_id=contributor_id)
                contributors[contributor_id] = contributor
            # Load keywords
            keywords = [keyword for keyword in all_keywords if keyword.submission_snapshot_id == submission_snapshot_id]
            # Load files
            files = [file for file in all_files if file.submission_snapshot_id == submission_snapshot_id]
            snapshots.append(SubmissionSnapshot(
                website_id,
                site_submission_id,
                contributor,
                scan_datetime,
                submission_snapshot_id=submission_snapshot_id,
                ingest_datetime=ingest_datetime,
                uploader_site_user_id=uploader_site_user_id,
                is_deleted=is_deleted,
                title=title,
                description=description,
                datetime_posted=datetime_posted,
                extra_data=extra_data,
                keywords=keywords,
                files=files,
            ))
        return snapshots
