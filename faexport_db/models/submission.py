from __future__ import annotations
import datetime
from typing import Optional, Dict, Any, List, TYPE_CHECKING

from faexport_db.db import (
    merge_dicts,
    Database,
    json_to_db,
)
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.file import File
from faexport_db.models.keyword import SubmissionKeyword
from faexport_db.models.user import User



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
    def uploader_site_user_id(self) -> str:
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
            "cache_info": {
                "snapshot_count": len(self.snapshots),
                "is_deleted": self.is_deleted,
                "first_scanned": self.first_scanned,
                "latest_update": self.latest_update,
            },
            "submission_info": {
                "uploader_site_user_id": self.uploader_site_user_id,
                "title": self.title,
                "description": self.description,
                "datetime_posted": self.datetime_posted.isoformat() if self.datetime is not None else None,
                "extra_data": self.extra_data,
                "keywords": [keyword.to_web_json() for keyword in self.keywords],
                "files": [file.to_web_json() for file in self.files.values()],
            }
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
            "WHERE website_id = %s AND site_submission_id = %s"
        )
        snapshots = []
        contributors = {}
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
            keywords = SubmissionKeyword.list_for_submission_snapshot(db, submission_snapshot_id)
            # Load files
            files = File.list_for_submission_snapshot(db, submission_snapshot_id)
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
        self.ingest_datetime = ingest_datetime
        self.uploader_site_user_id = uploader_site_user_id
        self.is_deleted = is_deleted
        self.title = title
        self.description = description
        self.datetime_posted = datetime_posted
        self.extra_data = extra_data
        self.keywords: Optional[List[SubmissionKeyword]] = keywords
        if ordered_keywords is not None:
            self.keywords = [
                SubmissionKeyword(keyword, ordinal=ordinal) for ordinal, keyword in enumerate(ordered_keywords)
            ]
        if unordered_keywords is not None:
            self.keywords = [
                SubmissionKeyword(keyword) for keyword in unordered_keywords
            ]
        self.files: Optional[List[File]] = files
    
    @property
    def keywords_recorded(self) -> bool:
        return self.keywords is not None

    def create_snapshot(self, db: "Database") -> None:
        snapshot_rows = db.insert(
            "WITH e AS ("
            "INSERT INTO submission_snapshots "
            "(website_id, site_submission_id, scan_datetime, archive_contributor_id, ingest_datetime, "
            "uploader_site_user_id, is_deleted, title, description, datetime_posted, keywords_recorded, extra_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (website_id, site_submission_id, scan_datetime, archive_contributor_id) DO NOTHING "
            "RETURNING submission_snapshot_id "
            ") SELECT * FROM e "
            "UNION SELECT submission_snapshot_id FROM submission_snapshots "
            "WHERE website_id = %s AND site_submission_id = %s AND scan_datetime = %s AND archive_contributor_id = %s",
            (
                self.website_id, self.site_submission_id, self.scan_datetime, self.contributor.contributor_id,
                self.ingest_datetime, self.uploader_site_user_id, self.is_deleted, self.title, self.description,
                self.datetime_posted, self.keywords_recorded, json_to_db(self.extra_data),
                self.website_id, self.site_submission_id, self.scan_datetime, self.contributor.contributor_id
            )
        )
        self.submission_snapshot_id = snapshot_rows[0][0]
        # Save keywords
        if self.keywords is not None:
            for keyword in self.keywords:
                keyword.save(db, self.submission_snapshot_id)
        # Save files
        if self.files is not None:
            for file in self.files:
                file.save(db, self.submission_snapshot_id)

    def save(self, db: "Database") -> None:
        if self.submission_snapshot_id is None:
            self.create_snapshot(db)
