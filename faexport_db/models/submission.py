from __future__ import annotations
import datetime
from typing import Optional, Dict, Any, List, TYPE_CHECKING

from faexport_db.db import (
    merge_dicts,
    Database,
    json_to_db,
    UNSET,
)
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.file import FileList, File
from faexport_db.models.keyword import SubmissionKeywordsListUpdate, SubmissionKeywordsList, SubmissionKeyword
from faexport_db.models.user import User

if TYPE_CHECKING:
    from faexport_db.models.file import FileListUpdate


class Submission:
    # TODO: re-implement to work from snapshots
    def __init__(
        self,
        website_id: str,
        site_submission_id: str,
        snapshots: List["SubmissionSnapshot"],
    ):
        self.website_id = website_id
        self.site_submission_id = site_submission_id
        self.snapshots = snapshots
        # TODO: Convert to properties
        self.is_deleted = is_deleted
        self.first_scanned = first_scanned
        self.latest_update = latest_update
        self.uploader = uploader
        self.uploader_create: UserUpdate = UNSET
        self.title = title
        self.description = description
        self.datetime_posted = datetime_posted
        self.extra_data = extra_data
        self.keywords = keywords
        self.files = files

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
            # TODO: Load files
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
                keywords=keywords
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
        self.keywords = keywords
        if ordered_keywords is not None:
            self.keywords = [
                SubmissionKeyword(keyword, ordinal=ordinal) for ordinal, keyword in enumerate(ordered_keywords)
            ]
        if unordered_keywords is not None:
            self.keywords = [
                SubmissionKeyword(keyword) for keyword in unordered_keywords
            ]
        self.files = files

    def create_snapshot(self, db: "Database") -> None:
        snapshot_rows = db.insert(
            "WITH e AS ("
            "INSERT INTO submission_snapshots "
            "(website_id, site_submission_id, scan_datetime, archive_contributor_id, ingest_datetime, "
            "uploader_site_user_id, is_deleted, title, description, datetime_posted, extra_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (website_id, site_submission_id, scan_datetime, archive_contributor_id) DO NOTHING "
            "RETURNING submission_snapshot_id "
            ") SELECT * FROM e "
            "UNION SELECT submission_snapshot_id FROM submission_snapshots "
            "WHERE website_id = %s AND site_submission_id = %s AND scan_datetime = %s AND archive_contributor_id = %s",
            (
                self.website_id, self.site_submission_id, self.scan_datetime, self.contributor.contributor_id,
                self.ingest_datetime, self.uploader_site_user_id, self.is_deleted, self.title, self.description,
                self.datetime_posted, json_to_db(self.extra_data),
                self.website_id, self.site_submission_id, self.scan_datetime, self.contributor.contributor_id
            )
        )
        self.submission_snapshot_id = snapshot_rows[0][0]
        # Save keywords
        for keyword in self.keywords:
            keyword.save(db, self.submission_snapshot_id)
        # Save files
        for file in self.files:
            file.save(db, self.submission_snapshot_id)

    def save(self, db: "Database") -> None:
        if self.submission_snapshot_id is None:
            self.create_snapshot(db)
