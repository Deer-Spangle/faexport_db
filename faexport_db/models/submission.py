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
    def __init__(
        self,
        submission_id: int,
        website_id: str,
        site_submission_id: str,
        is_deleted: bool,
        first_scanned: datetime.datetime,
        latest_update: datetime.datetime,
        uploader: Optional[User],
        title: Optional[str],
        description: Optional[str],
        datetime_posted: Optional[datetime.datetime],
        extra_data: Optional[Dict[str, Any]],
        keywords: SubmissionKeywordsList,
        files: FileList,
    ):
        self.submission_id = submission_id
        self.website_id = website_id
        self.site_submission_id = site_submission_id
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
        self.keywords_update: SubmissionKeywordsListUpdate = UNSET
        self.files = files
        self.files_update: FileListUpdate = UNSET
        self.updated = False

    def add_update(self, update: "SubmissionUpdate") -> None:
        if update.update_time > self.latest_update:
            self.updated = True  # At the very least, update time is being updated
            self.latest_update = update.update_time
            if update.is_deleted is not UNSET:
                self.is_deleted = update.is_deleted
            if update.uploader_update is not UNSET:
                if self.uploader is not None:
                    self.uploader.add_update(update.uploader_update)
                else:
                    self.uploader_create = update.uploader_update
            if update.title is not UNSET:
                self.title = update.title
            if update.description is not UNSET:
                self.description = update.description
            if update.datetime_posted is not UNSET:
                self.datetime_posted = update.datetime_posted
            if update.add_extra_data is not UNSET:
                self.extra_data = merge_dicts(self.extra_data, update.add_extra_data)
            if update.ordered_keywords is not UNSET:
                self.keywords_update = SubmissionKeywordsListUpdate.from_ordered_keywords(update.ordered_keywords)
            if update.unordered_keywords is not UNSET:
                self.keywords_update = SubmissionKeywordsListUpdate.from_unordered_keywords(update.unordered_keywords)
            if update.files is not UNSET:
                self.files_update = update.files
            return
        # If it's an older update, we can still update some things if they're unset
        if self.title is None and update.title is not UNSET:
            self.updated = self.updated or (self.title != update.title)
            self.title = update.title
        if self.description is None and update.description is not UNSET:
            self.updated = self.updated or (self.description != update.description)
            self.description = update.description
        if self.datetime_posted is None and update.datetime_posted is not UNSET:
            self.updated = self.updated or (
                self.datetime_posted != update.datetime_posted
            )
            self.datetime_posted = update.datetime_posted
        if update.add_extra_data is not UNSET:
            new_extra_data = merge_dicts(update.add_extra_data, self.extra_data)
            self.updated = self.updated or (self.extra_data != new_extra_data)
            self.extra_data = new_extra_data
        # Update uploader
        if update.uploader_update is not UNSET:
            if self.uploader is None:
                self.uploader_create = update.uploader_update
            else:
                self.uploader.add_update(update.uploader_update)
        # Update keywords
        if self.keywords is None and update.ordered_keywords is not UNSET:
            self.keywords_update = SubmissionKeywordsListUpdate.from_ordered_keywords(update.ordered_keywords)
        if self.keywords is None and update.unordered_keywords is not UNSET:
            self.keywords_update = SubmissionKeywordsListUpdate.from_unordered_keywords(update.unordered_keywords)
        # Update files
        if update.files is not UNSET:
            self.files.add_update(update.files)
        # Update first scanned, if this update is older
        if self.first_scanned > update.update_time:
            self.updated = True
            self.first_scanned = update.update_time

    def save(self, db: "Database") -> None:
        if self.updated:
            db.update(
                "UPDATE submissions "
                "SET is_deleted = %s, first_scanned = %s, latest_update = %s, uploader_id = %s, title = %s, "
                "description = %s, datetime_posted = %s, extra_data = %s "
                "WHERE submission_id = %s",
                (
                    self.is_deleted,
                    self.first_scanned,
                    self.latest_update,
                    self.uploader.user_id if self.uploader else None,
                    self.title,
                    self.description,
                    self.datetime_posted,
                    json_to_db(self.extra_data),
                    self.submission_id,
                ),
            )
        # Update uploader
        if self.uploader is not None and self.uploader.updated:
            self.uploader.save(db)
        if self.uploader_create is not UNSET:
            self.uploader = self.uploader_create.create_user(db)
        # Update keywords
        if self.keywords_update is not UNSET:
            self.keywords = self.keywords_update.save(db, self.submission_id)
        # Update files
        if self.files.updated:
            self.files.save(db)
        if self.files_update is not UNSET:
            self.files = self.files_update.create(db, self.submission_id)

    @classmethod
    def from_database(
        cls, db: "Database", website_id: str, site_submission_id: str
    ) -> Optional["Submission"]:
        sub_rows = db.select(
            "SELECT submission_id, is_deleted, first_scanned, latest_update, uploader_id, title, description, "
            "datetime_posted, extra_data "
            "FROM submissions "
            "WHERE website_id = %s AND site_submission_id = %s",
            (website_id, site_submission_id),
        )
        if not sub_rows:
            return None
        (
            sub_id,
            is_deleted,
            first_scanned,
            latest_update,
            uploader_id,
            title,
            description,
            datetime_posted,
            extra_data,
        ) = sub_rows[0]
        uploader = None
        if uploader_id is not None:
            uploader = User.from_database_by_user_id(db, uploader_id)
        keywords = SubmissionKeywordsList.from_database(db, sub_id)
        files = FileList.from_database(db, sub_id)
        return cls(
            sub_id,
            website_id,
            site_submission_id,
            is_deleted,
            first_scanned,
            latest_update,
            uploader,
            title,
            description,
            datetime_posted,
            extra_data,
            keywords,
            files
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
