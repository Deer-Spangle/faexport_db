from __future__ import annotations
import datetime
from typing import Optional, Dict, Any, List, TYPE_CHECKING

from psycopg2 import errors
from psycopg2.errorcodes import UNIQUE_VIOLATION

from faexport_db.db import (
    merge_dicts,
    Database,
    json_to_db,
    unset_to_null,
    UNSET,
)
from faexport_db.models.file import FileList
from faexport_db.models.keyword import SubmissionKeywordsListUpdate, SubmissionKeywordsList
from faexport_db.models.user import User

if TYPE_CHECKING:
    from faexport_db.models.file import FileListUpdate
    from faexport_db.models.user import UserUpdate


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


class SubmissionUpdate:
    def __init__(
        self,
        website_id: str,
        site_submission_id: str,
        update_time: datetime.datetime = None,
        is_deleted: bool = False,
        *,
        uploader_update: UserUpdate = UNSET,
        title: str = UNSET,
        description: str = UNSET,
        datetime_posted: datetime.datetime = UNSET,
        add_extra_data: Dict[str, Any] = UNSET,
        ordered_keywords: List[str] = UNSET,
        unordered_keywords: List[str] = UNSET,
        files: FileListUpdate = UNSET,
    ):
        self.website_id = website_id
        self.site_submission_id = site_submission_id
        self.update_time = update_time or datetime.datetime.now(datetime.timezone.utc)
        self.is_deleted = is_deleted
        self.uploader_update = uploader_update
        if self.uploader_update is not UNSET:
            if self.uploader_update.website_id is None:
                self.uploader_update.website_id = self.website_id
            if not self.uploader_update.update_time_set:
                self.uploader_update.update_time = self.update_time
        self.title = title
        self.description = description
        self.datetime_posted = datetime_posted
        self.add_extra_data = add_extra_data
        self.ordered_keywords = ordered_keywords
        self.unordered_keywords = unordered_keywords
        self.files = files
        if self.files is not UNSET:
            for file_update in self.files.file_updates:
                if not file_update.update_time_set:
                    file_update.update_time = self.update_time

    def create_submission(self, db: "Database") -> Submission:
        # Handle things which may be unset
        uploader = None
        uploader_id = None
        if self.uploader_update is not UNSET:
            uploader = self.uploader_update.save(db)
            uploader_id = uploader.user_id
        title = unset_to_null(self.title)
        description = unset_to_null(self.description)
        datetime_posted = unset_to_null(self.datetime_posted)
        extra_data = unset_to_null(self.add_extra_data)
        submission_rows = db.insert(
            "INSERT INTO submissions "
            "(website_id, site_submission_id, is_deleted, first_scanned, latest_update, uploader_id, title, "
            "description, datetime_posted, extra_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING submission_id",
            (
                self.website_id,
                self.site_submission_id,
                self.is_deleted,
                self.update_time,
                self.update_time,
                uploader_id,
                title,
                description,
                datetime_posted,
                json_to_db(extra_data),
            ),
        )
        submission_id = submission_rows[0][0]
        # Save keywords
        submission_keywords = SubmissionKeywordsList(submission_id, [])
        if self.ordered_keywords is not UNSET:
            keywords_update = SubmissionKeywordsListUpdate.from_ordered_keywords(self.ordered_keywords)
            submission_keywords = keywords_update.save(db, submission_id)
        if self.unordered_keywords is not UNSET:
            keywords_update = SubmissionKeywordsListUpdate.from_unordered_keywords(self.unordered_keywords)
            submission_keywords = keywords_update.save(db, submission_id)
        # Save files
        files = FileList(submission_id, [])
        if self.files is not UNSET:
            files = self.files.create(db, submission_id)
        return Submission(
            submission_id,
            self.website_id,
            self.site_submission_id,
            self.is_deleted,
            self.update_time,
            self.update_time,
            uploader,
            title,
            description,
            datetime_posted,
            extra_data,
            submission_keywords,
            files
        )

    def save(self, db: "Database") -> Submission:
        submission = Submission.from_database(
            db, self.website_id, self.site_submission_id
        )
        if submission is not None:
            submission.add_update(self)
            submission.save(db)
            return submission
        try:
            return self.create_submission(db)
        except errors.lookup(UNIQUE_VIOLATION):
            submission = Submission.from_database(db, self.website_id, self.site_submission_id)
            if submission is None:
                raise ValueError("Submission existed, and then disappeared")
            submission.add_update(self)
            submission.save(db)
            return submission
