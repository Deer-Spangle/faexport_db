from __future__ import annotations
from typing import Optional, Dict, Any, List
import datetime

from faexport_db.db import UNSET, Database, unset_to_null, merge_dicts, json_to_db


class File:
    def __init__(
            self,
            file_id: int,
            submission_id: int,
            site_file_id: Optional[str],
            is_current: bool,
            first_scanned: datetime.datetime,
            latest_update: datetime.datetime,
            file_url: Optional[str],
            file_size: Optional[int],
            extra_data: Optional[Dict[str, Any]],
            hashes: FileHashList,
    ):
        self.file_id = file_id
        self.submission_id = submission_id
        self.site_file_id = site_file_id
        self.is_current = is_current
        self.first_scanned = first_scanned
        self.latest_update = latest_update
        self.file_url = file_url
        self.file_size = file_size
        self.extra_data = extra_data
        self.hashes = hashes
        self.updated = False

    def is_clashing(self, update: FileUpdate) -> bool:
        if update.file_url is not UNSET and self.file_url is not None and self.file_url != update.file_url:
            return True
        if update.file_size is not UNSET and self.file_size is not None and self.file_size != update.file_size:
            return True
        if update.add_hashes is not UNSET and self.hashes.is_clashing(update.add_hashes):
            return True
        return False

    def add_update(self, update: FileUpdate) -> None:
        if update.update_time > self.latest_update:
            self.latest_update = update.update_time
            self.updated = True
            if update.file_url is not UNSET:
                self.file_url = update.file_url
            if update.file_size is not UNSET:
                self.file_size = update.file_size
            if update.add_extra_data is not UNSET:
                self.extra_data = merge_dicts(self.extra_data, update.add_extra_data)
            if update.add_hashes is not UNSET:
                self.hashes.add_update(update.add_hashes)
            return
        if update.update_time < self.first_scanned:
            self.first_scanned = update.update_time
            self.updated = True
        if update.add_extra_data is not UNSET:
            self.extra_data = merge_dicts(update.add_extra_data, self.extra_data)
            self.updated = True
        if update.add_hashes is not UNSET:
            self.hashes.add_update(update.add_hashes)
            self.updated = self.updated or self.hashes.updated

    def save(self, db: Database) -> None:
        if self.updated:
            db.update(
                "UPDATE files SET first_scanned = %s, latest_update = %s, file_url = %s, file_size = %s, "
                "extra_data = %s "
                "WHERE file_id = %s",
                (
                    self.first_scanned, self.latest_update, self.file_url, self.file_size, json_to_db(self.extra_data),
                    self.file_id
                )
            )
        self.hashes.save(db)


class FileUpdate:
    def __init__(
            self,
            file_url: str = UNSET,
            *,
            update_time: datetime.datetime = None,
            site_file_id: str = UNSET,
            file_size: str = UNSET,
            add_extra_data: Dict[str, Any] = UNSET,
            add_hashes: FileHashListUpdate = UNSET,
    ):
        self.file_url = file_url
        self.update_time = update_time or datetime.datetime.now(datetime.timezone.utc)
        self.update_time_set = update_time is not None
        self.site_file_id = site_file_id
        self.file_size = file_size
        self.add_extra_data = add_extra_data
        self.add_hashes = add_hashes

    def create(self, db: Database, submission_id: int) -> File:
        site_file_id = unset_to_null(self.site_file_id)
        file_url = unset_to_null(self.file_url)
        file_size = unset_to_null(self.file_size)
        extra_data = unset_to_null(self.add_extra_data)
        file_rows = db.insert(
            "INSERT INTO files "
            "(submission_id, site_file_id, is_current, first_scanned, latest_update, file_url, file_size, extra_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING file_id",
            (submission_id, site_file_id, True, self.update_time, self.update_time, file_url, file_size, extra_data)
        )
        file_id = file_rows[0][0]
        hashes = None
        if self.add_hashes is not UNSET:
            hashes = self.add_hashes.create(db, file_id)
        return File(
            file_id,
            submission_id,
            site_file_id,
            True,
            self.update_time,
            self.update_time,
            file_url,
            file_size,
            extra_data,
            hashes
        )


class FileList:
    def __init__(
            self,
            sub_id: int,
            files: List[File],
    ) -> None:
        self.sub_id = sub_id
        self.files = files
        self.add_files: List[FileUpdate] = []

    @property
    def updated(self) -> bool:
        if self.add_files:
            return True
        return any(file.updated for file in self.files)

    def current_matching_file(self, site_file_id: Optional[str]) -> Optional[File]:
        return next(
            filter(
                lambda file: file.is_current and file.site_file_id == site_file_id,
                self.files
            ),
            None
        )

    def add_file_update(self, update: FileUpdate) -> None:
        # Check if a file exists for that file ID
        site_file_id = unset_to_null(update.site_file_id)
        matching = self.current_matching_file(site_file_id)
        if matching is None:
            self.add_files.append(update)
            return
        # Check if the update clashes with the file that exists
        if matching.is_clashing(update):
            if update.update_time > matching.latest_update:
                matching.is_current = False
                matching.updated = True
                update.is_current = True
                self.add_files.append(update)
                return
            elif update.update_time < matching.first_scanned:
                update.is_current = False
                self.add_files.append(update)
                return
            else:
                raise ValueError("File Update clashes, confusing")
        else:
            matching.add_update(update)

    def add_update(self, update: FileListUpdate) -> None:
        for file_update in update.file_updates:
            self.add_file_update(file_update)

    @classmethod
    def from_database(cls, db: Database, sub_id: int) -> FileList:
        file_rows = db.select(
            "SELECT file_id, site_file_id, is_current, first_scanned, latest_update, file_url, file_size, extra_data "
            "FROM files WHERE submission_id = %s",
            (sub_id,)
        )
        files = []
        for file_row in file_rows:
            file_id, site_file_id, is_current, first_scanned, latest_update, file_url, file_size, extra_data = file_row
            hashes = FileHashList.from_database(db, file_id)
            files.append(File(
                file_id,
                sub_id,
                site_file_id, is_current, first_scanned, latest_update, file_url, file_size, extra_data, hashes
            ))
        return FileList(sub_id, files)

    def save(self, db: Database) -> None:
        for file in self.files:
            if file.updated:
                file.save(db)
        new_files = []
        for update in self.add_files:
            new_files.append(update.create(db, self.sub_id))
        self.files += new_files
        self.add_files.clear()


class FileListUpdate:
    def __init__(
            self,
            file_updates: List[FileUpdate],
    ) -> None:
        self.file_updates = file_updates

    def create(self, db: Database, submission_id: int) -> Optional[FileList]:
        file_list = []
        for file_update in self.file_updates:
            file_list.append(file_update.create(db, submission_id))
        return FileList(submission_id, file_list)


class FileHash:
    def __init__(
            self,
            hash_id: int,
            file_id: int,
            algo_id: str,
            hash_value: str
    ) -> None:
        self.hash_id = hash_id
        self.file_id = file_id
        self.algo_id = algo_id
        self.hash_value = hash_value


class FileHashUpdate:
    def __init__(
            self,
            algo_id: str,
            hash_value: str
    ):
        self.algo_id = algo_id
        self.hash_value = hash_value

    def create(self, db: Database, file_id: int) -> FileHash:
        hash_rows = db.insert(
            "INSERT INTO file_hashes (file_id, algo_id, hash_value) VALUES (%s, %s, %s) RETURNING hash_id",
            (file_id, self.algo_id, self.hash_value)
        )
        hash_id = hash_rows[0][0]
        return FileHash(
            hash_id,
            file_id,
            self.algo_id,
            self.hash_value
        )


class FileHashList:
    def __init__(
            self,
            file_id: int,
            hashes: List[FileHash]
    ) -> None:
        self.file_id = file_id
        self.hashes = hashes
        self.add_hashes: List[FileHashUpdate] = []

    @property
    def updated(self) -> bool:
        return bool(self.add_hashes)

    @classmethod
    def from_database(cls, db: Database, file_id: int) -> "FileHashList":
        hash_rows = db.select(
            "SELECT hash_id, algo_id, hash_value FROM file_hashes WHERE file_id = %s",
            (file_id,)
        )
        return FileHashList(
            file_id,
            [FileHash(hash_id, file_id, algo_id, hash_value) for hash_id, algo_id, hash_value in hash_rows]
        )

    def is_clashing(self, update: FileHashListUpdate) -> bool:
        my_algo_ids = {file_hash.algo_id for file_hash in self.hashes}
        update_hash_ids = {file_hash.algo_id for file_hash in update.hashes}
        return bool(my_algo_ids.intersection(update_hash_ids))

    def has_hash(self, new_hash: FileHashUpdate) -> bool:
        for file_hash in self.hashes:
            if file_hash.algo_id == new_hash.algo_id:
                return True

    def add_update(self, update: FileHashListUpdate) -> None:
        self.add_hashes = []
        for file_hash in update.hashes:
            if not self.has_hash(file_hash):
                self.add_hashes.append(file_hash)

    def save(self, db: Database) -> None:
        new_hashes = []
        for file_hash in self.add_hashes:
            new_hashes.append(file_hash.create(db, self.file_id))
        self.hashes += new_hashes
        self.add_hashes.clear()


class FileHashListUpdate:
    def __init__(
            self,
            hashes: List[FileHashUpdate]
    ) -> None:
        self.hashes = hashes

    def create(self, db: Database, file_id: int) -> FileHashList:
        return FileHashList(
            file_id,
            [update.create(db, file_id) for update in self.hashes]
        )
