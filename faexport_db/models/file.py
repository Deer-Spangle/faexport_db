from __future__ import annotations
from typing import Optional, Dict, Any, List
import datetime

from faexport_db.db import UNSET, Database, unset_to_null, merge_dicts, json_to_db


class File:
    def __init__(
            self,
            file_id: int,
            site_file_id: Optional[str],
            *,
            submission_snapshot_id: int = None,
            file_url: Optional[str] = None,
            file_size: Optional[int] = None,
            extra_data: Optional[Dict[str, Any]] = None,
            hashes: List[FileHash] = None,
    ):
        self.file_id = file_id
        self.site_file_id = site_file_id
        self.submission_snapshot_id = submission_snapshot_id
        self.file_url = file_url
        self.file_size = file_size
        self.extra_data = extra_data
        self.hashes = hashes

    def is_clashing(self, update: FileUpdate) -> bool:
        if update.file_url is not UNSET and self.file_url is not None and self.file_url != update.file_url:
            return True
        if update.file_size is not UNSET and self.file_size is not None and self.file_size != update.file_size:
            return True
        if update.add_hashes is not UNSET and self.hashes.is_clashing(update.add_hashes):
            return True
        return False

    def create_snapshot(self, db: "Database") -> None:
        file_rows = db.insert(
            "INSERT INTO submission_snapshot_files "
            "(submission_snapshot_id, site_file_id, file_url, file_size, extra_data) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING file_id",
            (self.submission_snapshot_id, self.site_file_id, self.file_url, self.file_size, self.extra_data)
        )
        self.file_id = file_rows[0][0]

    def save(self, db: Database, submission_snapshot_id: int) -> None:
        self.submission_snapshot_id = submission_snapshot_id
        if self.file_id is not None:
            self.create_snapshot(db)
        for file_hash in self.hashes:
            file_hash.save(db, self.file_id)

    @classmethod
    def list_for_submission_snapshot(db: Database, submission_snapshot_id: int) -> List["File"]:
        file_rows = db.select(
            "SELECT file_id, site_file_id, file_url, file_size, extra_data "
            "FROM submission_snapshot_files "
            "WHERE submission_snapshot_id = %s",
            (submission_snapshot_id,)
        )
        files = []
        for file_row in file_rows:
            file_id, site_file_id, file_url, file_size, extra_data = file_row
            hashes = FileHash.list_for_file(db, file_id)
            files.append(File(
                file_id,
                site_file_id,
                submission_snapshot_id=submission_snapshot_id,
                file_url=file_url,
                file_size=file_size,
                extra_data=extra_data,
                hashes=hashes,
            ))
        return files


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
            algo_id: str,
            hash_value: bytes,
            *,
            file_id: int = None,
            hash_id: int = None,
    ) -> None:
        self.algo_id = algo_id
        self.hash_value = hash_value
        self.file_id = file_id
        self.hash_id = hash_id

    def create_snapshot(self, db: Database) -> None:
        hash_rows = db.insert(
            "INSERT INTO submission_snapshot_file_hashes "
            "(file_id, algo_id, hash_value) "
            "VALUES (%s, %s, %s) RETURNING hash_id",
            (self.file_id, self.algo_id, self.hash_value)
        )
        self.hash_id = hash_rows[0][0]

    def save(self, db: Database, file_id: int) -> None:
        self.file_id = file_id
        if self.hash_id is None:
            self.create_snapshot(db)
    
    @classmethod
    def list_for_file(db: Database, file_id: int) -> List["FileHash"]:
        hash_rows = db.select(
            "SELECT hash_id, algo_id, hash_value "
            "FROM submission_snapshot_file_hashes "
            "WHERE file_id = %s",
            (file_id,)
        )
        hashes = []
        for hash_row in hash_rows:
            hash_id, algo_id, hash_value = hash_row
            hashes.append(FileHash(
                algo_id,
                hash_value,
                file_id=file_id,
                hash_id=hash_id,
            ))
        return hashes


class FileHashUpdate:
    def __init__(
            self,
            algo_id: str,
            hash_value: bytes
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
