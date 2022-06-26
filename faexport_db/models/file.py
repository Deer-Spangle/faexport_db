from __future__ import annotations
from typing import Optional, Dict, Any, List, TYPE_CHECKING

from faexport_db.db import UNSET, Database

if TYPE_CHECKING:
    import datetime


class File:
    def __init__(
            self,
            file_id: int,
            submission_id: int,
            site_file_id: Optional[str],
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
        self.first_scanned = first_scanned
        self.latest_update = latest_update
        self.file_url = file_url
        self.file_size = file_size
        self.extra_data = extra_data
        self.hashes = hashes


class FileList:
    def __init__(
            self,
            sub_id: int,
            files: List[File],
    ) -> None:
        self.sub_id = sub_id
        self.files = files

    @classmethod
    def from_database(cls, db: Database, sub_id: int) -> Optional[FileList]:
        file_rows = db.select(
            "SELECT file_id, site_file_id, first_scanned, latest_update, file_url, file_size, extra_data "
            "FROM files WHERE submission_id = %s",
            (sub_id,)
        )
        if not file_rows:
            return None
        files = []
        for file_row in file_rows:
            file_id, site_file_id, first_scanned, latest_update, file_url, file_size, extra_data = file_row
            hashes = FileHashList.from_database(db, file_id)
            files.append(File(
                file_id,
                sub_id,
                site_file_id, first_scanned, latest_update, file_url, file_size, extra_data, hashes
            ))
        return FileList(sub_id, files)


class FileUpdate:
    def __init__(
            self,
            file_url: str = UNSET,
            *,
            file_size: str = UNSET,
            add_extra_data: Dict[str, Any] = UNSET,
            add_hashes: List["FileHashUpdate"] = UNSET,
    ):
        self.file_url = file_url
        self.file_size = file_size
        self.add_extra_data = add_extra_data
        self.add_hashes = add_hashes


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


class FileHashList:
    def __init__(
            self,
            file_id: int,
            hashes: List[FileHash]
    ) -> None:
        self.file_id = file_id
        self.hashes = hashes

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
