from __future__ import annotations
from typing import Optional, Dict, Any, List
import base64

from faexport_db.db import Database, merge_dicts, json_to_db


class File:
    def __init__(
            self,
            site_file_id: Optional[str],
            *,
            file_id: int = None,
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
        self.hashes = hashes or []
    
    @property
    def hash_map_by_algo(self) -> Dict[int, FileHash]:
        return {file_hash.algo_id: file_hash for file_hash in self.hashes}

    def to_web_json(self) -> Dict:
        return {
            "file_url": self.file_url,
            "file_size": self.file_size,
            "extra_data": self.extra_data,
            "file_hashes": [file_hash.to_web_json() for file_hash in self.hashes],
        }

    def is_clashing(self, update: File) -> bool:
        if update.file_url is not None and self.file_url is not None and self.file_url != update.file_url:
            return True
        if update.file_size is not None and self.file_size is not None and self.file_size != update.file_size:
            return True
        # Check hashes do not conflict
        my_hash_ids = set(self.hash_map_by_algo.keys())
        update_hash_ids = set(update.hash_map_by_algo.keys())
        hash_overlap = my_hash_ids.intersection(update_hash_ids)
        for algo_id in hash_overlap:
            if self.hash_map_by_algo[algo_id].hash_value != update.hash_map_by_algo[algo_id].hash_value:
                return True
        # We don't check extra_data, as that is assumed mutable.
        return False
    
    def add_update(self, update: File) -> None:
        # Don't update file_url or file_size, as they are immutable
        self.extra_data = merge_dicts(self.extra_data, update.extra_data)
        # Add any new file hashes
        my_hash_map = self.hash_map_by_algo
        for file_hash in update.hashes:
            if file_hash.algo_id not in my_hash_map:
                self.hashes.append(file_hash)

    def create_snapshot(self, db: "Database") -> None:
        file_rows = db.insert(
            "WITH e AS ( "
            "INSERT INTO submission_snapshot_files "
            "(submission_snapshot_id, site_file_id, file_url, file_size, extra_data) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (submission_snapshot_id, site_file_id) DO NOTHING "
            "RETURNING file_id "
            ") SELECT * FROM e "
            "UNION SELECT file_id FROM submission_snapshot_files "
            "WHERE submission_snapshot_id = %s AND site_file_id = %s",
            (
                self.submission_snapshot_id, self.site_file_id,
                self.file_url, self.file_size, json_to_db(self.extra_data),
                self.submission_snapshot_id, self.site_file_id
            )
        )
        if not file_rows:
            file_rows = db.select(
                "SELECT file_id FROM submission_snapshot_files WHERE submission_snapshot_id = %s AND site_file_id = %s",
                (self.submission_snapshot_id, self.site_file_id)
            )
        self.file_id = file_rows[0][0]

    def save(self, db: Database, submission_snapshot_id: int) -> None:
        self.submission_snapshot_id = submission_snapshot_id
        if self.file_id is not None:
            self.create_snapshot(db)
        for file_hash in self.hashes:
            file_hash.save(db, self.file_id)

    @classmethod
    def list_for_submission_snapshot(cls, db: Database, submission_snapshot_id: int) -> List["File"]:
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
            files.append(cls(
                site_file_id,
                file_id=file_id,
                submission_snapshot_id=submission_snapshot_id,
                file_url=file_url,
                file_size=file_size,
                extra_data=extra_data,
                hashes=hashes,
            ))
        return files


class FileHash:
    def __init__(
            self,
            algo_id: int,
            hash_value: bytes,
            *,
            file_id: int = None,
            hash_id: int = None,
    ) -> None:
        self.algo_id = algo_id
        self.hash_value = hash_value
        self.file_id = file_id
        self.hash_id = hash_id
    
    def to_web_json(self) -> Dict:
        return {
            "algo_id": self.algo_id,
            "hash_value": base64.b64encode(self.hash_value).decode(),
        }

    def create_snapshot(self, db: Database) -> None:
        hash_rows = db.insert(
            "WITH e AS ( "
            "INSERT INTO submission_snapshot_file_hashes "
            "(file_id, algo_id, hash_value) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (file_id, hash_id) DO NOTHING "
            "RETURNING hash_id "
            ") SELECT * FROM e "
            "UNION SELECT hash_id FROM submission_snapshot_file_hashes WHERE file_id = %s AND algo_id = %s",
            (self.file_id, self.algo_id, self.hash_value, self.file_id, self.algo_id)
        )
        if not hash_rows:
            hash_rows = db.select(
                "SELECT hash_id FROM submission_snapshot_file_hashes WHERE file_id = %s AND algo_id = %s",
                (self.file_id, self.algo_id)
            )
        self.hash_id = hash_rows[0][0]

    def save(self, db: Database, file_id: int) -> None:
        self.file_id = file_id
        if self.hash_id is None:
            self.create_snapshot(db)
    
    @classmethod
    def list_for_file(cls, db: Database, file_id: int) -> List["FileHash"]:
        hash_rows = db.select(
            "SELECT hash_id, algo_id, hash_value "
            "FROM submission_snapshot_file_hashes "
            "WHERE file_id = %s",
            (file_id,)
        )
        hashes = []
        for hash_row in hash_rows:
            hash_id, algo_id, hash_value = hash_row
            hashes.append(cls(
                algo_id,
                hash_value,
                file_id=file_id,
                hash_id=hash_id,
            ))
        return hashes


class HashAlgo:

    def __init__(
        self,
        language: str,
        algorithm_name: str,
        *,
        algo_id: int = None
    ):
        self.language = language
        self.algorithm_name = algorithm_name
        self.algo_id = algo_id

    def to_web_json(self) -> Dict:
        return {
            "algo_id": self.algo_id,
            "language": self.language,
            "algorithm_name": self.algorithm_name,
        }
    
    def _create(self, db: Database) -> None:
        algo_rows = db.insert(
            "WITH e AS ( "
            "INSERT INTO hash_algos (language, algorithm_name) "
            "VALUES (%s, %s) "
            "ON CONFLICT (language, algorithm_name) DO NOTHING "
            "RETURNING algo_id "
            ") SELECT * FROM e "
            "UNION SELECT algo_id FROM hash_algos "
            "WHERE language = %s AND algorithm_name = %s",
            (self.language, self.algorithm_name, self.language, self.algorithm_name)
        )
        if not algo_rows:
            algo_rows = db.select(
                "SELECT algo_id FROM hash_algos WHERE language = %s AND algorithm_name = %s",
                (self.language, self.algorithm_name)
            )
        self.algo_id = algo_rows[0][0]
    
    def save(self, db: Database) -> None:
        if self.algo_id is None:
            self._create(db)
    
    @classmethod
    def list_all(cls, db: Database) -> List["HashAlgo"]:
        algo_rows = db.select(
            "SELECT algo_id, language, algorithm_name FROM hash_algos",
            tuple()
        )
        hash_algos = []
        for algo_row in algo_rows:
            algo_id, language, name = algo_row
            hash_algos.append(HashAlgo(
                language,
                name,
                algo_id=algo_id,
            ))
        return hash_algos
