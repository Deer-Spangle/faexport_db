import datetime
from typing import Optional, Dict, Any, List

from scripts.ingest.fa_indexer.models.db import UNSET


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
            extra_data: Optional[Dict[str, Any]]
    ):
        self.file_id = file_id
        self.submission_id = submission_id
        self.site_file_id = site_file_id
        self.first_scanned = first_scanned
        self.latest_update = latest_update
        self.file_url = file_url
        self.file_size = file_size
        self.extra_data = extra_data


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


class FileHashUpdate:
    def __init__(
            self,
            algo_id: str,
            hash_value: str
    ):
        self.algo_id = algo_id
        self.hash_value = hash_value
