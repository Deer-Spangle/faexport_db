import datetime
import json
import sqlite3
from abc import abstractmethod, ABC
from typing import Iterator, Optional, TypeVar, List, Tuple

import psycopg2

from faexport_db.ingest_formats.base import FormatResponse
from faexport_db.models.archive_contributor import ArchiveContributor
import tqdm

from faexport_db.db import Database
from faexport_db.models.file import FileHash, HashAlgo, File
from faexport_db.models.submission import SubmissionSnapshot
from faexport_db.models.user import UserSnapshot
from scripts.ingest.fa_indexer.main import setup_initial_data

DB_LOCATION = "./dump/findfurrypicbot/fa_bin/fa_bin.sqlite3"
SITE_ID = "fa"
DATA_DATE = datetime.datetime(2020, 1, 9, 0, 0, 0, tzinfo=datetime.timezone.utc)
CONTRIBUTOR = ArchiveContributor("FindFurryPicBot data ingest")
AHASH = HashAlgo("python", "ahash")
DHASH = HashAlgo("python", "dhash")
PHASH = HashAlgo("python", "phash")
WHASH = HashAlgo("python", "whash")


RowType = TypeVar("RowType")


class IngestionJob(ABC):
    SAVE_AFTER = 100

    @abstractmethod
    def row_count(self) -> Optional[int]:
        pass

    @abstractmethod
    def convert_row(self, row: RowType) -> FormatResponse:
        pass

    @abstractmethod
    def iterate_rows(self) -> Iterator[RowType]:
        pass

    def ingest_data(self, db: Database) -> None:
        submissions_by_row: List[Tuple[int, SubmissionSnapshot]] = []
        users_by_row: List[Tuple[int, UserSnapshot]] = []

        progress = tqdm.tqdm(self.iterate_rows(), desc="Scanning data", total=self.row_count())
        for row_num, row in enumerate(progress):
            result = self.convert_row(row)
            # Add result to cached rows
            for snapshot in result.submission_snapshots:
                submissions_by_row.append((row_num, snapshot))
            for snapshot in result.user_snapshots:
                users_by_row.append((row_num, snapshot))
            # Check whether to save submissions
            if len(submissions_by_row) > self.SAVE_AFTER:
                progress.set_description(f"Saving {len(submissions_by_row)} submission snapshots")
                SubmissionSnapshot.save_batch(db, [snapshot for _, snapshot in submissions_by_row])
                submissions_by_row.clear()
            # Check whether to save users
            if len(users_by_row) > self.SAVE_AFTER:
                progress.set_description(f"Saving {len(users_by_row)} user snapshots")
                UserSnapshot.save_batch(db, [snapshot for _, snapshot in users_by_row])
                users_by_row.clear()
            # Update description
            row_nums = [num for num, _ in submissions_by_row] + [num for num, _ in users_by_row]
            lowest_row_num = row_num
            if row_nums:
                lowest_row_num = min(row_nums)
            progress.set_description(
                f"Collecting snapshots ({len(submissions_by_row)}s, {len(users_by_row)}u, >{lowest_row_num}"
            )
        SubmissionSnapshot.save_batch(db, [snapshot for _, snapshot in submissions_by_row])
        UserSnapshot.save_batch(db, [snapshot for _, snapshot in users_by_row])


# noinspection SqlResolve
class FindFurryPicBotIngestion(IngestionJob):

    def __init__(self, sqlite_db: sqlite3.Connection) -> None:
        self.sqlite_db = sqlite_db

    def row_count(self) -> Optional[int]:
        cur = self.sqlite_db.cursor()
        result = cur.execute("SELECT COUNT(1) as count FROM posts")
        row_count = next(result)["count"]
        cur.close()
        return row_count

    def convert_row(self, row: sqlite3.Row) -> FormatResponse:
        snapshot = SubmissionSnapshot(
            SITE_ID,
            str(row["id"]),
            CONTRIBUTOR,
            DATA_DATE,
            files=[
                File(
                    None,
                    hashes=[
                        FileHash(AHASH.algo_id, row["a_hash"]),
                        FileHash(DHASH.algo_id, row["d_hash"]),
                        FileHash(PHASH.algo_id, row["p_hash"]),
                        FileHash(WHASH.algo_id, row["w_hash"]),
                    ]
                )
            ]
        )
        return FormatResponse(
            [snapshot]
        )

    def iterate_rows(self) -> Iterator[sqlite3.Row]:
        cur = self.sqlite_db.cursor()
        result = cur.execute("SELECT COUNT(1) as count FROM posts")
        row_count = next(result)["count"]
        cur.close()
        cur = self.sqlite_db.cursor()
        result = cur.execute("SELECT id, a_hash, p_hash, d_hash, w_hash FROM posts")
        for row in tqdm.tqdm(result, total=row_count):
            yield row
        self.sqlite_db.commit()
        cur.close()


if __name__ == "__main__":
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    setup_initial_data(db_obj, CONTRIBUTOR)
    AHASH.save(db_obj)
    DHASH.save(db_obj)
    PHASH.save(db_obj)
    WHASH.save(db_obj)

    sqlite_conn = sqlite3.connect(DB_LOCATION)
    sqlite_conn.row_factory = sqlite3.Row

    ingestor = FindFurryPicBotIngestion(sqlite_conn)
    ingestor.ingest_data(db_obj)
