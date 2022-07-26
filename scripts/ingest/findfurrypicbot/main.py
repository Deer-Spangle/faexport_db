import datetime
import json
import sqlite3
from typing import Iterator, Optional

import psycopg2

from faexport_db.ingest_formats.base import FormatResponse
from faexport_db.models.archive_contributor import ArchiveContributor
import tqdm

from faexport_db.db import Database
from faexport_db.models.file import FileHash, HashAlgo, File
from faexport_db.models.submission import SubmissionSnapshot
from faexport_db.models.website import Website
from scripts.ingest.ingestion_job import IngestionJob

DB_LOCATION = "./dump/findfurrypicbot/fa_bin/fa_bin.sqlite3"
SITE_ID = "fa"
WEBSITE = Website(SITE_ID, "Fur Affinity", "https://furaffinity.net")
DATA_DATE = datetime.datetime(2020, 1, 9, 0, 0, 0, tzinfo=datetime.timezone.utc)
CONTRIBUTOR = ArchiveContributor("FindFurryPicBot data ingest")
AHASH = HashAlgo("python", "ahash")
DHASH = HashAlgo("python", "dhash")
PHASH = HashAlgo("python", "phash")
WHASH = HashAlgo("python", "whash")


# noinspection SqlResolve
class FindFurryPicBotIngestion(IngestionJob):

    def __init__(self, sqlite_db: sqlite3.Connection, *, skip_rows: int = 0) -> None:
        super().__init__(skip_rows=skip_rows)
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
    WEBSITE.save(db_obj)
    CONTRIBUTOR.save(db_obj)
    AHASH.save(db_obj)
    DHASH.save(db_obj)
    PHASH.save(db_obj)
    WHASH.save(db_obj)

    sqlite_conn = sqlite3.connect(DB_LOCATION)
    sqlite_conn.row_factory = sqlite3.Row

    ingestor = FindFurryPicBotIngestion(sqlite_conn)
    ingestor.process(db_obj)
