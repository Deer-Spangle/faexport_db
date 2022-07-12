import datetime
import json
import sqlite3

import psycopg2
from faexport_db.models.archive_contributor import ArchiveContributor
import tqdm

from faexport_db.db import Database
from faexport_db.models.file import FileHash, FileListUpdate, FileUpdate, FileHashListUpdate, FileHashUpdate
from faexport_db.models.submission import SubmissionSnapshot, SubmissionUpdate, Submission
from scripts.ingest.fa_indexer.main import setup_initial_data

DB_LOCATION = "./dump/findfurrypicbot/fa_bin/fa_bin.sqlite3"
SITE_ID = "fa"
DATA_DATE = datetime.datetime(2020, 1, 9, 0, 0, 0, tzinfo=datetime.timezone.utc)
CONTRIBUTOR = ArchiveContributor("FindFurryPicBot data ingest")


def import_row(row: sqlite3.Row, db: Database) -> SubmissionSnapshot:
    snapshot = SubmissionSnapshot(
        SITE_ID,
        str(row["id"]),
        CONTRIBUTOR,
        DATA_DATE,
        files=[
            File(
                None,
                hashes=[
                    FileHash("python:ahash", row["a_hash"]),
                    FileHash("python:dhash", row["d_hash"]),
                    FileHash("python:phash", row["p_hash"]),
                    FileHash("python:whash", row["w_hash"]),
                ]
            )
        ]
    )
    snapshot.save(db)
    return snapshot


def ingest_data(sqlite_db: sqlite3.Connection, db: Database) -> None:
    cur = sqlite_db.cursor()
    result = cur.execute("SELECT COUNT(1) as count FROM posts")
    row_count = next(result)["count"]
    cur.close()
    cur = sqlite_db.cursor()
    result = cur.execute("SELECT id, a_hash, p_hash, d_hash, w_hash FROM posts")
    for row in tqdm.tqdm(result, total=row_count):
        import_row(row, db)
    sqlite_db.commit()
    cur.close()


if __name__ == "__main__":
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    setup_initial_data(db_obj, CONTRIBUTOR)

    sqlite_conn = sqlite3.connect(DB_LOCATION)
    sqlite_conn.row_factory = sqlite3.Row

    ingest_data(sqlite_conn, db_obj)
