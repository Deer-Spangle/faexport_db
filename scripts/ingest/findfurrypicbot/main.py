import datetime
import json
import sqlite3
from pathlib import Path

import psycopg2
import tqdm

from faexport_db.db import Database
from faexport_db.models.file import FileListUpdate, FileUpdate, FileHashListUpdate, FileHashUpdate
from faexport_db.models.submission import SubmissionUpdate, Submission
from scripts.ingest.fa_indexer.main import setup_initial_data

DB_LOCATION = "./dump/findfurrypicbot/fa_bin/fa_bin.sqlite3"
SITE_ID = "fa"
DATA_DATE = datetime.datetime(2020, 1, 9, 0, 0, 0, tzinfo=datetime.timezone.utc)


def import_row(row: sqlite3.Row, db: Database) -> Submission:
    update = SubmissionUpdate(
        SITE_ID,
        str(row["id"]),
        DATA_DATE,
        files=FileListUpdate([FileUpdate(
            add_hashes=FileHashListUpdate([
                FileHashUpdate("python:ahash", row["a_hash"]),
                FileHashUpdate("python:dhash", row["d_hash"]),
                FileHashUpdate("python:phash", row["p_hash"]),
                FileHashUpdate("python:whash", row["w_hash"])
            ])
        )])
    )
    return update.save(db)


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
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    setup_initial_data(db_obj)

    sqlite_conn = sqlite3.connect(DB_LOCATION)
    sqlite_conn.row_factory = sqlite3.Row

    ingest_data(sqlite_conn, db_obj)
