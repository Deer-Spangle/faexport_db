import datetime
import json
import csv
from typing import Optional, List, Dict

import dateutil.parser
import psycopg2
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.user import UserSnapshot
import tqdm

from faexport_db.db import Database
from scripts.ingest.fa_indexer.main import setup_initial_data

CSV_LOCATION = "./dump/foxoblue_userlist/data-1642685938898.csv"
SITE_ID = "fa"
DATA_DATE = datetime.datetime(2022, 1, 20, 13, 38, 58, tzinfo=datetime.timezone.utc)
CONTRIBUTOR = ArchiveContributor("Foxo//Blue FA user list export")


def import_row(row: Dict[str, str], db: Database) -> Optional[UserSnapshot]:
    username, updated_at, error = row.values()
    if error == "unknown_user":
        return None
    scan_datetime = csv_earliest_date()
    if updated_at != "NULL":
        scan_datetime = dateutil.parser.parse(updated_at)
    is_deleted = error != "NULL"
    extra_data = None
    if is_deleted:
        extra_data = {
            "deletion_type": error
        }
    snapshot = UserSnapshot(
        SITE_ID,
        username,
        CONTRIBUTOR,
        scan_datetime,
        is_deleted=is_deleted,
        extra_data=extra_data,
    )
    snapshot.save(db)
    return snapshot


def csv_row_count() -> int:
    with open(CSV_LOCATION, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        return sum(1 for _ in tqdm.tqdm(reader))


def csv_earliest_date() -> datetime.datetime:
    # TODO: caching
    earliest = "zzz"
    with open(CSV_LOCATION, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        for line in reader:
            if line[5]:
                earliest = min(earliest, line[5])
    return dateutil.parser.parse(earliest)


def ingest_csv(db: Database) -> None:
    row_count = csv_row_count()
    with open(CSV_LOCATION, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in tqdm.tqdm(reader, total=row_count):
            import_row(dict(row), db)


if __name__ == "__main__":
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    setup_initial_data(db_obj, CONTRIBUTOR)

    ingest_csv(db_obj)
