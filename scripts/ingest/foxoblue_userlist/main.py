import datetime
import json
import csv
from pathlib import Path
from typing import Optional, Iterator

import dateutil.parser
import psycopg2

from faexport_db.ingest_formats.base import FormatResponse
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.user import UserSnapshot

from faexport_db.db import Database
from faexport_db.models.website import Website
from scripts.ingest.ingestion_job import IngestionJob, RowType, cache_in_file, csv_count_rows

CSV_LOCATION = "./dump/foxoblue_userlist/data-1642685938898.csv"
SITE_ID = "fa"
WEBSITE = Website(SITE_ID, "Fur Affinity", "https://furaffinity.net")
DATA_DATE = datetime.datetime(2022, 1, 20, 13, 38, 58, tzinfo=datetime.timezone.utc)
CONTRIBUTOR = ArchiveContributor("Foxo//Blue FA user list export")


class FoxoBlueUserListIngestionJob(IngestionJob):

    def __init__(self):
        super().__init__()
        self.csv_location = CSV_LOCATION
        self.row_count_file = Path(__file__) / "cache_row_count.txt"
        self.earliest_date_file = Path(__file__) / "cache_earliest_date.txt"

    def _earliest_date_in_csv(self) -> datetime.datetime:
        earliest = "zzz"
        with open(self.csv_location, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for line in reader:
                if line[5]:
                    earliest = min(earliest, line[5])
        return dateutil.parser.parse(earliest)

    def row_count(self) -> Optional[int]:
        return int(cache_in_file(self.row_count_file, lambda: str(csv_count_rows(self.csv_location))))

    def earliest_date(self) -> datetime.datetime:
        return dateutil.parser.parse(
            cache_in_file(
                self.earliest_date_file,
                lambda: self._earliest_date_in_csv().isoformat()
            )
        )

    def convert_row(self, row: RowType) -> FormatResponse:
        username, updated_at, error = row.values()
        if error == "unknown_user":
            return FormatResponse()
        scan_datetime = self.earliest_date()
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
        return FormatResponse(user_snapshots=[snapshot])

    def iterate_rows(self) -> Iterator[RowType]:
        with open(self.csv_location, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                yield row


if __name__ == "__main__":
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    WEBSITE.save(db_obj)
    CONTRIBUTOR.save(db_obj)

    ingestion_job = FoxoBlueUserListIngestionJob()
    ingestion_job.process(db_obj)
