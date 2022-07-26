import base64
import datetime
import json
import csv
import sys
from pathlib import Path
from typing import Optional, List, Iterator

import dateutil.parser
import psycopg2

from faexport_db.ingest_formats.base import FormatResponse
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.file import HashAlgo, File, FileHash
from faexport_db.models.submission import SubmissionSnapshot

from faexport_db.db import Database, parse_datetime
from faexport_db.models.website import Website
from scripts.ingest.ingestion_job import IngestionJob, RowType, cache_in_file, csv_count_rows

CSV_LOCATION = "./dump/e621_db_export/posts-2022-07-13.csv"
WEBSITE = Website("e6", "e621", "https://e621.net")
DATA_DATE = datetime.datetime(2022, 7, 13, 0, 0, 0, tzinfo=datetime.timezone.utc)
CONTRIBUTOR = ArchiveContributor("e621 db_export")
MD5_HASH = HashAlgo("any", "md5")


def validate_row(row: List[str]) -> None:
    post_id, uploader_id, created_at, md5, source, rating, image_width, image_height, tag_string, locked_tags, fav_count, file_ext, parent_id, change_seq, approver_id, file_size, comment_count, description, duration, updated_at, is_deleted, is_pending, is_flagged, score, up_score, down_score, is_rating_locked, is_status_locked, is_note_locked = row
    assert created_at
    assert dateutil.parser.parse(created_at) is not None
    assert base64.b64decode(md5.encode('ascii')) is not None
    assert is_deleted in "tf"
    assert md5
    assert len(md5) > 4
    assert int(image_width) is not None  # A few swf files have negative image width/height
    assert int(image_height) is not None
    if duration:
        assert float(duration) is not None  # Some swf files have 0.0 duration
    assert rating in "eqs"
    assert int(fav_count) >= 0
    assert int(comment_count) is not None  # Some posts have negative comment count, e.g. 1195029
    if updated_at:
        assert dateutil.parser.parse(updated_at) is not None
    assert is_pending in "tf"
    assert is_flagged in "tf"
    assert int(score) is not None
    assert int(down_score) <= 0
    assert int(up_score) >= 0
    assert is_rating_locked in "tf"
    assert is_status_locked in "tf"
    assert is_note_locked in "tf"


class E621IngestJob(IngestionJob):

    def __init__(self, *, skip_rows: int = 0):
        super().__init__(skip_rows=skip_rows)
        self.csv_location = CSV_LOCATION
        self.row_count_file = Path(__file__) / "cache_row_count.txt"

        # Set up field size limit to be able to handle e621 data dumps
        max_int = sys.maxsize
        while True:
            try:
                csv.field_size_limit(max_int)
                break
            except OverflowError:
                max_int = int(max_int / 10)

    def row_count(self) -> Optional[int]:
        return int(cache_in_file(self.row_count_file, lambda: str(csv_count_rows(self.csv_location))))

    def convert_row(self, row: RowType) -> FormatResponse:
        post_id, uploader_id, created_at, md5, source, rating, image_width, image_height, tag_string, locked_tags, fav_count, file_ext, parent_id, change_seq, approver_id, file_size, comment_count, description, duration, updated_at, is_deleted, is_pending, is_flagged, score, up_score, down_score, is_rating_locked, is_status_locked, is_note_locked = row
        file_url = f"https://static1.e621.net/data/{md5[0:2]}/{md5[2:4]}/{md5}.{file_ext}"
        # Create snapshot
        snapshot = SubmissionSnapshot(
            WEBSITE.website_id,
            post_id,
            CONTRIBUTOR,
            DATA_DATE,
            datetime_posted=dateutil.parser.parse(created_at),
            description=description,
            uploader_site_user_id=uploader_id,
            is_deleted=is_deleted == "t",
            files=[
                File(
                    None,
                    file_url=file_url,
                    hashes=[FileHash(MD5_HASH.algo_id, base64.b64decode(md5.encode('ascii')))],
                    file_size=file_size,
                    extra_data={
                        "width": int(image_width),
                        "height": int(image_height),
                        "ext": file_ext,  # TODO: really?
                        "duration": float(duration) if duration else None
                    },
                )
            ],
            unordered_keywords=tag_string.split(),
            extra_data={
                "sources": [s.strip() for s in source.split("\n")],
                "rating": rating,
                "locked_tags": locked_tags.split(),  # TODO: really?
                "fav_count": int(fav_count),
                "comment_count": int(comment_count),
                "parent_id": parent_id,  # TODO: really?
                "change_seq": change_seq,  # TODO: really?
                "approver_id": approver_id,  # TODO: really?
                "updated_datetime": parse_datetime(updated_at),
                "is_pending": is_pending == "t",  # TODO: really?
                "is_flagged": is_flagged == "t",  # TODO: really?
                "score": int(score),
                "up_score": int(up_score),  # TODO: really?
                "down_score": int(down_score),  # TODO: really?
                "is_rating_locked": is_rating_locked == "t",  # TODO: really?
                "is_status_locked": is_status_locked == "t",  # TODO: really?
                "is_note_locked": is_note_locked == "t",  # TODO: really?
            },
        )
        return FormatResponse([snapshot])

    def iterate_rows(self) -> Iterator[RowType]:
        with open(CSV_LOCATION, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)  # skip headers
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
    MD5_HASH.save(db_obj)

    ingestion_job = E621IngestJob()
    ingestion_job.ingest_data(db_obj)
