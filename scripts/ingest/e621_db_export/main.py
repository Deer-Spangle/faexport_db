import base64
import datetime
import json
import csv
from typing import Optional, Dict

import dateutil.parser
import psycopg2
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.file import HashAlgo, File, FileHash
from faexport_db.models.submission import SubmissionSnapshot
import tqdm

from faexport_db.db import Database
from faexport_db.models.website import Website

CSV_LOCATION = "./dump/e621_db_export/posts-2022-07-13.csv"
WEBSITE = Website("e6", "e621", "https://e621.net")
DATA_DATE = datetime.datetime(2022, 7, 13, 0, 0, 0, tzinfo=datetime.timezone.utc)
CONTRIBUTOR = ArchiveContributor("e621 db_export")
MD5_HASH = HashAlgo("any", "md5")


def import_row(row: Dict[str, str], db: Database) -> Optional[SubmissionSnapshot]:
    post_id, uploader_id, created_at, md5, source, rating, image_width, image_height, tag_string, locked_tags, fav_count, file_ext, parent_id, change_seq, approver_id, file_size, comment_count, description, duration, updated_at, is_deleted, is_pending, is_flagged, score, up_score, down_score, is_rating_locked, is_status_locked, is_note_locked = row.values()
    # TODO: add some validation of the data?
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
                hashes=[FileHash(MD5_HASH.algo_id, base64.b64decode(md5.encode('ascii')))],
                file_size=file_size,
                extra_data={
                    "width": image_width,
                    "height": image_height,
                    "ext": file_ext,  # TODO: really?
                    "duration": duration,
                }
            )
        ],
        unordered_keywords=tag_string.split(),
        extra_data={
            "sources": source.split("\n"),
            "rating": rating,
            "locked_tags": locked_tags.split(),  # TODO: really?
            "fav_count": fav_count,
            "comment_count": comment_count,
            "parent_id": parent_id,  # TODO: really?
            "change_seq": change_seq,  # TODO: really?
            "approver_id": approver_id,  # TODO: really?
            "updated_datetime": dateutil.parser.parse(updated_at),
            "is_pending": is_pending == "t",  # TODO: really?
            "is_flagged": is_flagged == "t",  # TODO: really?
            "score": score,
            "up_score": up_score,  # TODO: really?
            "down_score": down_score,  # TODO: really?
            "is_rating_locked": is_rating_locked == "t",  # TODO: really?
            "is_status_locked": is_status_locked == "t",  # TODO: really?
            "is_note_locked": is_note_locked == "t",  # TODO: really?
        }
    )
    return snapshot


def csv_row_count() -> int:
    with open(CSV_LOCATION, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        return sum(1 for _ in tqdm.tqdm(reader))


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
    WEBSITE.save(db_obj)
    CONTRIBUTOR.save(db_obj)
    MD5_HASH.save(db_obj)

    ingest_csv(db_obj)
