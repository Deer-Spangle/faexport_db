import base64
import datetime
import json
import csv
import sys
from collections import Counter
from pathlib import Path
from typing import Optional, List, Iterator

import dateutil.parser
import psycopg2
import tqdm

from faexport_db.ingest_formats.base import FormatResponse
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.file import HashAlgo, File, FileHash
from faexport_db.models.submission import SubmissionSnapshot

from faexport_db.db import Database, parse_datetime
from faexport_db.models.website import Website
from scripts.ingest.ingestion_job import IngestionJob, RowType, cache_in_file, csv_count_rows

CSV_LOCATION = "./dump/e621_db_export/posts-2022-07-13.csv"
WEBSITE = Website("e621", "e621", "https://e621.net")
DATA_DATE = datetime.datetime(2022, 7, 13, 0, 0, 0, tzinfo=datetime.timezone.utc)
CONTRIBUTOR = ArchiveContributor("e621 db_export")
MD5_HASH = HashAlgo("any", "md5")


class E621IngestJob(IngestionJob):

    def __init__(self, *, skip_rows: int = 0):
        super().__init__(skip_rows=skip_rows)
        self.csv_location = CSV_LOCATION
        self.row_count_file = Path(__file__).parent / "cache_row_count.txt"

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

    def validate_row(self, row: List[str]) -> None:
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

    def iterate_rows(self) -> Iterator[RowType]:
        with open(CSV_LOCATION, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)  # skip headers
            for row in reader:
                yield row

    def investigate_data(self) -> None:
        twitter_usernames = []
        source_protocols = []
        source_domains = []
        raw_domains = []
        probably_wrong = set()
        for row in tqdm.tqdm(self.iterate_rows(), total=self.row_count()):
            post_id, uploader_id, created_at, md5, source, rating, image_width, image_height, tag_string, locked_tags, fav_count, file_ext, parent_id, change_seq, approver_id, file_size, comment_count, description, duration, updated_at, is_deleted, is_pending, is_flagged, score, up_score, down_score, is_rating_locked, is_status_locked, is_note_locked = row
            if not source.strip():
                continue
            sources = [s.strip() for s in source.split("\n")]
            for source_link in sources:
                if ", " in source_link:
                    probably_wrong.add(post_id)
                if "://" in source_link:
                    protocol, source_link = source_link.split("://", 1)
                    source_protocols.append(protocol)
                if source_link.startswith("www."):
                    source_link = source_link[4:]
                if "/" not in source_link:
                    raw_domains.append(source_link)
                else:
                    domain, source_path = source_link.split("/", 1)
                    source_domains.append(domain)
                    if domain == "twitter.com":
                        twitter_username = source_path
                        if "/" in twitter_username:
                            twitter_username, _ = twitter_username.split("/", 1)
                        if "?" in twitter_username:
                            twitter_username, _ = twitter_username.split("?", 1)
                        twitter_usernames.append(twitter_username)
        print(f"{len(probably_wrong)} posts have sources containing \", \" and are probably formatted wrong.")
        print(f"Source protocols: {Counter(source_protocols)}")
        print(f"There are {len(raw_domains)} sources which are just a raw domain")
        domain_counter = Counter(source_domains)
        print(f"There are {len(domain_counter)} unique domains mentioned")
        print(
            "Top five source domains: "
            + ", ".join(f"{domain}: {count}" for domain, count in domain_counter.most_common(5))
        )
        twitter_counter = Counter(twitter_usernames)
        print(f"There are {len(twitter_counter)} unique twitter usernames mentioned")
        print(
            "Top five twitter accounts: "
            + ", ".join(f"{acc}: {count}" for acc, count in twitter_counter.most_common(5))
        )
        with open("e621_dump_report.json", "w") as f:
            data = {
                "probably_wrong": list(probably_wrong),
                "source_protocols": {key: val for key, val in Counter(source_protocols).items()},
                "raw_domains": {key: val for key, val in Counter(raw_domains).items()},
                "source_domains": {key: val for key, val in domain_counter.items()},
                "twitter_usernames": {key: val for key, val in twitter_counter.items()}
            }
            json.dump(data, f)


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
    ingestion_job.process(db_obj)
