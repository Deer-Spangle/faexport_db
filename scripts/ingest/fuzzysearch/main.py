import base64
import csv
import dataclasses
import datetime
import json
import string
import struct
from collections import Counter
from pathlib import Path
from typing import Dict, Optional, Iterator

import dateutil.parser
import psycopg2

from faexport_db.db import Database
from faexport_db.ingest_formats.base import FormatResponse
from faexport_db.models.archive_contributor import ArchiveContributor
import tqdm

from faexport_db.models.file import FileHash, HashAlgo, File
from faexport_db.models.submission import SubmissionSnapshot
from faexport_db.models.website import Website
from scripts.ingest.fuzzysearch.user_lookup import WeasylLookup, UserLookup, FALookup, WEASYL_ID, FA_ID
from scripts.ingest.ingestion_job import IngestionJob, RowType, cache_in_file, csv_count_rows

FUZZYSEARCH_FILE = "./dump/fuzzysearch/fuzzysearch-dump-20220620.csv"
DATA_DATE = datetime.datetime(2022, 6, 22, 0, 0, 0, 0, datetime.timezone.utc)
CONTRIBUTOR = ArchiveContributor("FuzzySearch data ingest")
SHA_HASH = HashAlgo("any", "sha256")
DHASH = HashAlgo("rust", "dhash")


@dataclasses.dataclass
class SiteConfig:
    website: Website
    ingest_artist: bool
    user_lookup: UserLookup = None


fa_allowed_chars = set(string.ascii_letters + string.digits + "-_.~[]^`")
weasyl_allowed_chars = set(string.printable)


class FuzzysearchIngestionJob(IngestionJob):

    def __init__(self, site_configs: Dict[str, SiteConfig], *, skip_rows: int = 0):
        super().__init__(skip_rows=skip_rows)
        self.site_configs = site_configs
        self.csv_location = FUZZYSEARCH_FILE
        self.row_count_file = Path(__file__) / "cache_row_count.txt"
        self.earliest_date_file = Path(__file__) / "cache_earliest_date.txt"
        self._earliest_date = None
        self._earliest_date = self.earliest_date()

    def row_count(self) -> Optional[int]:
        return int(cache_in_file(self.row_count_file, lambda: str(csv_count_rows(self.csv_location))))

    def _csv_earliest_date(self) -> datetime.datetime:
        earliest = "zzz"
        with open(self.csv_location, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for line in tqdm.tqdm(reader, desc="Finding earliest date"):
                if line[5]:
                    earliest = min(earliest, line[5])
        return dateutil.parser.parse(earliest)

    def earliest_date(self) -> datetime.datetime:
        if self._earliest_date is not None:
            return self._earliest_date
        earliest_date = dateutil.parser.parse(cache_in_file(
            self.earliest_date_file,
            lambda: self._csv_earliest_date().isoformat()
        ))
        self._earliest_date = earliest_date
        return earliest_date

    def convert_row(self, row: Dict[str, str]) -> FormatResponse:
        site, submission_id, artists, hash_value, posted_at, updated_at, sha256, deleted, content_url = row.values()
        if hash_value == "":
            return FormatResponse()
        site_config = self.site_configs[site]
        website_id = site_config.website.website_id
        scan_date = self.earliest_date()
        if updated_at:
            scan_date = dateutil.parser.parse(updated_at)

        uploader_username = None
        user_snapshots = []
        if site_config.ingest_artist and site_config.user_lookup is not None:
            uploader_username, user_snapshots = site_config.user_lookup.lookup_user(
                artists,
                submission_id,
                CONTRIBUTOR,
                scan_date
            )

        posted_date = None
        if posted_at:
            posted_date = dateutil.parser.parse(posted_at)

        dhash_bytes = struct.pack(">q", int(hash_value))
        hashes = [
            FileHash(DHASH.algo_id, dhash_bytes)
        ]
        if sha256:
            sha_bytes = base64.b64decode(sha256.encode('ascii'))
            hashes.append(FileHash(SHA_HASH.algo_id, sha_bytes))
        file_url = None
        if content_url:
            file_url = content_url

        update = SubmissionSnapshot(
            website_id,
            submission_id,
            CONTRIBUTOR,
            scan_date,

            uploader_site_user_id=uploader_username,
            is_deleted=(deleted == "true"),
            datetime_posted=posted_date,
            files=[
                File(
                    None,
                    file_url=file_url,
                    hashes=hashes,
                )
            ]
        )
        return FormatResponse([update], user_snapshots)

    def validate_row(self, row: RowType) -> None:
        site, submission_id, artists, hash_value, posted_at, updated_at, sha256, deleted, content_url = row.values()
        if hash_value == "":  # About 5 million rows without any values, skip those
            return
        assert site in self.site_configs
        assert submission_id
        if site == "e621":
            pass
        elif site == "weasyl":
            assert set(artists).issubset(weasyl_allowed_chars)
        elif site == "furaffinity":
            assert set(artists).issubset(fa_allowed_chars)
        assert struct.pack(">q", int(hash_value))
        if posted_at:
            assert dateutil.parser.parse(posted_at)
        if updated_at:
            assert dateutil.parser.parse(updated_at)
        if sha256:
            assert base64.b64decode(sha256.encode('ascii'))
        assert deleted in ["true", "false"]
        # assert content_url  # Can be empty

    def investigate_data(self) -> None:
        weasyl_usernames = set()
        row_count = self.row_count()
        print(f"CSV has {row_count} rows")
        earliest_date = "zzz"
        site_list = []
        with open(FUZZYSEARCH_FILE, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in tqdm.tqdm(reader, total=row_count):
                drow = dict(row)
                if drow["updated_at"]:
                    updated_at = drow["updated_at"]
                    earliest_date = min(earliest_date, updated_at)
                site = drow["site"]
                site_list.append(site)
                if site == "e621":
                    continue
                username = drow["artists"]
                if site == "weasyl":
                    if not set(username).issubset(weasyl_allowed_chars):
                        weasyl_usernames.add(username)
                        print(f"Found an off weasyl username character: {username}")
                if site == "furaffinity":
                    if not set(username.lower()).issubset(fa_allowed_chars):
                        print(f"Found an odd FA username character: {username}")
        print(f"Earliest date: {dateutil.parser.parse(earliest_date)}")
        site_counter = Counter(site_list)
        sites = set(site_counter.keys())
        print(f"Site list: {sites}")
        print("Site counter: " + ", ".join(f"{site}: {count}" for site, count in site_counter.most_common()))
        print(f"Confusing weasyl display names: {weasyl_usernames}")

    def iterate_rows(self) -> Iterator[Dict]:
        with open(FUZZYSEARCH_FILE, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                yield dict(row)


if __name__ == "__main__":
    # Connect to database
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    # Set up site configuration
    site_confs = {
        "furaffinity": SiteConfig(
            Website(FA_ID, "Fur Affinity", "https://furaffinity.net"),
            True,
            FALookup(db_obj)
        ),
        "e621": SiteConfig(
            Website("e621", "e621", "https://e621.net"),
            False
        ),
        "weasyl": SiteConfig(
            Website(WEASYL_ID, "Weasyl", "https://weasyl.com"),
            True,
            WeasylLookup(db_obj, config.get("weasyl_api_key"))
        )
    }
    # Create websites from SITE_MAP
    for site_conf in site_confs.values():
        site_conf.website.save(db_obj)
    # Save contributor and hash algorithms
    CONTRIBUTOR.save(db_obj)
    SHA_HASH.save(db_obj)
    DHASH.save(db_obj)
    # Import data
    ingestion_job = FuzzysearchIngestionJob(site_confs)
    ingestion_job.process(db_obj)
