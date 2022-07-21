import base64
import csv
import dataclasses
import datetime
import json
import string
import struct
from typing import Dict, Optional

import dateutil.parser
import psycopg2

from faexport_db.db import Database
from faexport_db.models.archive_contributor import ArchiveContributor
import tqdm

from faexport_db.models.file import FileHash, HashAlgo, File
from faexport_db.models.submission import SubmissionSnapshot
from faexport_db.models.website import Website
from scripts.ingest.fuzzysearch.user_lookup import WeasylLookup, UserLookup, FALookup

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


FA_ID = "fa"
WEASYL_ID = "weasyl"

SITE_CONFIG = {
    "furaffinity": SiteConfig(
        Website(FA_ID, "Fur Affinity", "https://furaffinity.net"),
        True,
        FALookup()
    ),
    "e621": SiteConfig(
        Website("e621", "e621", "https://e621.net"),
        False
    )
}


def import_row(row: Dict[str, str], db: Database) -> Optional[SubmissionSnapshot]:
    site, submission_id, artists, hash_value, posted_at, updated_at, sha256, deleted, content_url = row.values()
    if hash_value == "":
        return None
    site_config = SITE_CONFIG[site]
    website_id = site_config.website.website_id
    scan_date = csv_earliest_date()
    if updated_at:
        scan_date = dateutil.parser.parse(updated_at)

    uploader_username = None
    if site_config.ingest_artist and site_config.user_lookup is not None:
        user_snapshots = site_config.user_lookup.get_user_snapshots(artists, submission_id, CONTRIBUTOR, scan_date)
        for user_snapshot in user_snapshots:
            user_snapshot.save(db)
            uploader_username = user_snapshot.site_user_id

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
    update.save(db)
    return update


def csv_row_count() -> int:
    return 40558648
    with open(FUZZYSEARCH_FILE, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        return sum(1 for _ in tqdm.tqdm(reader))


def csv_earliest_date() -> datetime.datetime:
    return datetime.datetime(2021, 4, 25, 18, 57, 56, 966994, datetime.timezone.utc)
    earliest = "zzz"
    with open(FUZZYSEARCH_FILE, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        for line in reader:
            if line[5]:
                earliest = min(earliest, line[5])
    return dateutil.parser.parse(earliest)


fa_allowed_chars = set(string.ascii_letters + string.digits + "-_.~[]^`")
weasyl_allowed_chars = set(string.printable)


def validate_row(row: Dict) -> None:
    site, submission_id, artists, hash_value, posted_at, updated_at, sha256, deleted, content_url = row.values()
    if hash_value == "":  # About 5 million rows without any values, skip those
        return
    assert site in SITE_CONFIG
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


def validate_csv() -> None:
    row_count = csv_row_count()
    with open(FUZZYSEARCH_FILE, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in tqdm.tqdm(reader, total=row_count):
            validate_row(row)


def investigate_csv() -> None:
    sites = set()
    weasyl_usernames = set()
    row_count = csv_row_count()
    print(f"CSV has {row_count} rows")
    earliest_date = datetime.datetime.now(datetime.timezone.utc)
    with open(FUZZYSEARCH_FILE, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in tqdm.tqdm(reader, total=row_count):
            drow = dict(row)
            if drow["updated_at"]:
                updated_at = dateutil.parser.parse(row["updated_at"])
                earliest_date = min(earliest_date, updated_at)
            site = drow["site"]
            sites.add(site)
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
    print(f"Earliest date: {earliest_date}")
    print(f"Site list: {sites}")
    print(f"Confusing weasyl display names: {weasyl_usernames}")


def ingest_csv(db: Database) -> None:
    row_count = csv_row_count()
    with open(FUZZYSEARCH_FILE, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in tqdm.tqdm(reader, total=row_count):
            import_row(dict(row), db)


if __name__ == "__main__":
    # Connect to database
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    # Add weasyl to site configs
    SITE_CONFIG[WEASYL_ID] = SiteConfig(
        Website(WEASYL_ID, "Weasyl", "https://weasyl.com"),
        True,
        WeasylLookup(config.get("weasyl_api_key"))
    )
    # Create websites from SITE_MAP
    for site_conf in SITE_CONFIG.values():
        site_conf.website.save(db_obj)
    # Save contributor
    CONTRIBUTOR.save(db_obj)
    # Save SHA_HASH and DHASH
    SHA_HASH.save(db_obj)
    DHASH.save(db_obj)
    # Import data
    # investigate_csv()
    validate_csv()
    # ingest_csv(db_obj)
