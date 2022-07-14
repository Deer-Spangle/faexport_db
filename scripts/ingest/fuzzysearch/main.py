import base64
import csv
import dataclasses
import datetime
import json
import string
from typing import Dict, Callable, Optional, List

import dateutil.parser
import psycopg2
import requests

from faexport_db.db import Database
from faexport_db.models.archive_contributor import ArchiveContributor
import tqdm

from faexport_db.models.file import FileHash, HashAlgo, File
from faexport_db.models.submission import SubmissionSnapshot
from faexport_db.models.user import UserSnapshot
from faexport_db.models.website import Website

FUZZYSEARCH_FILE = "./dump/fuzzysearch/fuzzysearch-dumps.csv"
DATA_DATE = datetime.datetime(2022, 6, 22, 0, 0, 0, 0, datetime.timezone.utc)
CONTRIBUTOR = ArchiveContributor("FuzzySearch data ingest")
SHA_HASH = HashAlgo("any", "sha256")
DHASH = HashAlgo("rust", "dhash")


@dataclasses.dataclass
class SiteConfig:
    website: Website
    ingest_artist: bool
    user_lookup: Callable[
        [str, str, ArchiveContributor, datetime.datetime], Optional[List[UserSnapshot]]
    ] = lambda display_name, sub_id, contributor, scan_datetime: None


FA_ID = "fa"
WEASYL_ID = "weasyl"


class WeasylLookup:
    username_chars = string.ascii_letters + string.digits

    def __init__(self):
        self.cache: Dict[str, List[UserSnapshot]] = {}

    def get_user_snapshots(
            self,
            display_name: str,
            submission_id: str,
            contributor: ArchiveContributor,
            scan_datetime: datetime.datetime
    ) -> List[UserSnapshot]:
        if display_name in self.cache:
            return self.cache[display_name]
        username_guess = "".join([char for char in display_name.lower() if char in self.username_chars])
        # Fetch weasyl user response
        try:
            resp = requests.get(f"https://weasyl.com/api/users/{username_guess}/view").json()
            site_username = resp["login_name"]
            site_display_name = resp["username"]
            if display_name == site_display_name:
                snapshots = [
                    UserSnapshot(
                        WEASYL_ID,
                        site_username,
                        contributor,
                        scan_datetime,
                        display_name=display_name
                    ),
                    UserSnapshot(
                        WEASYL_ID,
                        site_username,
                        contributor,
                        datetime.datetime.now(datetime.timezone.utc),
                        display_name=site_display_name,
                        extra_data={
                            "catchphrase": resp["cachephrase"],
                            "profile_text": resp["profile_text"],
                            "stream_text": resp["stream_text"],  # TODO: really?
                            "show_favorites_bar": resp["show_favorites_bar"],  # TODO: really?
                            "show_favorites_tab": resp["show_favorites_tab"],  # TODO: really?
                            "banned": resp["banned"],
                            "suspended": resp["suspended"],
                            "streaming_status": resp["streaming_status"],  # TODO: really?
                            "created_at": dateutil.parser.parse(resp["created_at"]),
                            "media": resp["media"],  # TODO: Pull apart structure?
                            "full_name": resp["full_name"],
                            "folders": resp["folders"],
                            "commission_info": resp["commission_info"],
                            "recent_type": resp["recent_type"],  # TODO: what?
                            "featured_submission": resp["featured_submission"],  # TODO: what structure? site ID?
                            "statistics": resp["statistics"],  # TODO: pull apart?
                        }
                    )
                ]
                self.cache[display_name] = snapshots
                return snapshots
        except Exception:
            pass
        resp = requests.get(f"https://weasyl.com/api/submission/{submission_id}/view").json()
        site_username = resp["owner_login"]
        site_display_name = resp["owner"]
        snapshots = [
            UserSnapshot(
                WEASYL_ID,
                site_username,
                contributor,
                scan_datetime,
                display_name=display_name
            ),
            UserSnapshot(
                WEASYL_ID,
                site_username,
                contributor,
                datetime.datetime.now(datetime.timezone.utc),
                display_name=site_display_name,
                extra_data={
                    "media": resp["owner_media"]  # TODO: Pull apart structure?
                }
            )
        ]
        self.cache[display_name] = snapshots
        self.cache[site_display_name] = snapshots
        return snapshots


WEASYL_LOOKUP = WeasylLookup()
SITE_CONFIG = {
    "furaffinity": SiteConfig(
        Website(FA_ID, "Fur Affinity", "https://furaffinity.net"),
        True,
        lambda display_name, sub_id, contributor, scan_datetime: [
            UserSnapshot(
                FA_ID,
                display_name.replace("_", ""),
                contributor,
                scan_datetime,
                display_name=display_name
            )
        ]
    ),
    "weasyl": SiteConfig(
        Website(WEASYL_ID, "Weasyl", "https://weasyl.com"),
        True,
        lambda display_name, sub_id, contributor, scan_datetime: WEASYL_LOOKUP.get_user_snapshots(
            display_name, sub_id, contributor, scan_datetime
        )
    ),
    "e621": SiteConfig(
        Website("e621", "e621", "https://e621.net"),
        False
    )
}


def import_row(row: Dict[str, str], db: Database) -> SubmissionSnapshot:
    site, submission_id, artists, hash_value, posted_at, updated_at, sha256, deleted, content_url = row.values()
    site_config = SITE_CONFIG[site]
    website_id = site_config.website.website_id
    scan_date = csv_earliest_date()
    if updated_at:
        scan_date = dateutil.parser.parse(updated_at)

    uploader_username = None
    if site_config.ingest_artist:
        user_snapshots = site_config.user_lookup(artists, submission_id, CONTRIBUTOR, scan_date)
        for user_snapshot in user_snapshots:
            user_snapshot.save(db)
            uploader_username = user_snapshot.site_user_id

    dhash_bytes = int(hash_value).to_bytes(8, byteorder='big')
    hashes = [
        FileHash(DHASH.algo_id, dhash_bytes)
    ]
    if sha256:
        sha_bytes = base64.b64decode(sha256.encode('ascii'))
        hashes.append(FileHash(SHA_HASH.algo_id, sha_bytes))

    update = SubmissionSnapshot(
        website_id,
        submission_id,
        CONTRIBUTOR,
        ingest_date,

        uploader_site_user_id=uploader_username,
        is_deleted=(deleted == "true"),
        datetime_posted=dateutil.parser.parse(posted_at),
        files=[
            File(
                None,
                file_url=content_url,
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


def csv_earliest_date() -> datetime.dateime:
    return datetime.datetime(2021, 4, 25, 18, 57, 56, 966994, datetime.timezone.utc)
    earliest = "zzz"
    with open(FUZZYSEARCH_FILE, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        for line in reader:
            if line[5]:
                earliest = min(earliest, line[5])
    return dateutil.parser.parse(earliest)


def check_csv() -> None:
    fa_allowed_chars = set(string.ascii_lowercase + string.digits + "-_.~[]^`")
    weasyl_allowed_chars = set(string.ascii_letters + string.digits + " -_.'@&!~|`")
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
    # Create websites from SITE_MAP
    for site_conf in SITE_CONFIG.values():
        site_conf.website.save(db_obj)
    # Save contributor
    CONTRIBUTOR.save(db_obj)
    # Save SHA_HASH and DHASH
    SHA_HASH.save(db_obj)
    DHASH.save(db_obj)
    # Import data
    check_csv()
    # ingest_csv(db_obj)
