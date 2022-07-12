import base64
import csv
import dataclasses
import datetime
import string
from typing import Dict, Callable

import dateutil.parser
from faexport_db.models.archive_contributor import ArchiveContributor
import tqdm

from faexport_db.models.file import FileListUpdate, FileUpdate, FileHashListUpdate, FileHashUpdate
from faexport_db.models.submission import Submission, SubmissionSnapshot, SubmissionUpdate
from faexport_db.models.user import UserSnapshot, UserUpdate
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
    username_transform: Callable[[str], str] = lambda x: x


SITE_CONFIG = {
    "furaffinity": SiteConfig(
        Website("fa", "Fur Affinity", "https://furaffinity.net"),
        True,
        lambda username: username.replace("_", "").lower()
    ),
    "weasyl": SiteConfig(
        Website("weasyl", "Weasyl", "https://weasyl.com"),
        True,
        # TODO: Need to strip non-alpahnumeric to make url, then lookup real username
    ),
    "e621": SiteConfig(
        Website("e621", "e621", "https://e621.net"),
        False
    )
}


def import_row(row: Dict[str, str]) -> Submission:
    site, submission_id, artists, hash_value, posted_at, updated_at, sha256, deleted, content_url = row
    site_config = SITE_CONFIG[site]
    website_id = site_config.website.website_id
    uploader_username = None
    if site_config.ingest_artist:
        # TODO: Weasyl username lookup won't be this easy
        uploader = UserSnapshot(
            website_id,
            site_config.username_transform(artists),
            display_name=artists
        )
        uploader.save(db)
        uploader_username = uploader.site_user_id
    ingest_date = DATA_DATE  # TODO: That doesn't seem right, it should be the lowest value, not newest
    if updated_at:
        ingest_date = dateutil.parser.parse(updated_at)

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


fa_allowed_chars = set(string.ascii_lowercase + string.digits + "-_.~[]^`")
weasyl_allowed_chars = set(string.ascii_letters + string.digits + " -_.'@&!~|`")
weasyl_transform = lambda name: "".join([char for char in name if char in string.ascii_letters + string.digits])
# TODO: Oh no. Weasyl usernames don't always do this. "Mr.Pink" -> "pinkpalooka"
# Need to do an additional lookup, fetching the user page and stuff to get the real username.
# Sometimes, the username won't exist anymore. In that case, we need to lookup the submission



def csv_row_count() -> int:
    return 40558648
    with open(FUZZYSEARCH_FILE, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        return sum(1 for _ in tqdm.tqdm(reader))


def ingest_csv(db: Database) -> None:
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
    ingest_csv(db_obj)
