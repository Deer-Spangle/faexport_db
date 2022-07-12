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
    username_lookup: Callable[[str, str, Database], str] = lambda username, sub_id, db: username


SITE_CONFIG = {
    "furaffinity": SiteConfig(
        Website("fa", "Fur Affinity", "https://furaffinity.net"),
        True,
        lambda username, _, __: username.replace("_", "").lower()
    ),
    "weasyl": SiteConfig(
        Website("weasyl", "Weasyl", "https://weasyl.com"),
        True,
        lambda username, sub_id, db: get_weasyl_username(username, sub_id, db)
    ),
    "e621": SiteConfig(
        Website("e621", "e621", "https://e621.net"),
        False
    )
}


WEASYL_LOOKUP = {}
def get_weasyl_username(display_name: str, submission_id: str, db: Database) -> str:
    if display_name in WEASYL_LOOKUP:
        return WEASYL_LOOKUP[display_name]
    username_guess = "".join([char for char in display_name.lower() if char in string.ascii_letters + string.digits])
    # Fetch weasyl user page
    try:
        # Check display name on guessed user page
        resp = requests.get(f"https://weasyl.com/~{username_guess}")
        body = resp.content
        site_display_name = body.split("<title>")[1].split("’s profile — Weasyl</title>")[0]
        # If display name is correct on page, return username from page
        if display_name == site_display_name:
            site_username = body.split("<link rel=\"canonical\" href=\"https://www.weasyl.com/~")[1].split("\" />")[0]
            WEASYL_LOOKUP[display_name] = site_username
            return site_username
    except Exception:
        pass
    # Else, get submission page, and get username and display name from there
    submission_url = f"https://weasyl.com/~username/submissions/{submission_id}/"
    resp = requests.get(submission_url)
    body = resp.content
    site_user_tag = body.split("<div id=\"db-user\">")[1].split("<a class=\"username\" href=\"/~")[1].split("</a>")[0]
    site_username, site_display_name = site_user_tag.split("\">")
    WEASYL_LOOKUP[display_name] = site_username
    WEASYL_LOOKUP[site_display_name] = site_username
    user_snapshot = UserSnapshot(
        SITE_CONFIG["weasyl"].website.website_id,
        site_username,
        CONTRIBUTOR,
        display_name=site_display_name,
    )
    user_snapshot.save(db)
    return site_username


def import_row(row: Dict[str, str], db: Database) -> Submission:
    site, submission_id, artists, hash_value, posted_at, updated_at, sha256, deleted, content_url = row
    site_config = SITE_CONFIG[site]
    website_id = site_config.website.website_id
    ingest_date = DATA_DATE  # TODO: That doesn't seem right, it should be the lowest value, not newest
    if updated_at:
        ingest_date = dateutil.parser.parse(updated_at)

    uploader_username = None
    if site_config.ingest_artist:
        username = site_config.username_lookup(artists, submission_id, db)
        uploader = UserSnapshot(
            website_id,
            username,
            CONTRIBUTOR,
            ingest_date,
            display_name=artists
        )
        uploader.save(db)
        uploader_username = uploader.site_user_id

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


def check_csv(db: Database) -> None:
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
    check_csv(db_obj)
    # ingest_csv(db_obj)
