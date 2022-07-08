import base64
import csv
import dataclasses
import datetime
import string
from typing import Dict, Callable

import dateutil.parser
import tqdm

from faexport_db.db import UNSET
from faexport_db.models.file import FileListUpdate, FileUpdate, FileHashListUpdate, FileHashUpdate
from faexport_db.models.submission import Submission, SubmissionUpdate
from faexport_db.models.user import UserUpdate
from faexport_db.models.website import Website

FUZZYSEARCH_FILE = "./dump/fuzzysearch/fuzzysearch-dumps.csv"
DATA_DATE = datetime.datetime(2022, 6, 22, 0, 0, 0, 0, datetime.timezone.utc)


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
    uploader = UNSET
    if site_config.ingest_artist:
        # TODO: check for other odd characters
        uploader = UserUpdate(website_id, site_config.username_transform(artists), display_name=artists)
    ingest_date = DATA_DATE
    if updated_at:
        ingest_date = dateutil.parser.parse(updated_at)

    dhash_bytes = int(hash_value).to_bytes(8, byteorder='big')
    hashes = [
        FileHashUpdate("rust:dhash", dhash_bytes)
    ]
    if sha256:
        sha_bytes = base64.b64decode(sha256.encode('ascii'))
        hashes.append(FileHashUpdate("sha256", sha_bytes))

    update = SubmissionUpdate(
        website_id,
        submission_id,
        update_time=ingest_date,
        is_deleted=(deleted == "true"),
        uploader_update=uploader,
        datetime_posted=dateutil.parser.parse(posted_at),
        files=FileListUpdate([
            FileUpdate(
                content_url,
                add_hashes=FileHashListUpdate(hashes)
            )
        ])
    )
    print(row)
    print(site)


fa_allowed_chars = set(string.ascii_lowercase + string.digits + "-_.~[]^`")
weasyl_allowed_chars = set(string.ascii_letters + string.digits + " -_.'@&!~|`")
weasyl_transform = lambda name: "".join([char for char in name if char in string.ascii_letters + string.digits])
# TODO: Oh no. Weasyl usernames don't do this. "Mr.Pink" -> "pinkpalooka"
# Need to do an additional lookup, fetching the user page and stuff to get the real username


def csv_row_count() -> int:
    return 40558648
    with open(FUZZYSEARCH_FILE, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        return sum(1 for _ in tqdm.tqdm(reader))


def ingest_csv() -> None:
    sites = set()
    weasyl_usernames = set()
    row_count = csv_row_count()
    print(f"CSV has {row_count} rows")
    with open(FUZZYSEARCH_FILE, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in tqdm.tqdm(reader, total=row_count):
            drow = dict(row)
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
    print(sites)
    print(weasyl_usernames)


if __name__ == "__main__":
    # TODO: connect to database
    # TODO: Create websites from SITE_MAP
    ingest_csv()
