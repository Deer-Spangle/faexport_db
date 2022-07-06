import glob
import json
import datetime
from pathlib import Path
from typing import Dict

import psycopg2
import dateutil.parser
import tqdm

from faexport_db.db import Database
from faexport_db.models.file import FileUpdate, FileHashUpdate, FileListUpdate, FileHashListUpdate
from faexport_db.models.submission import SubmissionUpdate
from faexport_db.models.user import UserUpdate
from faexport_db.models.website import Website

DATA_DIR = "./fa-indexer/"
SITE_ID = "fa"
DATA_DATE = datetime.datetime(2019, 12, 4, 0, 0, 0, tzinfo=datetime.timezone.utc)


def import_submission_data(db: Database, submission_data: Dict) -> None:
    if submission_data["id"] == "641877":
        submission_data["description"] = submission_data["description"].replace("\0", "/0")
    sub_update = SubmissionUpdate(
        SITE_ID,
        str(submission_data["id"]),
        DATA_DATE,
        False,
        uploader_update=UserUpdate(
            SITE_ID, submission_data["username"]
        ),
        title=submission_data["title"],
        description=submission_data["description"],
        datetime_posted=dateutil.parser.parse(submission_data["date"]),
        add_extra_data={"rating": submission_data["rating"]},
        ordered_keywords=submission_data["keywords"],
        files=FileListUpdate([FileUpdate(
            submission_data["filename"],
            add_hashes=FileHashListUpdate([FileHashUpdate("test", "abc123")])
        )])
    )
    submission = sub_update.save(db)


def scan_directory(db: Database, dir_path: str) -> None:
    for file in tqdm.tqdm(glob.glob(dir_path + "/**/*.json", recursive=True)):
        with open(file, "r") as f:
            data = json.load(f)
        for sub_id, submission in data.items():
            if submission is None:
                continue
            # if int(sub_id) < 641877:
            #     continue
            # TODO: find timezone?
            tqdm.tqdm.write(f"Importing submission: {sub_id}")
            import_submission_data(db, submission)
            return


def setup_initial_data(db: Database) -> None:
    website = Website(SITE_ID, "Fur Affinity", "https://furaffinity.net")
    website.save(db)


if __name__ == "__main__":
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, "r") as f:
        config = json.load(f)
    db_conn = psycopg2.connect(config["db_conn"])
    db_obj = Database(db_conn)
    setup_initial_data(db_obj)
    scan_directory(db_obj, DATA_DIR)
