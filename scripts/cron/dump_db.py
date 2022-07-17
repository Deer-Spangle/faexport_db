import json
import os
import sys

import psycopg2
import tqdm

from faexport_db.db import Database, CustomJSONEncoder
from faexport_db.models.submission import Submission, SubmissionSnapshot
from faexport_db.models.website import Website


def create_snapshot_dump(db: Database, filename: str) -> None:
    with open(filename, "w") as f:
        websites = Website.list_all(db)
        row = 0
        cap = 10
        for website in websites:
            for snapshot in tqdm.tqdm(SubmissionSnapshot.list_all(db, website.website_id), total=cap):
                # TODO: very slow, probably needs a submission_id index on files and keywords?
                data = json.dumps(snapshot.to_web_json(), cls=CustomJSONEncoder) + "\n"
                f.write(data)
                row += 1
                if row > cap:
                    return
    pass  # TODO


def create_submission_dump(db: Database, filename: str) -> None:
    with open(filename, "w") as f:
        websites = Website.list_all(db)
        row = 0
        cap = 100
        for website in websites:
            for site_sub_id in tqdm.tqdm(Submission.list_unique_site_ids(db, website.website_id), total=cap):
                sub = Submission.from_database(db, website.website_id, site_sub_id)
                data = json.dumps(sub.to_web_json(), cls=CustomJSONEncoder) + "\n"
                f.write(data)
                row += 1
                if row > cap:
                    return
    pass  # TODO


if __name__ == "__main__":
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    os.makedirs("export", exist_ok=True)
    create_snapshot_dump(db_obj, "export/snapshots.csv")
    sys.exit(0)  # TODO: Probably not worth trying to optimise this until after bulk ingest is done
    create_submission_dump(db_obj, "export/submissions.csv")
