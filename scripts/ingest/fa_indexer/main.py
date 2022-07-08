import glob
import json
import time
import datetime
import multiprocessing
import multiprocessing.pool
from multiprocessing import Queue, Process
from pathlib import Path
from queue import Empty
from typing import Dict

import psycopg2
import dateutil.parser
import tqdm

from faexport_db.db import Database
from faexport_db.models.file import FileUpdate, FileListUpdate
from faexport_db.models.submission import SubmissionUpdate, Submission
from faexport_db.models.user import UserUpdate
from faexport_db.models.website import Website

DATA_DIR = "./dump/fa-indexer/"
SITE_ID = "fa"
DATA_DATE = datetime.datetime(2019, 12, 4, 0, 0, 0, tzinfo=datetime.timezone.utc)

DONE_SIGNAL = "DONE"


def import_submission_data(db: Database, submission_data: Dict) -> Submission:
    if submission_data["id"] == 641877:
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
        )])
    )
    submission = sub_update.save(db)
    return submission


class Processor:
    def __init__(self, dsn: str, queue: Queue, resp_queue: Queue, num: int):
        self.dsn = dsn
        self.queue = queue
        self.resp_queue = resp_queue
        self.num = num

    def process_entries(
            self,
    ) -> None:
        conn = psycopg2.connect(self.dsn)
        db = Database(conn)
        while True:
            try:
                submission_data = self.queue.get(False)
            except Empty:
                time.sleep(0.1)
                continue
            # print(f"Importing submission: {submission_data['id']} in worker {self.num}")
            if submission_data == DONE_SIGNAL:
                break
            submission = import_submission_data(db, submission_data)
            self.resp_queue.put(submission.site_submission_id)


def scan_directory(dsn: str, dir_path: str) -> None:
    num_processes = 10
    queue = multiprocessing.Queue()
    resp_queue = multiprocessing.Queue()
    processors = [Processor(dsn, queue, resp_queue, num) for num in range(num_processes)]
    processes = [Process(target=processor.process_entries) for processor in processors]
    for process in processes:
        process.start()
    todo_ids = set()

    for file in tqdm.tqdm(glob.glob(dir_path + "/**/*.json", recursive=True)):
        print(f"Opening file {file}")
        with open(file, "r") as f:
            data = json.load(f)
        for submission in data.values():
            if submission is None:
                continue
            todo_ids.add(str(submission["id"]))
            queue.put(submission)
        while len(todo_ids) > 0:
            # print(f"{len(todo_ids)} submissions to do")
            resp_id = resp_queue.get()
            todo_ids.remove(resp_id)
    for _ in range(num_processes + 1):
        queue.put(DONE_SIGNAL)
    for process in processes:
        process.join()


def setup_initial_data(db: Database) -> None:
    website = Website(SITE_ID, "Fur Affinity", "https://furaffinity.net")
    website.save(db)


if __name__ == "__main__":
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    setup_initial_data(db_obj)
    scan_directory(db_dsn, DATA_DIR)
