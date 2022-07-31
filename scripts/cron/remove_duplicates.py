import datetime
import json
import sys
import threading
import time
from contextlib import contextmanager
from typing import List

import psycopg2
import tqdm

from faexport_db.db import Database, chunks

DRY_RUN = False


class Timer:
    def __init__(self, msg: str):
        self.msg = msg
        self.start_time = None
        self.running = False
        self.decimals = 2
        self.total_time_taken = None

    def _format_delta(self, delta: datetime.timedelta) -> str:
        delta_str = str(delta)
        if "." not in delta_str:
            delta_str += ".0"
        result, decimals = delta_str.split(".", 1)
        if self.decimals > 0:
            decimals = decimals[:self.decimals].rjust(self.decimals, "0")
            result += "." + decimals
        return result

    def start(self) -> None:
        self.start_time = datetime.datetime.now()
        self.running = True
        while self.running:
            time_taken = (datetime.datetime.now() - self.start_time)
            sys.stderr.write("\r")
            sys.stderr.write(f"{self.msg}: [{self._format_delta(time_taken)}]")
            sys.stderr.flush()
            time.sleep(0.1)
        self.total_time_taken = datetime.datetime.now() - self.start_time
        sys.stdout.write(f"\r{self.msg} complete! Took {self._format_delta(self.total_time_taken)}\n")

    def stop(self):
        self.running = False


@contextmanager
def timer(msg: str) -> None:
    timer_obj = Timer(msg)
    timer_thread = threading.Thread(target=timer_obj.start)
    timer_thread.start()
    yield
    timer_obj.stop()
    timer_thread.join()


def delete_file_hashes(db: Database, hash_ids: List[int]) -> None:
    if DRY_RUN:
        return
    if not hash_ids:
        print("No file hashes to remove")
        return
    chunk_size = 1000
    chunk_count = (len(hash_ids) // chunk_size) + 1
    for hash_ids_chunk in tqdm.tqdm(chunks(hash_ids, chunk_size), "Removing hashes", total=chunk_count):
        print(f"Removing {len(hash_ids_chunk)} hashes")
        db.update("DELETE FROM submission_snapshot_file_hashes WHERE hash_id IN %s", (tuple(hash_ids_chunk),))


def delete_file_hashes_by_file(db: Database, file_ids: List[int]) -> None:
    if DRY_RUN:
        return
    if not file_ids:
        print("No file hashes to remove")
        return
    chunk_size = 1000
    chunk_count = (len(file_ids) // chunk_size) + 1
    for file_ids_chunk in tqdm.tqdm(chunks(file_ids, chunk_size), "Removing hashes", total=chunk_count):
        print(f"Removing {len(file_ids_chunk)} hashes")
        db.update("DELETE FROM submission_snapshot_file_hashes WHERE file_id IN %s", (tuple(file_ids_chunk),))


def remove_orphaned_file_hashes(db: Database) -> int:
    print("Scanning for orphaned file hashes")
    orphaned_hashes = db.select_iter(
        "SELECT hashes.hash_id "
        "FROM submission_snapshot_file_hashes hashes "
        "LEFT JOIN submission_snapshot_files files "
        "ON hashes.file_id = files.file_id "
        "WHERE files.file_id IS NULL",
        tuple()
    )
    remove_ids = []
    with timer("Scanning for orphaned file hashes"):
        for hash_row in orphaned_hashes:
            hash_id = hash_row[0]
            print(f"Removed orphaned file hash, ID: {hash_id}")
            remove_ids.append(hash_id)
    delete_file_hashes(db, remove_ids)
    return len(remove_ids)


def remove_duplicate_file_hashes(db: Database) -> int:
    print("Scanning for duplicate file hashes")
    duplicate_hashes = db.select_iter(
        "SELECT hash_id FROM ( "
        "SELECT hash_id, row_number() over w as rnum "
        "FROM submission_snapshot_file_hashes WINDOW w AS ( "
        "PARTITION BY file_id, algo_id ORDER BY hash_id DESC "
        ")) t WHERE t.rnum > 1",
        tuple()
    )
    remove_ids = []
    with timer("Scanning for duplicate file hashes"):
        for hash_row in duplicate_hashes:
            hash_id = hash_row[0]
            print(f"Removing duplicate file hash, ID: {hash_id}")
            remove_ids.append(hash_id)
    delete_file_hashes(db, remove_ids)
    return len(remove_ids)


def delete_files(db: Database, file_ids: List[int]) -> None:
    if DRY_RUN:
        return
    if not file_ids:
        print("No files to remove")
        return
    chunk_size = 1000
    chunk_count = (len(file_ids) // chunk_size) + 1
    for file_ids_chunk in tqdm.tqdm(chunks(file_ids, chunk_size), "Removing files", total=chunk_count):
        print(f"Removing {len(file_ids_chunk)} files")
        db.update("DELETE FROM submission_snapshot_file_hashes WHERE file_id IN %s", (tuple(file_ids_chunk),))
        db.update("DELETE FROM submission_snapshot_files WHERE file_id IN %s", (tuple(file_ids_chunk),))


def remove_orphaned_files(db: Database) -> int:
    print("Scanning for orphaned files")
    orphaned_files = db.select_iter(
        "SELECT files.file_id "
        "FROM submission_snapshot_files files "
        "LEFT JOIN submission_snapshots submissions "
        "ON files.submission_snapshot_id = submissions.submission_snapshot_id "
        "WHERE submissions.submission_snapshot_id IS NULL",
        tuple()
    )
    remove_ids = []
    with timer("Scanning for orphaned files"):
        for file_row in orphaned_files:
            file_id = file_row[0]
            print(f"Removed orphaned file, ID: {file_id}")
            remove_ids.append(file_id)
    delete_files(db, remove_ids)
    return len(remove_ids)


def remove_duplicate_files(db: Database) -> int:
    print("Scanning for duplicate files")
    duplicate_files = db.select_iter(
        "SELECT file_id FROM ( "
        "SELECT file_id, row_number() over w as rnum "
        "FROM submission_snapshot_files WINDOW w AS ( "
        "PARTITION BY submission_snapshot_id, site_file_id ORDER BY file_id DESC "
        ")) t WHERE t.rnum > 1",
        tuple()
    )
    remove_ids = []
    with timer("Scanning for duplicate files"):
        for file_row in duplicate_files:
            file_id = file_row[0]
            print(f"Removing duplicate file, ID: {file_id}")
            remove_ids.append(file_id)
    delete_files(db, remove_ids)
    return len(remove_ids)


def delete_keywords(db: Database, keyword_ids: List[int]) -> None:
    if DRY_RUN:
        return
    if not keyword_ids:
        print("No keywords to remove")
        return
    chunk_size = 1000
    chunk_count = (len(keyword_ids) // chunk_size) + 1
    for keyword_ids_chunk in tqdm.tqdm(chunks(keyword_ids, chunk_size), "Removing keywords", total=chunk_count):
        print(f"Removing {len(keyword_ids_chunk)} keywords")
        db.update(
            "DELETE FROM submission_snapshot_keywords WHERE keyword_id IN %s",
            (tuple(keyword_ids_chunk),)
        )


def remove_orphaned_keywords(db: Database) -> int:
    print("Scanning for orphaned keywords")
    orphaned_keywords = db.select_iter(
        "SELECT keywords.keyword_id "
        "FROM submission_snapshot_keywords keywords "
        "LEFT JOIN submission_snapshots submissions "
        "ON keywords.submission_snapshot_id = submissions.submission_snapshot_id "
        "WHERE submissions.submission_snapshot_id IS NULL",
        tuple()
    )
    remove_ids = []
    with timer("Scanning for orphaned keywords"):
        for keyword_row in orphaned_keywords:
            keyword_id = keyword_row[0]
            print(f"Removed orphaned keyword, ID: {keyword_id}")
            remove_ids.append(keyword_id)
    delete_keywords(db, remove_ids)
    return len(remove_ids)


def delete_submissions(db: Database, submission_ids: List[int]) -> None:
    if DRY_RUN:
        return
    if not submission_ids:
        print("No submission snapshots to remove")
        return
    chunk_size = 1000
    chunk_count = (len(submission_ids) // chunk_size) + 1
    file_ids = []
    for submission_ids_chunk in tqdm.tqdm(
            chunks(submission_ids, chunk_size),
            "Removing submissions",
            total=chunk_count
    ):
        print(f"Removing {len(submission_ids_chunk)} keywords")

        file_rows = db.select_iter(
            "SELECT file_id FROM submission_snapshot_files WHERE submission_snapshot_id IN %s",
            (tuple(submission_ids_chunk),)
        )
        file_ids.extend([file_row[0] for file_row in file_rows])
        db.update(
            "DELETE FROM submission_snapshot_files WHERE submission_snapshot_id IN %s",
            (tuple(submission_ids_chunk),)
        )
        db.update(
            "DELETE FROM submission_snapshot_keywords WHERE submission_snapshot_id IN %s",
            (tuple(submission_ids_chunk),)
        )
        db.update(
            "DELETE FROM submission_snapshots WHERE submission_snapshot_id IN %s",
            (tuple(submission_ids_chunk),)
        )
    delete_file_hashes_by_file(db, file_ids)


def remove_duplicate_submission_snapshots(db: Database) -> int:
    print("Scanning for duplicate submissions")
    duplicate_rows = db.select_iter(
        "SELECT submission_snapshot_id FROM ( "
        "SELECT submission_snapshot_id, row_number() over w as rnum "
        "FROM submission_snapshots WINDOW w AS ( "
        "PARTITION BY website_id, site_submission_id, scan_datetime, archive_contributor_id "
        "ORDER BY submission_snapshot_id "
        ")) t WHERE t.rnum > 1",
        tuple()
    )
    remove_ids = []
    with timer("Scanning for duplicate submissions"):
        for submission_row in duplicate_rows:
            snapshot_id = submission_row[0]
            print(f"Removing duplicate submission snapshot, ID: {snapshot_id}")
            remove_ids.append(snapshot_id)
    delete_submissions(db, remove_ids)
    return len(remove_ids)


def delete_users(db: Database, user_ids: List[int]) -> None:
    if DRY_RUN:
        return
    if not user_ids:
        print("No user snapshots to remove")
        return
    chunk_size = 1000
    chunk_count = (len(user_ids) // chunk_size) + 1
    for user_ids_chunk in tqdm.tqdm(chunks(user_ids, chunk_size), "Removing users", total=chunk_count):
        print(f"Removing {len(user_ids_chunk)} users")
        db.update(
            "DELETE FROM user_snapshots WHERE user_snapshot_id IN %s",
            (tuple(user_ids_chunk),)
        )


def remove_duplicate_user_snapshots(db: Database) -> int:
    print("Scanning for duplicate users")
    duplicate_users = db.select_iter(
        "SELECT user_snapshot_id FROM ( "
        "SELECT user_snapshot_id, row_number() over w as rnum "
        "FROM user_snapshots WINDOW w AS ( "
        "PARTITION BY website_id, site_user_id, scan_datetime, archive_contributor_id ORDER BY user_snapshot_id "
        ")) t WHERE t.rnum > 1",
        tuple()
    )
    remove_ids = []
    with timer("Scanning for duplicate users"):
        for user_row in duplicate_users:
            snapshot_id = user_row[0]
            print(f"Removing duplicate user snapshot, ID: {snapshot_id}")
            remove_ids.append(snapshot_id)
    delete_users(db, remove_ids)
    return len(remove_ids)


if __name__ == "__main__":
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    removed_users = remove_duplicate_user_snapshots(db_obj)
    removed_hashes = remove_orphaned_file_hashes(db_obj)
    removed_hashes += remove_duplicate_file_hashes(db_obj)
    removed_files = remove_orphaned_files(db_obj)
    removed_files += remove_duplicate_files(db_obj)
    removed_keywords = remove_orphaned_keywords(db_obj)
    removed_submissions = remove_duplicate_submission_snapshots(db_obj)
    print(f"Removed users: {removed_users}")
    print(f"Removed hashes: {removed_hashes}")
    print(f"Removed keywords: {removed_keywords}")
    print(f"Removed files: {removed_files}")
    print(f"Removed submissions: {removed_submissions}")
