import json
from typing import List

import psycopg2
import tqdm

from faexport_db.db import Database, chunks

DRY_RUN = True


def remove_file_hash(db: Database, hash_id: int) -> None:
    if DRY_RUN:
        return
    db.update(
        "DELETE FROM submission_snapshot_file_hashes WHERE hash_id = %s",
        (hash_id,)
    )


def scan_file_hashes(db: Database) -> int:
    print("Scanning file hashes")
    hash_rows = db.select(
        "SELECT hash_id, file_id, algo_id FROM submission_snapshot_file_hashes", tuple()
    )
    index = set()
    removed = 0
    for hash_row in tqdm.tqdm(hash_rows):
        index_entry = (hash_row[1], hash_row[2])
        if index_entry in index:
            print(f"Removing duplicate file hash, ID: {hash_row[0]}")
            remove_file_hash(db, hash_row[0])
            removed += 1
        else:
            index.add(index_entry)
    return removed


def remove_file(db: Database, file_id: int) -> None:
    if DRY_RUN:
        return
    db.update("DELETE FROM submission_snapshot_file_hashes WHERE file_id = %s", (file_id,))
    db.update("DELETE FROM submission_snapshot_files WHERE file_id = %s", (file_id,))


def scan_files(db: Database) -> int:
    print("Scanning files")
    file_rows = db.select(
        "SELECT file_id, submission_snapshot_id, site_file_id FROM submission_snapshot_files", tuple()
    )
    index = set()
    removed = 0
    for file_row in tqdm.tqdm(file_rows):
        index_entry = (file_row[1], file_row[2])
        if index_entry in index:
            print(f"Removing duplicate file, ID: {file_row[0]}")
            remove_file(db, file_row[0])
            removed += 1
        else:
            index.add(index_entry)
    return removed


def remove_submission(db: Database, sub_id: int) -> None:
    if DRY_RUN:
        return
    file_rows = db.select(
        "SELECT file_id FROM submission_snapshot_files WHERE submission_snapshot_id = %s",
        (sub_id,)
    )
    for file_row in file_rows:
        db.update("DELETE FROM submission_snapshot_file_hashes WHERE file_id = %s", (file_row[0],))
    db.update("DELETE FROM submission_snapshot_files WHERE submission_snapshot_id = %s", (sub_id,))
    db.update("DELETE FROM submission_snapshot_keywords WHERE submission_snapshot_id = %s", (sub_id,))
    db.update("DELETE FROM submission_snapshots WHERE submission_snapshot_id = %s", (sub_id,))


def scan_submissions(db: Database) -> int:
    print("Scanning submissions")
    sub_rows = db.select(
        "SELECT submission_snapshot_id, website_id, site_submission_id, scan_datetime, archive_contributor_id "
        "FROM submission_snapshots", tuple()
    )
    index = set()
    removed = 0
    for sub_row in tqdm.tqdm(sub_rows):
        index_entry = tuple(sub_row[1:])
        if index_entry in index:
            print(f"Removing duplicate submission, ID: {sub_row[0]}")
            remove_submission(db, sub_row[0])
            removed += 1
        else:
            index.add(index_entry)
    return removed


def remove_user(db: Database, user_id: int) -> None:
    if DRY_RUN:
        return
    db.update("DELETE FROM user_snapshots WHERE user_snapshot_id = %s", (user_id,))


def remove_users(db: Database, user_ids: List[int]) -> None:
    if DRY_RUN:
        return
    chunk_size = 1000
    chunk_count = (len(user_ids) // chunk_size) + 1
    for user_ids_chunk in tqdm.tqdm(chunks(user_ids, chunk_size), total=chunk_count):
        print(f"Removing {len(user_ids_chunk)} users")
        db.update(
            "DELETE FROM user_snapshots WHERE user_snapshot_id IN %s",
            (tuple(user_ids_chunk),)
        )


def scan_users(db: Database) -> int:
    print("Scanning users")
    user_rows = db.select(
        "SELECT user_snapshot_id, website_id, site_user_id, scan_datetime, archive_contributor_id FROM user_snapshots",
        tuple()
    )
    index = set()
    remove_ids = []
    for user_row in tqdm.tqdm(user_rows):
        index_entry = tuple(user_row[1:])
        if index_entry in index:
            print(f"Found duplicate user, ID: {user_row[0]}")
            remove_ids.append(user_row[0])
        else:
            index.add(index_entry)
    remove_users(db, remove_ids)
    return len(remove_ids)


if __name__ == "__main__":
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    removed_users = scan_users(db_obj)
    removed_hashes = scan_file_hashes(db_obj)
    removed_files = scan_files(db_obj)
    removed_submissions = scan_submissions(db_obj)
    print(f"Removed users: {removed_users}")
    print(f"Removed hashes: {removed_hashes}")
    print(f"Removed files: {removed_files}")
    print(f"Removed submissions: {removed_submissions}")
