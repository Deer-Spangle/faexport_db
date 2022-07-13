import json

import psycopg2
import tqdm

from faexport_db.db import Database

DRY_RUN = True


def remove_file_hash(db: Database, hash_id: int) -> None:
    if DRY_RUN:
        return
    db.update(
        "DELETE FROM submission_snapshot_file_hashes WHERE hash_id = %s",
        (hash_id,)
    )


def scan_file_hashes(db: Database) -> None:
    print("Scanning file hashes")
    hash_rows = db.select(
        "SELECT hash_id, file_id, algo_id FROM submission_snapshot_file_hashes", tuple()
    )
    index = set()
    for hash_row in tqdm.tqdm(hash_rows):
        index_entry = (hash_row[1], hash_row[2])
        if index_entry in index:
            print(f"Removing duplicate file hash, ID: {hash_row[0]}")
            remove_file_hash(db, hash_row[0])
        else:
            index.add(index_entry)


def remove_file(db: Database, file_id: int) -> None:
    if DRY_RUN:
        return
    db.update("DELETE FROM submission_snapshot_file_hashes WHERE file_id = %s", (file_id,))
    db.update("DELETE FROM submission_snapshot_files WHERE file_id = %s", (file_id,))


def scan_files(db: Database) -> None:
    print("Scanning files")
    file_rows = db.select(
        "SELECT file_id, submission_snapshot_id, site_file_id FROM submission_snapshot_files", tuple()
    )
    index = set()
    for file_row in tqdm.tqdm(file_rows):
        index_entry = (file_row[1], file_row[2])
        if index_entry in index:
            print(f"Removing duplicate file, ID: {file_row[0]}")
            remove_file(db, file_row[0])
        else:
            index.add(index_entry)


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


def scan_submissions(db: Database) -> None:
    print("Scanning submissions")
    sub_rows = db.select(
        "SELECT submission_snapshot_id, website_id, site_submission_id, scan_datetime, archive_contributor_id "
        "FROM submission_snapshots", tuple()
    )
    index = set()
    for sub_row in tqdm.tqdm(sub_rows):
        index_entry = tuple(sub_row[1:])
        if index_entry in index:
            print(f"Removing duplicate submission, ID: {sub_row[0]}")
            remove_submission(db, sub_row[0])
        else:
            index.add(index_entry)


def remove_user(db: Database, user_id: int) -> None:
    if DRY_RUN:
        return
    db.update("DELETE FROM user_snapshots WHERE user_snapshot_id = %s", (user_id,))


def scan_users(db: Database) -> None:
    print("Scanning users")
    user_rows = db.select(
        "SELECT user_snapshot_id, website_id, site_user_id, scan_datetime, archive_contributor_id FROM user_snapshots",
        tuple()
    )
    index = set()
    for user_row in tqdm.tqdm(user_rows):
        index_entry = tuple(user_row[1:])
        if index_entry in index:
            print(f"Removing duplicate user, ID: {user_row[0]}")
            remove_user(db, user_row[0])
        else:
            index.add(index_entry)


if __name__ == "__main__":
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    scan_users(db_obj)
    scan_file_hashes(db_obj)
    scan_files(db_obj)
    scan_submissions(db_obj)
