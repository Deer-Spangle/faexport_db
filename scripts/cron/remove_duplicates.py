import json
from typing import List

import psycopg2
import tqdm

from faexport_db.db import Database, chunks

DRY_RUN = False


def remove_file_hashes(db: Database, hash_ids: List[int]) -> None:
    if DRY_RUN:
        return
    chunk_size = 1000
    chunk_count = (len(hash_ids) // chunk_size) + 1
    for hash_ids_chunk in tqdm.tqdm(chunks(hash_ids, chunk_size), "Removing hashes", total=chunk_count):
        print(f"Removing {len(hash_ids_chunk)} hashes")
        db.update("DELETE FROM submission_snapshot_file_hashes WHERE hash_id IN %s", (tuple(hash_ids_chunk),))


def remove_file_hashes_by_file(db: Database, file_ids: List[int]) -> None:
    if DRY_RUN:
        return
    chunk_size = 1000
    chunk_count = (len(file_ids) // chunk_size) + 1
    for file_ids_chunk in tqdm.tqdm(chunks(file_ids, chunk_size), "Removing hashes", total=chunk_count):
        print(f"Removing {len(file_ids_chunk)} hashes")
        db.update("DELETE FROM submission_snapshot_file_hashes WHERE file_id IN %s", (tuple(file_ids_chunk),))


def remove_orphaned_and_duplicate_file_hashes(db: Database) -> int:
    print("Scanning file hashes")
    valid_file_ids = {
        row[0]: set() for row in tqdm.tqdm(
            db.select_iter("SELECT file_id FROM submission_snapshot_files", tuple()),
            "Listing file IDs"
        )
    }
    hash_rows = db.select_iter(
        "SELECT hash_id, file_id, algo_id FROM submission_snapshot_file_hashes", tuple()
    )
    remove_ids = []
    for hash_row in tqdm.tqdm(hash_rows, "Scanning file hashes"):
        hash_id, file_id, algo_id = hash_row
        if file_id not in valid_file_ids:
            print(f"Found orphaned file hash, ID: {hash_id}")
            remove_ids.append(hash_id)
        else:
            if algo_id not in valid_file_ids[file_id]:
                valid_file_ids[file_id].add(algo_id)
            else:
                print(f"Removing duplicate file hash, ID: {hash_id}")
                remove_ids.append(hash_id)
    remove_file_hashes(db, remove_ids)
    return len(remove_ids)


def remove_files(db: Database, file_ids: List[int]) -> None:
    if DRY_RUN:
        return
    chunk_size = 1000
    chunk_count = (len(file_ids) // chunk_size) + 1
    for file_ids_chunk in tqdm.tqdm(chunks(file_ids, chunk_size), "Removing files", total=chunk_count):
        print(f"Removing {len(file_ids_chunk)} files")
        db.update("DELETE FROM submission_snapshot_file_hashes WHERE file_id IN %s", (tuple(file_ids_chunk),))
        db.update("DELETE FROM submission_snapshot_files WHERE file_id IN %s", (tuple(file_ids_chunk),))


def remove_orphaned_and_duplicate_files(db: Database) -> int:
    print("Scanning files")
    valid_sub_ids = {
        row[0]: set() for row in tqdm.tqdm(
            db.select_iter("SELECT submission_snapshot_id FROM submission_snapshots", tuple()),
            "Listing submission IDs"
        )
    }
    file_rows = db.select_iter(
        "SELECT file_id, submission_snapshot_id, site_file_id FROM submission_snapshot_files", tuple()
    )
    remove_ids = []
    for file_row in tqdm.tqdm(file_rows, "Scanning files"):
        file_id, sub_id, site_file_id = file_row
        if sub_id not in valid_sub_ids:
            print(f"Removing orphaned file, ID: {file_id}")
            remove_ids.append(file_id)
        else:
            if site_file_id not in valid_sub_ids[sub_id]:
                valid_sub_ids[sub_id].add(site_file_id)
            else:
                print(f"Removing duplicate file, ID: {file_id}")
                remove_ids.append(file_id)
    remove_files(db, remove_ids)
    return len(remove_ids)


def remove_keyword(db: Database, keyword_id: int) -> None:
    if DRY_RUN:
        return
    db.update("DELETE FROM submission_snapshot_keywords WHERE keyword_id = %s", (keyword_id,))


def remove_keywords(db: Database, keyword_ids: List[int]) -> None:
    if DRY_RUN:
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
    print("Scanning keywords")
    valid_sub_ids = {
        row[0] for row in tqdm.tqdm(
            db.select_iter("SELECT submission_snapshot_id FROM submission_snapshots", tuple()),
            "Listing submission IDs"
        )
    }
    keyword_rows = db.select_iter(
        "SELECT keyword_id, submission_snapshot_id FROM submission_snapshot_keywords", tuple()
    )
    remove_ids = []
    for keyword_row in tqdm.tqdm(keyword_rows, "Scanning keywords"):
        keyword_id, sub_id = keyword_row
        if sub_id not in valid_sub_ids:
            print(f"Removing orphaned keyword, ID: {keyword_id}")
            remove_ids.append(keyword_id)
    remove_keywords(db, remove_ids)
    return len(remove_ids)


def remove_submission(db: Database, sub_id: int) -> None:
    if DRY_RUN:
        return
    file_rows = db.select_iter(
        "SELECT file_id FROM submission_snapshot_files WHERE submission_snapshot_id = %s",
        (sub_id,)
    )
    for file_row in file_rows:
        db.update("DELETE FROM submission_snapshot_file_hashes WHERE file_id = %s", (file_row[0],))
    db.update("DELETE FROM submission_snapshot_files WHERE submission_snapshot_id = %s", (sub_id,))
    db.update("DELETE FROM submission_snapshot_keywords WHERE submission_snapshot_id = %s", (sub_id,))
    db.update("DELETE FROM submission_snapshots WHERE submission_snapshot_id = %s", (sub_id,))


def remove_submissions(db: Database, submission_ids: List[int]) -> None:
    if DRY_RUN:
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
    remove_file_hashes_by_file(db, file_ids)


def remove_duplicate_submission_snapshots(db: Database) -> int:
    print("Scanning submissions")
    sub_rows = db.select_iter(
        "SELECT submission_snapshot_id, website_id, site_submission_id, scan_datetime, archive_contributor_id "
        "FROM submission_snapshots", tuple()
    )
    index = {}
    remove_ids = []
    for sub_row in tqdm.tqdm(sub_rows, "Scanning submissions"):
        snapshot_id, website_id, site_submission_id, scan_datetime, contributor_id = sub_row
        if website_id not in index:
            index[website_id] = {contributor_id: {site_submission_id: {scan_datetime}}}
            continue
        if contributor_id not in index[website_id]:
            index[website_id][contributor_id] = {site_submission_id: {scan_datetime}}
            continue
        if site_submission_id not in index[website_id][contributor_id]:
            index[website_id][contributor_id][site_submission_id] = {scan_datetime}
        if scan_datetime in index[website_id][contributor_id][site_submission_id]:
            print(f"Planning to remove duplicate submission snapshot: {snapshot_id}")
            remove_ids.append(snapshot_id)
    remove_submissions(db, remove_ids)
    return len(remove_ids)


def remove_users(db: Database, user_ids: List[int]) -> None:
    if DRY_RUN:
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
    print("Scanning users")
    user_rows = db.select_iter(
        "SELECT user_snapshot_id, website_id, site_user_id, scan_datetime, archive_contributor_id FROM user_snapshots",
        tuple()
    )
    index = {}
    remove_ids = []
    for user_row in tqdm.tqdm(user_rows, "Scanning users"):
        snapshot_id, website_id, site_user_id, scan_datetime, contributor_id = user_row
        if website_id not in index:
            index[website_id] = {contributor_id: {site_user_id: {scan_datetime}}}
            continue
        if contributor_id not in index[website_id]:
            index[website_id][contributor_id] = {site_user_id: {scan_datetime}}
            continue
        if site_user_id not in index[website_id][contributor_id]:
            index[website_id][contributor_id][site_user_id] = {scan_datetime}
            continue
        if scan_datetime not in index[website_id][contributor_id][site_user_id]:
            index[website_id][contributor_id][site_user_id].add(scan_datetime)
            continue
        print(f"Found duplicate user snapshot, ID: {snapshot_id}")
        remove_ids.append(snapshot_id)
    remove_users(db, remove_ids)
    return len(remove_ids)


if __name__ == "__main__":
    config_path = "./config.json"
    with open(config_path, "r") as conf_file:
        config = json.load(conf_file)
    db_dsn = config["db_conn"]
    db_conn = psycopg2.connect(db_dsn)
    db_obj = Database(db_conn)
    removed_users = remove_duplicate_user_snapshots(db_obj)
    removed_hashes = remove_orphaned_and_duplicate_file_hashes(db_obj)
    removed_files = remove_orphaned_and_duplicate_files(db_obj)
    removed_keywords = remove_orphaned_keywords(db_obj)
    removed_submissions = remove_duplicate_submission_snapshots(db_obj)
    print(f"Removed users: {removed_users}")
    print(f"Removed hashes: {removed_hashes}")
    print(f"Removed keywords: {removed_keywords}")
    print(f"Removed files: {removed_files}")
    print(f"Removed submissions: {removed_submissions}")
