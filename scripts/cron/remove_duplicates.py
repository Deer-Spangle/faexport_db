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
    for hash_row in tqdm.tqdm(orphaned_hashes, "Scanning orphaned file hashes"):
        hash_id = hash_row[0]
        print(f"Removed orphaned file hash, ID: {hash_id}")
        remove_ids.append(hash_id)
    remove_file_hashes(db, remove_ids)
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
    for hash_row in tqdm.tqdm(duplicate_hashes, "Scanning duplicate file hashes"):
        hash_id = hash_row[0]
        print(f"Removing duplicate file hash, ID: {hash_id}")
        remove_ids.append(hash_id)
    remove_file_hashes(db, remove_ids)
    return len(remove_ids)


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
    for file_row in tqdm.tqdm(orphaned_files, "Scanning orphaned files"):
        file_id = file_row[0]
        print(f"Removed orphaned file, ID: {file_id}")
        remove_ids.append(file_id)
    remove_files(db, remove_ids)
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
    for file_row in tqdm.tqdm(duplicate_files, "Scanning duplicate files"):
        file_id = file_row[0]
        print(f"Removing duplicate file, ID: {file_id}")
        remove_ids.append(file_id)
    remove_files(db, remove_ids)
    return len(remove_ids)


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
    for keyword_row in tqdm.tqdm(orphaned_keywords, "Scanning orphaned keywords"):
        keyword_id = keyword_row[0]
        print(f"Removed orphaned keyword, ID: {keyword_id}")
        remove_ids.append(keyword_id)
    remove_keywords(db, remove_ids)
    return len(remove_ids)


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
    for submission_row in tqdm.tqdm(duplicate_rows, "Scanning duplicate submissions"):
        snapshot_id = submission_row[0]
        print(f"Removing duplicate submission snapshot, ID: {snapshot_id}")
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
    for user_row in tqdm.tqdm(duplicate_users, "Scanning duplicate files"):
        snapshot_id = user_row[0]
        print(f"Removing duplicate user snapshot, ID: {snapshot_id}")
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
