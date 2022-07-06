import datetime
import json
from typing import Tuple, List, Any, Optional, Dict, Union

UNSET = object()

# TODO: remove these
SITE_ID = "fa"
DATA_DATE = datetime.datetime(2019, 12, 4, 0, 0, 0, tzinfo=datetime.timezone.utc)


def merge_dicts(base: Optional[Dict[str, Any]], overlay: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if base is None:
        if overlay is None:
            return None
        return overlay
    if overlay is None:
        return base
    return {**base, **overlay}


def json_to_db(data: Optional[Dict[str, Any]]) -> Optional[str]:
    if data is None:
        return None
    return json.dumps(data)


def unset_to_null(obj: Union[type(UNSET), Any]) -> Optional[Any]:
    if obj is UNSET:
        return None
    return obj


class Database:
    def __init__(self, conn):
        self.conn = conn

    def select(self, query: str, args: Tuple) -> List[Any]:
        cur = self.conn.cursor()
        cur.execute(query, args)
        result = cur.fetchall()
        cur.close()
        return result

    def insert(self, query: str, args: Tuple) -> List[Any]:
        cur = self.conn.cursor()
        cur.execute(query, args)
        result = cur.fetchall()
        self.conn.commit()
        cur.close()
        return result

    def update(self, query: str, args: Tuple) -> None:
        cur = self.conn.cursor()
        cur.execute(query, args)
        self.conn.commit()
        cur.close()

    def add_or_update_submission(
            self,
            submission_id: int,
            title: str,
            description: str,
            upload_datetime: datetime.datetime,
            rating: str,
            uploader_id: int,
            keywords: List[str]
    ) -> int:
        if sub_row := self.select(
                "SELECT submission_id, latest_update, extra_data FROM submissions "
                "WHERE website_id = %s AND site_submission_id = %s",
                (SITE_ID, str(submission_id))
        ):
            sub_id, latest_update, extra_data = sub_row[0]
            extra_data["rating"] = rating
            if latest_update < DATA_DATE:
                self.update(
                    "UPDATE submissions "
                    "SET is_deleted = %s, latest_update = %s, uploader_id = %s, title = %s, description = %s, "
                    "datetime_posted = %s, extra_data = %s "
                    "WHERE submission_id = %s",
                    (
                        False, DATA_DATE, uploader_id, title, description, upload_datetime, json_to_db(extra_data),
                        sub_id
                    )
                )
                self.update_keywords(sub_id, keywords)
            return sub_id
        result = self.insert(
            "INSERT INTO submissions (website_id, site_submission_id, is_deleted, first_scanned, latest_update, "
            "uploader_id, title, description, datetime_posted, extra_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING submission_id", (
                SITE_ID, str(submission_id), False, DATA_DATE, DATA_DATE, uploader_id, title, description,
                upload_datetime, json_to_db({"rating": rating})
            ))
        sub_id = result[0][0]
        self.add_keywords(sub_id, keywords)
        return sub_id

    def update_keywords(self, sub_id: int, keywords: List[str]) -> None:
        keyword_rows = self.select(
            "SELECT keyword_id, keyword, ordinal FROM submission_keywords WHERE submission_id = %s", (sub_id,))
        db_keywords = [row[1] for row in sorted(keyword_rows, key=lambda r: r[2])]
        if keywords == db_keywords:
            return
        self.update("DELETE FROM submission_keywords WHERE submission_id = %s", (sub_id,))
        self.add_keywords(sub_id, keywords)

    def add_keywords(self, sub_id: int, keywords: List[str]) -> None:
        if not keywords:
            return
        arg_str = ", ".join("(%s, %s, %s)" for _ in range(len(keywords)))
        self.update("INSERT INTO submission_keywords (submission_id, keyword, ordinal) VALUES " + arg_str,
                    tuple(sum([[sub_id, keyword, num] for num, keyword in enumerate(keywords)], [])))

    def add_or_update_file(self, sub_id: int, filename: str) -> None:
        file_rows = self.select("SELECT file_id, file_url, latest_update FROM files WHERE submission_id = %s",
                                (sub_id,))
        if file_rows:
            file_id, file_url, datetime_posted = file_rows[0]
            if datetime_posted < DATA_DATE:
                if file_url != filename:
                    self.update("DELETE FROM file_hashes WHERE file_id = %s", (file_id,))
                self.update("UPDATE files SET file_url = %s, latest_update = %s WHERE file_id = %s",
                            (filename, DATA_DATE, file_id))
            return file_id
        file_id = self.insert("INSERT INTO files (submission_id, site_file_id, first_scanned, latest_update, file_url) "
                              "VALUES (%s, %s, %s, %s, %s) RETURNING file_id",
                              (sub_id, SITE_ID, DATA_DATE, DATA_DATE, filename))
        return file_id[0][0]
