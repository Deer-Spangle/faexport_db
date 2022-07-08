import datetime
import json
from typing import Tuple, List, Any, Optional, Dict, Union

import psycopg2

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
        with self.conn.cursor() as cur:
            cur.execute(query, args)
            result = cur.fetchall()
        return result

    def insert(self, query: str, args: Tuple) -> List[Any]:
        with self.conn.cursor() as cur:
            try:
                cur.execute(query, args)
                result = cur.fetchall()
                self.conn.commit()
            except psycopg2.Error as e:
                self.conn.rollback()
                raise e
        return result

    def update(self, query: str, args: Tuple) -> None:
        with self.conn.cursor() as cur:
            try:
                cur.execute(query, args)
                self.conn.commit()
            except psycopg2.Error as e:
                self.conn.rollback()
                raise e
