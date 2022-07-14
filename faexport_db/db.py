import json
from typing import Tuple, List, Any, Optional, Dict, TypeVar

import dateutil.parser
import psycopg2


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


N = TypeVar("N")


def chunks(lst: List[N], n: int) -> List[List[N]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def parse_datetime(datetime_str: Optional[str]) -> Optional[datetime.datetime]:
    if not datetime_str:
        return None
    return dateutil.parser.parse(datetime_str)


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
