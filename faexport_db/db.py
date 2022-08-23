import datetime
import json
from json import JSONEncoder
from typing import Tuple, List, Any, Optional, Dict, TypeVar, Iterable

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
    return json.dumps(data, cls=CustomJSONEncoder)


N = TypeVar("N")


def chunks(lst: List[N], n: int) -> Iterable[List[N]]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def parse_datetime(datetime_str: Optional[str]) -> Optional[datetime.datetime]:
    if not datetime_str:
        return None
    return dateutil.parser.parse(datetime_str)


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, datetime.datetime):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


class Database:
    def __init__(self, conn):
        self.conn = conn
        self.analyze = False

    def select(self, query: str, args: Tuple) -> List[Any]:
        with self.conn.cursor() as cur:
            cur.execute(query, args)
            result = cur.fetchall()
        if self.analyze:
            with self.conn.cursor() as cur:
                cur.execute("explain analyze "+query, args)
                analyze_result = cur.fetchall()
                print(analyze_result)
        return result

    def select_iter(self, query: str, args: Tuple) -> Iterable[Any]:
        with self.conn.cursor("select_iter") as cur:
            cur.execute(query, args)
            while True:
                rows = cur.fetchmany(5000)
                if not rows:
                    break
                for row in rows:
                    yield row

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

    def bulk_insert(
            self,
            table_name: str,
            columns: Tuple[str, ...],
            values: List[Tuple[Any, ...]],
            id_column: str,
            chunk_size: int = 1000
    ) -> Iterable[int]:
        if id_column in columns:
            raise ValueError("ID column should not be in the list of columns")
        if not columns:
            raise ValueError("Column list is missing")
        if not values:
            return []
        param_str = "(" + ", ".join("%s" for _ in columns) + ")"
        for values_chunk in chunks(values, chunk_size):
            query_str = (
                f"INSERT INTO {table_name} ("
                + ", ".join(columns) + ") VALUES "
                + ", ".join(param_str for _ in values_chunk)
                + f"RETURNING {id_column}"
            )
            param_values = tuple(sum([list(entry) for entry in values_chunk], start=[]))
            inserted_rows = self.insert(query_str, param_values)
            for row in inserted_rows:
                yield row[0]

    def update(self, query: str, args: Tuple) -> None:
        with self.conn.cursor() as cur:
            try:
                cur.execute(query, args)
                self.conn.commit()
            except psycopg2.Error as e:
                self.conn.rollback()
                raise e
