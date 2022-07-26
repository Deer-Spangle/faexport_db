import csv
import pathlib
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TypeVar, Optional, Iterator, List, Tuple, Callable, Union

import tqdm

from faexport_db.db import Database
from faexport_db.ingest_formats.base import FormatResponse
from faexport_db.models.submission import SubmissionSnapshot
from faexport_db.models.user import UserSnapshot

RowType = TypeVar("RowType")


def csv_count_rows(file_path: Union[str, pathlib.Path]) -> int:
    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        return sum(1 for _ in tqdm.tqdm(reader, desc="Counting rows"))


@contextmanager
def cache_in_file(file_path: Union[str, pathlib.Path], generator: Callable[[], str]) -> str:
    try:
        with open(file_path, "r") as cache_file:
            return cache_file.read()
    except FileNotFoundError:
        pass
    result = generator()
    with open(file_path, "w") as cache_file:
        cache_file.write(result)
    return result


class IngestionJob(ABC):
    SAVE_AFTER = 100

    def __init__(self, *, skip_rows: int = 0) -> None:
        self.skip_rows = skip_rows

    @abstractmethod
    def row_count(self) -> Optional[int]:
        pass

    @abstractmethod
    def convert_row(self, row: RowType) -> FormatResponse:
        pass

    @abstractmethod
    def iterate_rows(self) -> Iterator[RowType]:
        pass

    def ingest_data(self, db: Database) -> None:
        submissions_by_row: List[Tuple[int, SubmissionSnapshot]] = []
        users_by_row: List[Tuple[int, UserSnapshot]] = []

        progress = tqdm.tqdm(self.iterate_rows(), desc="Scanning data", total=self.row_count())
        for row_num, row in enumerate(progress):
            if row_num < self.skip_rows:
                continue
            result = self.convert_row(row)
            # Add result to cached rows
            for snapshot in result.submission_snapshots:
                submissions_by_row.append((row_num, snapshot))
            for snapshot in result.user_snapshots:
                users_by_row.append((row_num, snapshot))
            # Check whether to save submissions
            if len(submissions_by_row) > self.SAVE_AFTER:
                progress.set_description(f"Saving {len(submissions_by_row)} submission snapshots")
                SubmissionSnapshot.save_batch(db, [snapshot for _, snapshot in submissions_by_row])
                submissions_by_row.clear()
            # Check whether to save users
            if len(users_by_row) > self.SAVE_AFTER:
                progress.set_description(f"Saving {len(users_by_row)} user snapshots")
                UserSnapshot.save_batch(db, [snapshot for _, snapshot in users_by_row])
                users_by_row.clear()
            # Update description
            row_nums = [num for num, _ in submissions_by_row] + [num for num, _ in users_by_row]
            lowest_row_num = row_num
            if row_nums:
                lowest_row_num = min(row_nums)
            progress.set_description(
                f"Collecting snapshots ({len(submissions_by_row)}s, {len(users_by_row)}u, >{lowest_row_num}"
            )
        SubmissionSnapshot.save_batch(db, [snapshot for _, snapshot in submissions_by_row])
        UserSnapshot.save_batch(db, [snapshot for _, snapshot in users_by_row])
