import dataclasses
from abc import ABC, abstractmethod
from typing import Dict, List

from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.submission import SubmissionSnapshot
from faexport_db.models.user import UserSnapshot


@dataclasses.dataclass
class FormatResponse:
    submission_snapshots: List[SubmissionSnapshot] = dataclasses.field(default_factory=lambda: [])
    user_snapshots: List[UserSnapshot] = dataclasses.field(default_factory=lambda: [])
    
    def add_user_snapshot(self, user: UserSnapshot) -> None:
        self.user_snapshots.append(user)
        
    def add_submission_snapshot(self, sub: SubmissionSnapshot) -> None:
        self.submission_snapshots.append(sub)


class BaseFormat(ABC):
    @property
    @abstractmethod
    def format_name(self) -> str:
        pass

    @abstractmethod
    def format_web_data(self, web_data: Dict, contributor: ArchiveContributor) -> FormatResponse:
        pass


class SimpleUserSnapshot(BaseFormat):
    format_name = "user"
    
    def format_web_data(self, web_data: Dict, contributor: ArchiveContributor) -> FormatResponse:
        resp = FormatResponse()
        resp.add_user_snapshot(UserSnapshot.from_web_json(web_data, contributor))
        return resp


class SimpleSubmissionSnapshot(BaseFormat):
    format_name = "submission"

    def format_web_data(self, web_data: Dict, contributor: ArchiveContributor) -> FormatResponse:
        resp = FormatResponse()
        resp.add_submission_snapshot(SubmissionSnapshot.from_web_json(web_data, contributor))
        return resp


class BulkUserSnapshots(BaseFormat):
    @property
    def format_name(self) -> str:
        pass

    def format_web_data(self, web_data: Dict, contributor: ArchiveContributor) -> FormatResponse:
        pass