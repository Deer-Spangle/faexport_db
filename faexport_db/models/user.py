import datetime
from typing import Optional, Dict, Any, List

from faexport_db.db import merge_dicts, Database, json_to_db, parse_datetime
from faexport_db.models.archive_contributor import ArchiveContributor


class User:
    def __init__(
            self,
            website_id: str,
            site_user_id: str,
            snapshots: List["UserSnapshot"],
    ):
        self.website_id = website_id
        self.site_user_id = site_user_id
        self.snapshots = snapshots

    @property
    def sorted_snapshots(self) -> List["UserSnapshot"]:
        return sorted(self.snapshots, key=lambda s: s.scan_datetime, reverse=True)

    @property
    def is_deleted(self) -> bool:
        return self.sorted_snapshots[0].is_deleted

    @property
    def first_scanned(self) -> datetime.datetime:
        return self.sorted_snapshots[-1].scan_datetime

    @property
    def latest_update(self) -> datetime.datetime:
        return self.sorted_snapshots[0].scan_datetime

    @property
    def display_name(self) -> Optional[str]:
        for snapshot in self.snapshots:
            if snapshot.display_name is not None:
                return snapshot.display_name
        return None

    @property
    def extra_data(self) -> Dict[str, Any]:
        extra_data = {}
        for snapshot in self.sorted_snapshots[::-1]:
            if snapshot.extra_data is not None:
                extra_data = merge_dicts(extra_data, snapshot.extra_data)
        return extra_data
    
    def to_web_json(self) -> Dict:
        return {
            "website_id": self.website_id,
            "site_user_id": self.site_user_id,
            "cache_data": {
                "snapshot_count": len(self.snapshots),
                "first_scanned": self.first_scanned,
                "latest_update": self.latest_update,
            },
            "user_data": {
                "is_deleted": self.is_deleted,
                "display_name": self.display_name,
                "extra_data": self.extra_data,
            }
        }

    def to_web_snapshots_json(self) -> Dict:
        return {
            "website_id": self.website_id,
            "site_user_id": self.site_user_id,
            "snapshot_count": len(self.snapshots),
            "snapshots": [snapshot.to_web_json() for snapshot in self.sorted_snapshots]
        }

    @classmethod
    def from_database(
        cls, db: "Database", website_id: str, site_user_id: str
    ) -> Optional["User"]:
        snapshot_rows = db.select(
            "SELECT u.user_snapshot_id, u.scan_datetime, u.archive_contributor_id, a.name as contributor_name, "
            "u.ingest_datetime, u.is_deleted, u.display_name, u.extra_data "
            "FROM user_snapshots u "
            "LEFT JOIN archive_contributors a ON u.archive_contributor_id = a.contributor_id "
            "WHERE website_id = %s AND site_user_id = %s",
            (
                website_id, site_user_id
            )
        )
        snapshots = []
        contributors = {}
        for row in snapshot_rows:
            (
                snapshot_id, scan_datetime, contributor_id, contributor_name, ingest_datetime, is_deleted,
                display_name, extra_data
            ) = row
            contributor = contributors.get(contributor_id)
            if contributor is None:
                contributor = ArchiveContributor(contributor_name, contributor_id=contributor_id)
                contributors[contributor_id] = contributor
            snapshots.append(UserSnapshot(
                website_id,
                site_user_id,
                contributor,
                scan_datetime,
                user_snapshot_id=snapshot_id,
                ingest_datetime=ingest_datetime,
                is_deleted=is_deleted,
                display_name=display_name,
                extra_data=extra_data
            ))
        if not snapshots:
            return None
        return User(
            website_id,
            site_user_id,
            snapshots
        )


class UserSnapshot:
    def __init__(
            self,
            website_id: str,
            site_user_id: str,
            contributor: ArchiveContributor,
            scan_datetime: datetime.datetime = None,
            *,
            user_snapshot_id: int = None,
            ingest_datetime: datetime.datetime = None,
            is_deleted: bool = False,
            display_name: str = None,
            extra_data: Dict[str, Any] = None,
    ):
        self.website_id = website_id
        self.site_user_id = site_user_id
        self.scan_datetime = scan_datetime or datetime.datetime.now(datetime.timezone.utc)
        self.contributor = contributor

        self.user_snapshot_id = user_snapshot_id
        self.ingest_datetime = ingest_datetime or datetime.datetime.now(datetime.timezone.utc)
        self.is_deleted = is_deleted
        self.display_name = display_name
        self.extra_data = extra_data
    
    def to_web_json(self) -> Dict:
        return {
            "user_snapshot_id": self.user_snapshot_id,
            "website_id": self.website_id,
            "site_user_id": self.site_user_id,
            "cache_data": {
                "scan_datetime": self.scan_datetime,
                "archive_contributor": self.contributor.to_web_json(),
                "ingest_datetime": self.ingest_datetime,
            },
            "user_data": {
                "is_deleted": self.is_deleted,
                "display_name": self.display_name,
                "extra_data": self.extra_data,
            },
        }

    @classmethod
    def from_web_json(cls, web_data: Dict, contributor: ArchiveContributor) -> "UserSnapshot":
        return cls(
            web_data["website_id"],
            web_data["site_user_id"],
            contributor,
            parse_datetime(web_data.get("scan_datetime")),
            is_deleted=web_data.get("is_deleted", False),
            display_name=web_data.get("display_name"),
            extra_data=web_data.get("extra_data"),
        )

    def create_snapshot(self, db: "Database") -> None:
        user_rows = db.insert(
            "INSERT INTO user_snapshots "
            "(website_id, site_user_id, scan_datetime, archive_contributor_id, ingest_datetime, is_deleted, "
            "display_name, extra_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "RETURNING user_snapshot_id ",
            (
                self.website_id, self.site_user_id, self.scan_datetime, self.contributor.contributor_id,
                self.ingest_datetime, self.is_deleted, self.display_name, json_to_db(self.extra_data),
            )
        )
        self.user_snapshot_id = user_rows[0][0]

    def save(self, db: "Database") -> None:
        if self.user_snapshot_id is None:
            return self.create_snapshot(db)

    @classmethod
    def save_batch(cls, db: "Database", snapshots: List["UserSnapshot"]) -> None:
        unsaved = [snapshot for snapshot in snapshots if snapshot.user_snapshot_id is None]
        user_ids = db.bulk_insert(
            "user_snapshots",
            (
                "website_id", "site_user_id", "scan_datetime", "archive_contributor_id", "ingest_datetime",
                "is_deleted", "display_name", "extra_data"
            ),
            [
                (
                    user.website_id, user.site_user_id, user.scan_datetime, user.contributor.contributor_id,
                    user.ingest_datetime, user.is_deleted, user.display_name, json_to_db(user.extra_data)
                )
                for user in unsaved
            ],
            "user_snapshot_id"
        )
        for user_snapshot, snapshot_id in zip(unsaved, user_ids):
            user_snapshot.user_snapshot_id = snapshot_id
