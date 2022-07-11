import datetime
from typing import Optional, Dict, Any, List

from faexport_db.db import merge_dicts, Database, json_to_db
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
            scan_datetime: datetime = None,
            *,
            user_snapshot_id: int = None,
            ingest_datetime: datetime = None,
            is_deleted: bool = False,
            display_name: str = None,
            extra_data: Dict[str, Any] = None,
    ):
        self.website_id = website_id
        self.site_user_id = site_user_id
        self.scan_datetime = scan_datetime or datetime.datetime.now(datetime.timezone.utc)
        self.scan_datetime_set = scan_datetime is not None
        self.contributor = contributor

        self.user_snapshot_id = user_snapshot_id
        self.ingest_datetime = ingest_datetime or datetime.datetime.now(datetime.timezone.utc)
        self.is_deleted = is_deleted
        self.display_name = display_name
        self.extra_data = extra_data

    def create_snapshot(self, db: "Database") -> None:
        self.contributor.save(db)
        user_rows = db.insert(
            "WITH e AS( "
            "INSERT INTO user_snapshots "
            "(website_id, site_user_id, scan_datetime, archive_contributor_id, ingest_datatime, is_deleted, "
            "display_name, extra_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (website_id, site_user_id, scan_datetime, archive_contributor_id) DO NOTHING "
            "RETURNING user_snapshot_id "
            ") SELECT * FROM e "
            "UNION "
            "SELECT user_snapshot_id FROM user_snapshots "
            "WHERE website_id = %s, site_user_id = %s, scan_datetime = %s, archive_contributor_id = %s, ",
            (
                self.website_id, self.site_user_id, self.scan_datetime, self.contributor.contributor_id,
                self.ingest_datetime, self.is_deleted, self.display_name, json_to_db(self.extra_data),
                self.website_id, self.site_user_id, self.scan_datetime, self.contributor.contributor_id
            )
        )
        self.user_snapshot_id = user_rows[0][0]

    def save(self, db: "Database") -> None:
        if self.user_snapshot_id is None:
            return self.create_snapshot(db)
