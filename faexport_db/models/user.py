import datetime
from typing import Optional, Dict, Any, List, Tuple

from faexport_db.db import merge_dicts, Database, json_to_db, UNSET, unset_to_null


class User:
    def __init__(
            self,
            user_id: int,
            website_id: str,
            site_user_id: str,
            is_deleted: bool,
            first_scanned: datetime.datetime,
            latest_update: datetime.datetime,
            display_name: Optional[str],
            extra_data: Optional[Dict[str, Any]]
    ):
        self.user_id = user_id
        self.website_id = website_id
        self.site_user_id = site_user_id
        self.is_deleted = is_deleted
        self.first_scanned = first_scanned
        self.latest_update = latest_update
        self.display_name = display_name
        self.extra_data = extra_data
        self.updated = False

    def add_update(self, update: "UserUpdate") -> None:
        if update.update_time > self.latest_update:
            self.updated = True
            self.latest_update = update.update_time
            if update.is_deleted is not UNSET:
                self.is_deleted = update.is_deleted
            if update.display_name is not UNSET:
                self.display_name = update.display_name
            if update.add_extra_data is not UNSET:
                self.extra_data = merge_dicts(self.extra_data, update.add_extra_data)
            return
        # If it's an older update, we can still update some things
        if update.add_extra_data is not UNSET:
            new_extra_data = merge_dicts(update.add_extra_data, self.extra_data)
            self.updated = self.updated or (self.extra_data != new_extra_data)
            self.extra_data = new_extra_data
        if self.display_name is None and update.display_name is not UNSET:
            self.updated = self.updated or (self.display_name != update.display_name)
            self.display_name = update.display_name
        if self.first_scanned > update.update_time:
            self.updated = True
            self.first_scanned = update.update_time

    def save(self, db: "Database") -> None:
        if self.updated:
            db.update(
                "UPDATE users "
                "SET is_deleted = %s AND first_scanned = %s AND latest_update = %s AND display_name = %s "
                "AND extra_data = %s "
                "WHERE user_id = %s", (
                    self.is_deleted, self.first_scanned, self.latest_update, self.display_name,
                    json_to_db(self.extra_data), self.user_id
                )
            )

    @classmethod
    def from_database_by_user_id(cls, db: "Database", user_id: int) -> Optional["User"]:
        user_rows = db.select(
            "SELECT user_id, website_id, site_user_id, is_deleted, first_scanned, latest_update, display_name, "
            "extra_data "
            "FROM users WHERE user_id = %s",
            (user_id,),
        )
        return cls.from_user_rows(user_rows)

    @classmethod
    def from_database(
        cls, db: "Database", website_id: str, site_user_id: str
    ) -> Optional["User"]:
        user_rows = db.select(
            "SELECT user_id, website_id, site_user_id, is_deleted, first_scanned, latest_update, display_name, "
            "extra_data "
            "FROM users WHERE website_id = %s AND site_user_id = %s",
            (website_id, site_user_id),
        )
        return cls.from_user_rows(user_rows)

    @classmethod
    def from_user_rows(
        cls,
        user_rows: List[
            Tuple[
                int,
                str,
                str,
                bool,
                datetime.datetime,
                datetime.datetime,
                Optional[str],
                Optional[Dict],
            ]
        ],
    ) -> Optional["User"]:
        if not user_rows:
            return None
        (
            user_id,
            website_id,
            site_user_id,
            is_deleted,
            first_scanned,
            latest_update,
            display_name,
            extra_data,
        ) = user_rows[0]
        return cls(
            user_id,
            website_id,
            site_user_id,
            is_deleted,
            first_scanned,
            latest_update,
            display_name,
            extra_data,
        )


class UserUpdate:
    def __init__(
            self,
            website_id: str,
            site_user_id: str,
            update_time: datetime.datetime = None,
            is_deleted: bool = False,
            *,
            display_name: str = UNSET,
            add_extra_data: Dict[str, Any] = UNSET,
    ):
        self.website_id = website_id
        self.site_user_id = site_user_id
        self.update_time = update_time or datetime.datetime.now(datetime.timezone.utc)
        self.update_time_set = update_time is not None
        self.is_deleted = is_deleted
        self.display_name = display_name
        self.add_extra_data = add_extra_data

    def create_user(self, db: "Database") -> User:
        display_name = unset_to_null(self.display_name)
        extra_data = unset_to_null(self.add_extra_data)
        user_rows = db.insert(
            "INSERT INTO users "
            "(website_id, site_user_id, is_deleted, first_scanned, latest_update, display_name, extra_data) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING user_id", (
                self.website_id, self.site_user_id, self.is_deleted, self.update_time, self.update_time,
                display_name, extra_data
            )
        )
        user_id = user_rows[0][0]
        return User(
            user_id,
            self.website_id,
            self.site_user_id,
            self.is_deleted,
            self.update_time,
            self.update_time,
            display_name,
            extra_data
        )

    def save(self, db: "Database") -> User:
        user = User.from_database(db, self.website_id, self.site_user_id)
        if user is not None:
            user.add_update(self)
            user.save(db)
            return user
        return self.create_user(db)
