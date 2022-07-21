import datetime
import json
import string
import time
from abc import ABC, abstractmethod
from threading import Lock
from typing import Dict, List, Optional

import dateutil.parser
import requests

from faexport_db.db import CustomJSONEncoder
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.user import UserSnapshot

WEASYL_ID = "weasyl"
FA_ID = "fa"


class UserLookup(ABC):
    FILENAME: str = None

    def __init__(self, site_id: str):
        self.cache: Dict[str, List[UserSnapshot]] = self.load_cache()
        self.site_id = site_id

    def save_cache(self) -> None:
        data = {
            key: [snapshot.to_web_json() for snapshot in snapshots]
            for key, snapshots in self.cache.items()
        }
        with open(self.FILENAME, "w") as f:
            json.dump(data, f, cls=CustomJSONEncoder)

    @classmethod
    def load_cache(cls) -> Dict:
        try:
            with open(cls.FILENAME, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            return {}
        cache = {
            key: [
                UserSnapshot(
                    snapshot["website_id"],
                    snapshot["site_user_id"],
                    ArchiveContributor(
                        snapshot["cache_data"]["archive_contributor"]["name"],
                        contributor_id=snapshot["cache_data"]["archive_contributor"]["contributor_id"],
                    ),
                    dateutil.parser.parse(snapshot["cache_data"]["scan_datetime"]),
                    user_snapshot_id=snapshot["user_snapshot_id"],
                    ingest_datetime=snapshot["cache_data"]["ingest_datetime"],
                    is_deleted=snapshot["user_data"]["is_deleted"],
                    display_name=snapshot["user_data"]["display_name"],
                    extra_data=snapshot["user_data"]["extra_data"]
                )
                for snapshot in snapshots
            ]
            for key, snapshots in data.items()
        }
        return cache

    def get_user_snapshots(
            self,
            display_name: str,
            submission_id: str,
            contributor: ArchiveContributor,
            scan_datetime: datetime.datetime
    ) -> List[UserSnapshot]:
        if display_name in self.cache:
            snapshots = self.cache[display_name]
            if scan_datetime not in [snapshot.scan_datetime for snapshot in snapshots]:
                snapshots.append(UserSnapshot(
                    self.site_id,
                    snapshots[0].site_user_id,
                    contributor,
                    scan_datetime,
                    display_name=display_name
                ))
                self.cache[display_name] = snapshots
                self.save_cache()
            return snapshots
        snapshots = self.create_user_snapshots(display_name, submission_id, contributor, scan_datetime)
        for snapshot in snapshots:
            self.cache[snapshot.display_name] = snapshots
        self.save_cache()

    @abstractmethod
    def create_user_snapshots(
            self,
            display_name: str,
            submission_id: str,
            contributor: ArchiveContributor,
            scan_datetime: datetime.datetime
    ) -> List[UserSnapshot]:
        pass


class WeasylLookup(UserLookup):
    username_chars = string.ascii_letters + string.digits
    FILENAME = "./cache_weasyl_lookup.json"

    def __init__(self, api_key: Optional[str] = None) -> None:
        super().__init__()
        self.api_key = api_key
        self.last_request = datetime.datetime.now()
        self._lock = Lock()

    def fetch_api(self, path: str) -> Dict:
        with self._lock:
            while self.last_request + datetime.timedelta(seconds=1) > datetime.datetime.now():
                time.sleep(0.1)
            headers = {
                "User-Agent": "Spangle's faexport_db ingest thingy",
            }
            if self.api_key:
                headers["X-Weasyl-API-Key"] = self.api_key
            resp = requests.get(
                f"https://weasyl.com/api/{path}",
                headers=headers
            ).json()
            self.last_request = datetime.datetime.now()
            return resp

    def create_user_snapshots(
            self,
            display_name: str,
            submission_id: str,
            contributor: ArchiveContributor,
            scan_datetime: datetime.datetime
    ) -> List[UserSnapshot]:
        username_guess = "".join([char for char in display_name.lower() if char in self.username_chars])
        # Fetch weasyl user response
        try:
            resp = self.fetch_api(f"/users/{username_guess}/view")
            site_username = resp["login_name"]
            site_display_name = resp["username"]
            if display_name == site_display_name:
                snapshots = [
                    UserSnapshot(
                        WEASYL_ID,
                        site_username,
                        contributor,
                        scan_datetime,
                        display_name=display_name
                    ),
                    UserSnapshot(
                        WEASYL_ID,
                        site_username,
                        contributor,
                        datetime.datetime.now(datetime.timezone.utc),
                        display_name=site_display_name,
                        extra_data={
                            "full_name": resp["full_name"],
                            "catchphrase": resp["catchphrase"],
                            "profile_text": resp["profile_text"],
                            "stream_url": resp["stream_url"],
                            "stream_text": resp["stream_text"],
                            "show_favorites_bar": resp["show_favorites_bar"],
                            "show_favorites_tab": resp["show_favorites_tab"],
                            "banned": resp["banned"],
                            "suspended": resp["suspended"],
                            "streaming_status": resp["streaming_status"],
                            "created_at": dateutil.parser.parse(resp["created_at"]),
                            "media": resp["media"],
                            "avatar_url": resp["media"]["avatar"][0]["url"],
                            "folders": resp["folders"],
                            "commission_info": resp["commission_info"],
                            "recent_type": resp["recent_type"],
                            "featured_submission": resp["featured_submission"],
                            "statistics": resp["statistics"],
                        }
                    )
                ]
                return snapshots
        except Exception:
            pass
        resp = self.fetch_api(f"/submissions/{submission_id}/view")
        site_username = resp["owner_login"]
        site_display_name = resp["owner"]
        snapshots = [
            UserSnapshot(
                WEASYL_ID,
                site_username,
                contributor,
                scan_datetime,
                display_name=display_name
            ),
            UserSnapshot(
                WEASYL_ID,
                site_username,
                contributor,
                datetime.datetime.now(datetime.timezone.utc),
                display_name=site_display_name,
                extra_data={
                    "media": resp["owner_media"],
                    "avatar_url": resp["owner_media"]["avatar"][0]["url"],
                }
            )
        ]
        return snapshots


class FALookup(UserLookup):
    FILENAME = "./cache_fa_users.json"
    def create_user_snapshots(
            self,
            display_name: str,
            submission_id: str,
            contributor: ArchiveContributor,
            scan_datetime: datetime.datetime
    ) -> List[UserSnapshot]:
        return [
            UserSnapshot(
                FA_ID,
                display_name.replace("_", ""),
                contributor,
                scan_datetime,
                display_name=display_name
            )
        ]
