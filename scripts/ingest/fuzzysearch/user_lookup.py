import dataclasses
import datetime
import json
import string
import time
from abc import ABC, abstractmethod
from threading import Lock
from typing import Dict, List, Optional

import dateutil.parser
import requests

from faexport_db.db import CustomJSONEncoder, Database
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.user import UserSnapshot

WEASYL_ID = "weasyl"
FA_ID = "fa"


@dataclasses.dataclass
class CacheEntry:
    username: str

    def to_json(self) -> Dict:
        return {
            "username": self.username,
        }

    @classmethod
    def from_json(cls, data: Dict) -> "CacheEntry":
        return cls(
            data["username"],
        )


class UserLookup(ABC):
    FILENAME: str = None

    def __init__(self, site_id: str, db: Database):
        self.cache: Dict[str, CacheEntry] = self.load_cache()
        self.site_id = site_id
        self.db = db
        self.save_calls = 0

    def save_cache(self) -> None:
        self.save_calls += 1
        if self.save_calls < 50:
            return
        self.save_calls = 0
        data = {
            key: entry.to_json() for key, entry in self.cache.items()
        }
        with open(self.FILENAME, "w") as f:
            json.dump(data, f, cls=CustomJSONEncoder)

    @classmethod
    def load_cache(cls) -> Dict[str, CacheEntry]:
        try:
            with open(cls.FILENAME, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            return {}
        cache = {
            key: CacheEntry.from_json(cache_data) for key, cache_data in data.items()
        }
        return cache

    def get_user_cache(
            self,
            display_name: str,
            submission_id: str,
            contributor: ArchiveContributor,
            scan_datetime: datetime.datetime
    ) -> CacheEntry:
        cache_entry = self.cache.get(display_name)
        if cache_entry is not None:
            return cache_entry
        snapshots = self.create_user_snapshots(display_name, submission_id, contributor, scan_datetime)
        cache_entry = CacheEntry(
            snapshots[0].site_user_id,
        )
        for snapshot in snapshots:
            snapshot.save(self.db)
            self.cache[snapshot.display_name] = cache_entry
        self.save_cache()
        return cache_entry

    @abstractmethod
    def create_user_snapshots(
            self,
            display_name: str,
            submission_id: str,
            contributor: ArchiveContributor,
            scan_datetime: datetime.datetime
    ) -> List[UserSnapshot]:
        pass

    def get_username(
            self,
            display_name: str,
            submission_id: str,
            contributor: ArchiveContributor,
            scan_date: datetime.datetime
    ) -> str:
        cache_entry = self.get_user_cache(display_name, submission_id, contributor, scan_date)
        return cache_entry.username


class WeasylLookup(UserLookup):
    username_chars = string.ascii_letters + string.digits
    FILENAME = "./cache_weasyl_lookup.json"

    def __init__(self, db: Database, api_key: Optional[str] = None) -> None:
        super().__init__(WEASYL_ID, db)
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

    def __init__(self, db: Database):
        super().__init__(FA_ID, db)

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
