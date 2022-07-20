import datetime
import json
import string
import time
from threading import Lock
from typing import Dict, List

import dateutil.parser
import requests

from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.user import UserSnapshot

WEASYL_ID = "weasyl"


class WeasylLookup:
    username_chars = string.ascii_letters + string.digits
    FILENAME = "./cache_weasyl_lookup.json"

    def __init__(self):
        self.cache: Dict[str, List[UserSnapshot]] = self.load_cache()
        self.last_request = datetime.datetime.now()
        self._lock = Lock()

    def save_cache(self) -> None:
        data = {
            key: [snapshot.to_web_json() for snapshot in snapshots]
            for key, snapshots in self.cache.items()
        }
        with open(self.FILENAME, "w") as f:
            json.dump(data, f)

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

    def fetch_api(self, path: str) -> Dict:
        with self._lock:
            while self.last_request + datetime.timedelta(seconds=1) > datetime.datetime.now():
                time.sleep(0.1)
            resp = requests.get(
                f"https://weasyl.com/api/{path}",
                headers={
                    "User-Agent": "Spangle's faexport_db ingest thingy"
                }
            ).json()
            self.last_request = datetime.datetime.now()
            return resp

    def get_user_snapshots(
            self,
            display_name: str,
            submission_id: str,
            contributor: ArchiveContributor,
            scan_datetime: datetime.datetime
    ) -> List[UserSnapshot]:
        if display_name in self.cache:
            return self.cache[display_name]
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
                            "catchphrase": resp["cachephrase"],
                            "profile_text": resp["profile_text"],
                            "stream_text": resp["stream_text"],
                            "show_favorites_bar": resp["show_favorites_bar"],
                            "show_favorites_tab": resp["show_favorites_tab"],
                            "banned": resp["banned"],
                            "suspended": resp["suspended"],
                            "streaming_status": resp["streaming_status"],
                            "created_at": dateutil.parser.parse(resp["created_at"]),
                            "media": resp["media"],
                            "avatar_url": resp["media"]["avatar"][0]["url"],
                            "full_name": resp["full_name"],
                            "folders": resp["folders"],
                            "commission_info": resp["commission_info"],
                            "recent_type": resp["recent_type"],
                            "featured_submission": resp["featured_submission"],
                            "statistics": resp["statistics"],
                        }
                    )
                ]
                self.cache[display_name] = snapshots
                self.save_cache()
                return snapshots
        except Exception:
            pass
        resp = self.fetch_api(f"/submission/{submission_id}/view")
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
        self.cache[display_name] = snapshots
        self.cache[site_display_name] = snapshots
        self.save_cache()
        return snapshots
