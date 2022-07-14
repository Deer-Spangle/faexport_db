import datetime
from typing import Dict

import dateutil.parser

from faexport_db.ingest_formats.base import BaseFormat, FormatResponse
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.file import File
from faexport_db.models.submission import SubmissionSnapshot
from faexport_db.models.user import UserSnapshot

FA_SITE_ID = "fa"


class FAExportSubmission(BaseFormat):
    format_name = "faexport_submission"

    def format_web_data(self, web_data: Dict, contributor: ArchiveContributor) -> FormatResponse:
        scrape_time = datetime.datetime.now(datetime.timezone.utc)
        resp = FormatResponse()
        if "error" in web_data:
            return resp

        submission_id = web_data["link"].strip("/").split("/")[-1]
        submission = SubmissionSnapshot(
            FA_SITE_ID,
            submission_id,
            contributor,
            scrape_time,
            uploader_site_user_id=web_data["profile_name"],
            title=web_data["title"],
            description=web_data["description"],
            datetime_posted=dateutil.parser.parse(web_data["posted_at"]),
            extra_data={
                "rating": web_data["rating"],
                "category": web_data["category"],
                "theme": web_data["theme"],
                "species": web_data["species"],
                "gender": web_data["gender"],
                "fav_count": web_data["favorites"],
                "comment_count": web_data["comments"],
                "view_count": web_data["views"]
            },
            files=[File(
                None,
                file_url=web_data["download"],
                extra_data={
                    "thumbnail_url": web_data["thumbnail"],
                    "full_preview_url": web_data["full"],
                    "image_width": web_data["resolution"].split("x")[0] if web_data["resolution"] else None,
                    "image_height": web_data["resolution"].split("x")[1] if web_data["resolution"] else None
                }
            )],
            ordered_keywords=web_data["keywords"]
        )
        resp.add_submission_snapshot(submission)
        user = UserSnapshot(
            FA_SITE_ID,
            web_data["profile_name"],
            contributor,
            scrape_time,
            display_name=web_data["name"],
            extra_data={
                "avatar_url": web_data["avatar"]
            }
        )
        resp.add_user_snapshot(user)
        return resp


class FAExportUser(BaseFormat):
    format_name = "faexport_user"

    def format_web_data(self, web_data: Dict, contributor: ArchiveContributor) -> FormatResponse:
        scrape_time = datetime.datetime.now(datetime.timezone.utc)
        resp = FormatResponse()
        if "error" in web_data:
            if web_data["error"].startswith("User has disabled their account"):
                user_id = web_data["url"].strip("/").split("/")[-1]
                resp.add_user_snapshot(UserSnapshot(
                    FA_SITE_ID,
                    user_id,
                    contributor,
                    scrape_time,
                    is_deleted=True
                ))
            return resp
        user_id = web_data["profile"].strip("/").split("/")[-1]
        user = UserSnapshot(
            FA_SITE_ID,
            user_id,
            contributor,
            scrape_time,
            display_name=web_data["name"],
            extra_data={
                "account_type": web_data["account_type"],
                "avatar_url": web_data["avatar"],
                "user_title": web_data["user_title"],
                "registered_datetime": dateutil.parser.parse(web_data["registered_at"]),
                "guest_access": web_data["guest_access"],
                "current_mood": web_data["current_mood"],
                "profile_html": web_data["artist_profile"],
                "view_count": web_data["pageviews"],
                "submission_count": web_data["submissions"],
                "comments_received_count": web_data["comments_received"],
                "comments_given_count": web_data["comments_given"],
                "journal_count": web_data["journals"],
                "favorite_count": web_data["favorites"],
                "featured_submission_id": web_data.get("featured_submission", {}).get("id"),
                "profile_id_submission_id": web_data.get("profile_id", {}).get("id"),
                "artist_information": web_data["artist_information"],
                "contact_information": web_data["contact_information"],
                "watchers_count": web_data["watchers"]["count"],
                "watching_count": web_data["watching"]["count"],
                "watchers_recent": [
                    {
                        "site_user_id": watcher_data["profile_name"],
                        "display_name": watcher_data["name"]
                    } for watcher_data in web_data["watchers"]["recent"]
                ],
                "watching_recent": [
                    {
                        "site_user_id": watching_data["profile_name"],
                        "display_name": watching_data["name"]
                    } for watching_data in web_data["watching"]["recent"]
                ],
            }
        )
        resp.add_user_snapshot(user)
        for watch_data in web_data["watchers"]["recent"] + web_data["watching"]["recent"]:
            resp.add_user_snapshot(UserSnapshot(
                FA_SITE_ID,
                watch_data["profile_name"],
                contributor,
                scrape_time,
                display_name=watch_data["name"]
            ))
        if web_data["featured_submission"]:
            resp.add_submission_snapshot(featured_submission(
                web_data["featured_submission"],
                contributor,
                scrape_time,
                user_id
            ))
        if web_data["profile_id"]:
            resp.add_submission_snapshot(featured_submission(
                web_data["profile_id"],
                contributor,
                scrape_time,
                user_id
            ))
        return resp


def featured_submission(
        sub_data: Dict,
        contributor: ArchiveContributor,
        scrape_time: datetime.datetime,
        user_id: str,
) -> SubmissionSnapshot:
    return SubmissionSnapshot(
        FA_SITE_ID,
        sub_data["id"],
        contributor,
        scrape_time,
        uploader_site_user_id=user_id,
        title=sub_data["title"],
        files=[File(
            None,
            extra_data={
                "thumbnail_url": sub_data["thumbnail"],
            }
        )]
    )
