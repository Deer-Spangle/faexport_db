import json
import os
from typing import Dict, Tuple, Type

import psycopg2
from werkzeug.routing import BaseConverter, ValidationError

from faexport_db.db import Database
from faexport_db.ingest_formats.base import SimpleUserSnapshot, SimpleSubmissionSnapshot, BaseFormat
from faexport_db.ingest_formats.faexport import FAExportUser, FAExportSubmission
from faexport_db.models.archive_contributor import ArchiveContributor
from faexport_db.models.file import HashAlgo
from faexport_db.models.submission import Submission
from faexport_db.models.user import User
from faexport_db.models.website import Website
from flask import Flask, request


class IngestionFormatConverter(BaseConverter):
    """Extracts an ingestion format from the path and returns an ingestion formatter"""
    format_classes = [SimpleUserSnapshot, SimpleSubmissionSnapshot, FAExportUser, FAExportSubmission]
    regex = "|".join(klass.format_name for klass in format_classes)

    def to_python(self, value: str) -> Type[BaseFormat]:
        format_map = {klass.format_name: klass for klass in self.format_classes}
        klass = format_map.get(value)
        if klass:
            return klass
        raise ValidationError()

    def to_url(self, value: Type[BaseFormat]) -> str:
        if isinstance(value.format_name, str):
            return str(value.format_name)
        raise ValidationError()


app = Flask(__name__)
app.url_map.converters["ingest_format"] = IngestionFormatConverter

dsn = os.getenv("DSN")
if dsn is None:
    with open("./config.json", "r") as f:
        conf = json.load(f)
    dsn = conf["db_conn"]
db_conn = psycopg2.connect(dsn)
db = Database(db_conn)


def error_resp(code: int, message: str) -> Tuple[Dict, int]:
    return {
        "error": {
            "code": code,
            "message": message
        }
    }, code


@app.route('/')
def hello():
    return (
        'Welcome to FAExport_DB. This is a project to provide a cache database for furry art websites, and '
        'hopefully reduce scraping impact on those websites!'
    )


@app.route("/api/view/submissions/<website_id>/<submission_id>.json")
def view_submission(website_id: str, submission_id: str):
    website = Website.from_database(db, website_id)
    if not website:
        return error_resp(404, f"Website does not exist by ID: {website_id}")
    submission = Submission.from_database(db, website.website_id, submission_id)
    if not submission:
        return error_resp(404, f"There is no entry for a submission with the ID {submission_id} on {website.full_name}")
    return {
        "data": submission.to_web_json()
    }


@app.route("/api/view/submissions/<website_id>/<submission_id>/snapshots.json")
def view_submission_snapshots(website_id: str, submission_id: str):
    website = Website.from_database(db, website_id)
    if not website:
        return error_resp(404, f"Website does not exist by ID: {website_id}")
    submission = Submission.from_database(db, website.website_id, submission_id)
    if not submission:
        return error_resp(
            404,
            f"There are no snapshots for a submission with the ID {submission_id} on {website.full_name}"
        )
    return {
        "data": submission.to_web_snapshots_json()
    }


@app.route("/api/view/users/<website_id>/<user_id>.json")
def view_user(website_id: str, user_id: str):
    website = Website.from_database(db, website_id)
    if not website:
        return error_resp(404, f"Website does not exist by ID: {website_id}")
    user = User.from_database(db, website_id, user_id)
    if not user:
        return error_resp(404, f"There is no entry for a user with the ID {user_id} on {website.full_name}")
    return {
        "data": user.to_web_json()
    }


@app.route("/api/view/users/<website_id>/<user_id>/snapshots.json")
def view_user_snapshots(website_id: str, user_id: str):
    website = Website.from_database(db, website_id)
    if not website:
        return error_resp(404, f"Website does not exist by ID: {website_id}")
    user = User.from_database(db, website_id, user_id)
    if not user:
        return error_resp(404, f"There are no snapshots for a user with the ID {user_id} on {website.full_name}")
    return {
        "data": user.to_web_snapshots_json()
    }


@app.route("/api/view/users/<website_id>.json")
def list_users(website_id: str):
    website = Website.from_database(db, website_id)
    if not website:
        return error_resp(404, f"Website does not exist by ID: {website_id}")
    user_rows = db.select(
        "SELECT DISTINCT site_user_id FROM user_snapshots WHERE website_id = %s",
        (website.website_id,)
    )
    user_ids = [user_row[0] for user_row in user_rows]
    return {
        "data": {
            "num_users": len(user_ids),
            "user_ids": user_ids
        }
    }


@app.route("/api/ingest/<ingest_format:formatter>", methods=["POST"])
def ingest_data(formatter: BaseFormat):
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return error_resp(403, "An API key is required to access this service")
    contributor = ArchiveContributor.from_database_by_api_key(db, api_key)
    if contributor is None:
        return error_resp(403, "Invalid API key")
    web_data = request.json
    if not web_data:
        return error_resp(400, "Submission snapshot data must be posted as json")
    format_resp = formatter.format_web_data(web_data, contributor)
    for snapshot in format_resp.submission_snapshots:
        snapshot.save(db)
    for snapshot in format_resp.user_snapshots:
        snapshot.save(db)


@app.route("/api/websites.json")
def list_websites() -> Dict:
    websites = Website.list_all(db)
    return {
        "data": {
            "websites": [website.to_web_json(db) for website in websites]
        }
    }


@app.route("/api/hash_algos.json")
def list_hash_algos() -> Dict:
    hash_algos = HashAlgo.list_all(db)
    return {
        "data": {
            "hash_algos": [hash_algo.to_web_json() for hash_algo in hash_algos]
        }
    }


@app.route("/api/archive_contributors.json")
def list_archive_contributors() -> Dict:
    contributors = ArchiveContributor.list_all(db)
    return {
        "data": {
            "archive_contributors": [contributor.to_web_json(db) for contributor in contributors]
        }
    }
