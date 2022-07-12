from faexport_db.models.file import HashAlgo
from faexport_db.models.website import Website
from flask import Flask

app = Flask(__name__)

db = Database()

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
        return {
            "error": f"Website does not exist by ID: {website_id}"
        }
    submission = Submission.from_database(db, website.website_id, submission_id)
    if not submission:
        return {
            "error": f"There is no entry for submission with the ID {submission_id} on {website.full_name}"
        }
    return {
        "error": None,
        "data": submission.to_web_json()
    }

@app.route("/api/view/users/<website_id>/<user_id>.json")
def view_user(website_id: str, user_id: str):
    website = Website.from_database(db, website_id)
    if not website:
        return {
            "error": f"Website does not exist by ID: {website_id}"
        }
    user = User.from_database(db, website_id)
    if not user:
        return {
            "error": f"There is no entry for submission with the ID {submission_id} on {website.full_name}"
        }
    return {
        "error": None,
        "data": user.to_web_json()
    }

@app.route("/api/websites.json")
def list_websites() -> Dict:
    websites = Website.list_all(db)
    return {
        "error": None,
        "data": {
            "websites": [website.to_web_json() for website in websites]
        }
    }

@app.route("/api/hash_algos.json")
def list_hash_algos() -> Dict:
    hash_algos = HashAlgo.list_all(db)
    return {
        "error": None,
        "data": {
            "hash_algos": [hash_algo.to_web_json() for hash_algo in hash_algos]
        }
    }
