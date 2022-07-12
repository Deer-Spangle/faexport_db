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
