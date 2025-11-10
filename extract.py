import json
import os
import csv
import requests

# ---------------- CONFIGURATION ----------------
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_OWNER = "Scytale-exercise"
REPO_NAME = "scytale-repo3"

RAW_FILE = os.path.join("outputs", "raw", "raw_pr_data.json")
OUTPUT_FILE = os.path.join("outputs", "processed", "pr_report.csv")

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

# ------------- HELPER FUNCTIONS ----------------
def fetch_pr_reviews(pr_number):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/pulls/{pr_number}/reviews"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return []
    return response.json()

def fetch_commit_checks(sha):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/commits/{sha}/check-runs"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return []
    return response.json().get("check_runs", [])

def cr_passed(pr_number):
    reviews = fetch_pr_reviews(pr_number)
    for review in reviews:
        if review.get("state") == "APPROVED":
            return "✅"
    return "❌"

def checks_passed(sha):
    checks = fetch_commit_checks(sha)
    if not checks:
        return "❌"
    for check in checks:
        if check.get("conclusion") != "success":
            return "❌"
    return "✅"

# ---------------- MAIN ----------------
with open(RAW_FILE, "r", encoding="utf-8") as f:
    prs = json.load(f)

rows = []
for pr in prs:
    pr_number = pr["number"]
    pr_title = pr["title"]
    author = pr["user"]["login"]
    merge_date = pr["merged_at"]
    merge_sha = pr["merge_commit_sha"]

    cr = cr_passed(pr_number)
    checks = checks_passed(merge_sha)

    rows.append([pr_number, pr_title, author, merge_date, cr, checks])
    print(f"Processed PR #{pr_number}: CR={cr}, CHECKS={checks}")

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["PR Number", "Title", "Author", "Merge Date", "CR_Passed", "CHECKS_PASSED"])
    writer.writerows(rows)

print(f"✅ Report saved to {OUTPUT_FILE}")
