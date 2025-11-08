import os
import sys
import json
import argparse
from datetime import datetime, timezone, timedelta
import requests
from requests.adapters import HTTPAdapter, Retry


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract merged PRs to raw JSON")
    p.add_argument("--owner", default="Scytale-exercise", help="Repository owner/org")
    p.add_argument("--repo", default="scytale-repo3", help="Repository name")
    p.add_argument("--start-date", default=None,
                   help="Filter merged PRs on/after this date (YYYY-MM-DD)")
    p.add_argument("--end-date", default=None,
                   help="Filter merged PRs on/before this date (YYYY-MM-DD)")
    p.add_argument("--out", default="outputs/raw/raw_pr_data.json",
                   help="Output path for raw JSON")
    return p.parse_args()


def to_utc_date(d: str | None, end: bool = False) -> datetime | None:
    """Convert YYYY-MM-DD to UTC datetime bounds (inclusive)."""
    if not d:
        return None
    base = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end:
        # inclusive end of day
        return base + timedelta(days=1) - timedelta(microseconds=1)
    return base


def iso_to_dt_utc(iso: str) -> datetime:
    # GitHub timestamps are like "2025-11-06T09:05:11Z"
    return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def build_session(token: str) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "scytale-extract-script"
    })
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def next_link(resp: requests.Response) -> str | None:
    link = resp.headers.get("Link", "")
    # Example: <https://api.github.com/...&page=2>; rel="next", <...>; rel="last"
    if not link:
        return None
    parts = [p.strip() for p in link.split(",")]
    for p in parts:
        if 'rel="next"' in p:
            start = p.find("<") + 1
            end = p.find(">")
            return p[start:end]
    return None


def main():
    args = parse_args()

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("❌ GITHUB_TOKEN environment variable is not set.", file=sys.stderr)
        sys.exit(2)

    start_dt = to_utc_date(args.start_date, end=False)
    end_dt = to_utc_date(args.end_date, end=True)

    session = build_session(token)

    base_url = f"https://api.github.com/repos/{args.owner}/{args.repo}/pulls"
    params = {
        "state": "closed",
        "per_page": 100,
        "page": 1,
        "sort": "updated",
        "direction": "desc",
    }

    print(f"⏳ Fetching merged PRs from {args.owner}/{args.repo} ...")
    print(f"   Date filter: start={args.start_date or 'None'} end={args.end_date or 'None'}")

    merged_kept = []
    pages_scanned = 0

    url = base_url
    while True:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"❌ GitHub API error {resp.status_code}: {resp.text[:300]}", file=sys.stderr)
            sys.exit(1)

        prs = resp.json() or []
        pages_scanned += 1

        # keep merged PRs only + apply date filters (inclusive)
        for pr in prs:
            merged_at = pr.get("merged_at")
            if not merged_at:
                continue
            mdt = iso_to_dt_utc(merged_at)
            if start_dt and mdt < start_dt:
                continue
            if end_dt and mdt > end_dt:
                continue
            merged_kept.append(pr)

        nxt = next_link(resp)
        if not nxt or not prs:
            break
        # when following a link, don't pass params again
        url, params = nxt, None

    # Ensure output directory exists (lowercase 'outputs' everywhere)
    out_path = args.out
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(merged_kept, f, indent=2)

    print(f"✅ Pages scanned: {pages_scanned}")
    print(f"✅ Merged PRs matched filters: {len(merged_kept)}")
    print(f"💾 Saved raw PR data to {out_path}")


if __name__ == "__main__":
    main()
