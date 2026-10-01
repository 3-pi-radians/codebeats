"""Field audit: which payload fields exist per event type before and after 7 Oct 2025.

Reads rows with tabledata.list (not billed by BigQuery), so this costs 0 bytes.
Outputs:
  reports/field_audit.md                       local only (.md files are git-ignored): presence table
  data/samples/field_audit/<day>/<type>.json   git-ignored: a few raw payloads for inspection

Run: .venv/bin/python scripts/01_field_audit.py
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from google.cloud import bigquery

from codebeat.config import REPO_ROOT, load_settings, secret
from codebeat.ingest.bq import BigQueryGuard

DAYS = {"20220315": "in study span", "20250930": "pre-change", "20251015": "post-change"}
EVENT_TYPES = ["PushEvent", "PullRequestEvent", "PullRequestReviewEvent",
               "PullRequestReviewCommentEvent", "IssuesEvent", "IssueCommentEvent",
               "ReleaseEvent", "CreateEvent", "WatchEvent", "ForkEvent"]
REPORT_GROUPS = EVENT_TYPES[:2] + ["PullRequestEvent:closed"] + EVENT_TYPES[2:]
TARGET_PER_TYPE = 200
MAX_ROWS_PER_DAY = 150_000
N_SAMPLES_SAVED = 3

# Fields the pipeline needs. Anything else found is listed in the CSV, not the report.
KEY_FIELDS = {
    "PushEvent": ["ref", "head", "before", "size", "distinct_size", "commits", "push_id"],
    "PullRequestEvent": ["action", "number", "pull_request.merged", "pull_request.merged_at",
                         "pull_request.merged_by", "pull_request.user.login",
                         "pull_request.author_association", "pull_request.created_at",
                         "pull_request.closed_at", "pull_request.title", "pull_request.body",
                         "pull_request.base.ref", "pull_request.base.repo.language",
                         "pull_request.comments", "pull_request.review_comments"],
    "PullRequestReviewEvent": ["action", "review.state", "review.user.login",
                               "review.submitted_at", "review.body", "review.author_association",
                               "pull_request.number"],
    "PullRequestReviewCommentEvent": ["comment.body", "comment.user.login", "comment.created_at",
                                      "comment.author_association", "pull_request.number"],
    "IssuesEvent": ["action", "issue.number", "issue.user.login", "issue.created_at",
                    "issue.closed_at", "issue.title", "issue.body", "issue.author_association",
                    "issue.pull_request", "issue.state"],
    "IssueCommentEvent": ["action", "comment.body", "comment.user.login", "comment.created_at",
                          "comment.author_association", "issue.number", "issue.pull_request"],
    # closed PRs only: does the payload still say whether the PR was merged?
    "PullRequestEvent:closed": ["pull_request.merged", "pull_request.merged_at",
                                "pull_request.merged_by", "pull_request.closed_at",
                                "pull_request.merge_commit_sha"],
    "ReleaseEvent": ["action", "release.tag_name", "release.published_at", "release.prerelease"],
    "CreateEvent": ["ref_type", "ref", "master_branch"],
    "WatchEvent": ["action"],
    "ForkEvent": ["forkee.id"],
}


def flatten(obj, prefix=""):
    """Yield (path, value) for nested dicts. Lists are reported as one leaf, not expanded."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                yield p, v
                yield from flatten(v, p)
            else:
                yield p, v


def audit_day(guard: BigQueryGuard, day: str, sample_dir: Path):
    counts = defaultdict(int)
    present = defaultdict(lambda: defaultdict(int))   # type -> path -> rows where key exists
    nonnull = defaultdict(lambda: defaultdict(int))   # type -> path -> rows where value non-null/non-empty
    samples = defaultdict(list)
    table = f"githubarchive.day.{day}"
    for row in guard.iter_rows(table, ["type", "payload", "created_at"],
                               f"field_audit {day}", MAX_ROWS_PER_DAY):
        t = row["type"]
        if t not in EVENT_TYPES or counts[t] >= TARGET_PER_TYPE:
            if all(counts[e] >= TARGET_PER_TYPE for e in EVENT_TYPES):
                break
            continue
        counts[t] += 1
        payload = json.loads(row["payload"]) if row["payload"] else {}
        groups = [t]
        if t == "PullRequestEvent" and payload.get("action") == "closed":
            groups.append("PullRequestEvent:closed")
            counts["PullRequestEvent:closed"] += 1
        for g in groups:
            for path, val in flatten(payload):
                present[g][path] += 1
                if val not in (None, "", [], {}):
                    nonnull[g][path] += 1
        if len(samples[t]) < N_SAMPLES_SAVED:
            samples[t].append(payload)
    sample_dir.mkdir(parents=True, exist_ok=True)
    for t, s in samples.items():
        (sample_dir / f"{t}.json").write_text(json.dumps(s, indent=2)[:200_000])
    return counts, present, nonnull


def cell(n, present, nonnull, path):
    if n == 0:
        return "no rows"
    p, nn = present.get(path, 0), nonnull.get(path, 0)
    if p == 0:
        return "**absent**"
    return f"{100 * nn / n:.0f}% / {100 * p / n:.0f}%"


def main():
    settings = load_settings()
    client = bigquery.Client(project=secret(settings["bigquery"]["project_env"]))
    guard = BigQueryGuard(client, REPO_ROOT / settings["bigquery"]["byte_log"],
                          settings["bigquery"]["max_bytes_without_approval"])
    results = {}
    for day in DAYS:
        print(f"auditing {day} ...", flush=True)
        results[day] = audit_day(guard, day, REPO_ROOT / "data/samples/field_audit" / day)
        print("  rows per type:", dict(results[day][0]), flush=True)

    # full path inventory as CSV (no user data, just field names and rates)
    csv_lines = ["event_type,path," + ",".join(f"{d}_present,{d}_nonnull,{d}_n" for d in DAYS)]
    for t in REPORT_GROUPS:
        paths = sorted(set().union(*(results[d][1][t].keys() for d in DAYS)))
        for p in paths:
            vals = []
            for d in DAYS:
                n = results[d][0][t]
                vals += [str(results[d][1][t].get(p, 0)), str(results[d][2][t].get(p, 0)), str(n)]
            csv_lines.append(f"{t},{p}," + ",".join(vals))
    (REPO_ROOT / "reports/field_audit_paths.csv").write_text("\n".join(csv_lines) + "\n")

    md = ["# GH Archive field audit", "",
          "Generated by `scripts/01_field_audit.py` via tabledata.list (0 bytes billed).",
          "Cell = `% rows non-null / % rows where the key exists`. **absent** = key never seen "
          "(→ `not_available` from this source). Full path list: `reports/field_audit_paths.csv`.", "",
          "| Day | Role | Rows per type |", "|---|---|---|"]
    for d, role in DAYS.items():
        md.append(f"| {d} | {role} | " + ", ".join(f"{t}: {results[d][0][t]}" for t in REPORT_GROUPS) + " |")
    for t in REPORT_GROUPS:
        md += ["", f"## {t}", "", "| Field | " + " | ".join(DAYS) + " |",
               "|---|" + "---|" * len(DAYS)]
        for f in KEY_FIELDS[t]:
            md.append(f"| `{f}` | " + " | ".join(
                cell(results[d][0][t], results[d][1][t], results[d][2][t], f) for d in DAYS) + " |")
    md += ["", "## Interpretation", "", "_To be written after reviewing the numbers above._", ""]
    (REPO_ROOT / "reports/field_audit.md").write_text("\n".join(md))
    print("wrote reports/field_audit.md and reports/field_audit_paths.csv")


if __name__ == "__main__":
    main()
