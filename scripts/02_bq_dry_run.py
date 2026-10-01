"""D2 confirmation: does `WHERE repo.name IN (...)` reduce bytes scanned on GH Archive?

Only dry runs (free) plus table metadata. Repo list: bigquery.dry_run_repos in config/settings.yaml.
Output: reports/bq_dry_run.md (local only; .md files are git-ignored). Every dry run is appended to logs/bigquery_bytes.csv.

Run: .venv/bin/python scripts/02_bq_dry_run.py
"""
from __future__ import annotations

from datetime import datetime, timezone

from google.cloud import bigquery

from codebeat.config import REPO_ROOT, load_settings, secret
from codebeat.ingest.bq import BigQueryGuard, human_bytes

PRE, POST = "20250915", "20251115"   # same days as the field audit
PRUNE_RATIO = 0.5   # filtered must scan < 50% of unfiltered to count as pruning


def main():
    s = load_settings()
    client = bigquery.Client(project=secret(s["bigquery"]["project_env"]))
    guard = BigQueryGuard(client, REPO_ROOT / s["bigquery"]["byte_log"],
                          s["bigquery"]["max_bytes_without_approval"])
    repos = s["bigquery"]["dry_run_repos"]
    name_list = ", ".join(f"'{r}'" for r in repos)
    t0 = s["study"]["t0"].replace("-", "")
    end = s["study"]["data_end"].replace("-", "")

    meta = []
    for day in (PRE, POST):
        t = client.get_table(f"githubarchive.day.{day}")
        meta.append((day, t.num_bytes, t.num_rows, t.clustering_fields, t.time_partitioning))

    cols_payload = "type, repo.id AS repo_id, actor.login, created_at, payload"
    cols_light = "type, repo.id AS repo_id, actor.login, created_at"
    where = f"WHERE repo.name IN ({name_list})"
    cases = []
    for day in (PRE, POST):
        tbl = f"`githubarchive.day.{day}`"
        cases += [
            (day, "payload, no filter", f"SELECT {cols_payload} FROM {tbl}"),
            (day, "payload, repo.name filter", f"SELECT {cols_payload} FROM {tbl} {where}"),
            (day, "no payload, repo.name filter", f"SELECT {cols_light} FROM {tbl} {where}"),
        ]
    span = f"`githubarchive.day.*` WHERE _TABLE_SUFFIX BETWEEN '{t0}' AND '{end}' AND repo.name IN ({name_list})"
    cases += [
        (f"{t0}-{end}", "payload, repo.name filter (45-month span)", f"SELECT {cols_payload} FROM {span}"),
        (f"{t0}-{end}", "no payload, repo.name filter (45-month span)", f"SELECT {cols_light} FROM {span}"),
    ]
    results = [(d, label, guard.dry_run(sql, f"D2 {d} {label}")) for d, label, sql in cases]

    by = {(d, l): b for d, l, b in results}
    verdicts = []
    for day in (PRE, POST):
        f, u = by[(day, "payload, repo.name filter")], by[(day, "payload, no filter")]
        verdicts.append((day, f, u, f < PRUNE_RATIO * u))
    prunes = all(v[3] for v in verdicts)
    span_payload = by[(f"{t0}-{end}", "payload, repo.name filter (45-month span)")]
    free_tb = s["bigquery"]["free_tier_bytes_per_month"]

    md = ["# BigQuery dry-run (D2 confirmation)", "",
          f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by `scripts/02_bq_dry_run.py`. "
          f"Dry runs only: **0 bytes billed** by this script.", "",
          f"Filter: `repo.name IN (...)` with {len(repos)} known repos: {', '.join(repos)}.",
          "On-demand billing equals bytes processed (rounded up to 1 MB, minimum 10 MB per table), "
          "so the dry-run figure is the billing estimate.", "",
          "## Table layout", "", "| Table | Size | Rows | Clustering | Partitioning |", "|---|---|---|---|---|"]
    md += [f"| day.{d} | {human_bytes(b)} | {r:,} | {c or 'none'} | {p or 'none'} |" for d, b, r, c, p in meta]
    md += ["", "## Bytes processed", "", "| Table(s) | Query | Bytes | Human |", "|---|---|---|---|"]
    md += [f"| {d} | {l} | {b:,} | {human_bytes(b)} |" for d, l, b in results]
    md += ["", "## Pruning check", "", "| Day | Filtered | Unfiltered | Ratio | Prunes? |", "|---|---|---|---|---|"]
    md += [f"| {d} | {human_bytes(f)} | {human_bytes(u)} | {f / u:.3f} | {'yes' if ok else 'no'} |"
           for d, f, u, ok in verdicts]
    md += ["", "## Decision", "",
           f"- Filter prunes: **{'yes' if prunes else 'no'}**.",
           f"- Reading `payload` for the 45-month span would scan **{human_bytes(span_payload)}** "
           f"({span_payload / free_tb:.1f}x the 1 TB monthly free tier), regardless of repo count.",
           "- Per D2: " + ("filter prunes, so GH Archive stays an option for PR/issue/review/comment data."
                          if prunes else
                          "filter does not prune, so **GraphQL is used throughout for PR, issue, review and "
                          "comment data**. BigQuery is kept only for cheap-column queries, each dry-run first."),
           ""]
    (REPO_ROOT / "reports/bq_dry_run.md").write_text("\n".join(md))
    print("\n".join(md))


if __name__ == "__main__":
    main()
