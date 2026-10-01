"""BigQuery cost guard: every query is dry-run first, capped, and logged.

Free alternative for looking at rows: `preview_rows` uses the tabledata.list API,
which BigQuery does not bill. It cannot filter, so it is for field audits only.
"""
from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path

LOG_FIELDS = ["timestamp_utc", "purpose", "kind", "table_or_api", "bytes_processed",
              "bytes_billed", "cap_bytes", "status", "sql_sha1"]


class OverCapError(RuntimeError):
    pass


def human_bytes(n: int | None) -> str:
    if n is None:
        return "n/a"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n) < 1000:
            return f"{n:.1f} {unit}"
        n /= 1000
    return f"{n:.1f} PB"


class BigQueryGuard:
    def __init__(self, client, log_path: Path | str, cap_bytes: int):
        self.client = client
        self.log_path = Path(log_path)
        self.cap_bytes = cap_bytes

    def _log(self, purpose, kind, target, processed, billed, status, sql=""):
        new = not self.log_path.exists()
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.log_path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
            if new:
                w.writeheader()
            w.writerow({
                "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "purpose": purpose, "kind": kind, "table_or_api": target,
                "bytes_processed": processed if processed is not None else "",
                "bytes_billed": billed if billed is not None else "",
                "cap_bytes": self.cap_bytes, "status": status,
                "sql_sha1": hashlib.sha1(sql.encode()).hexdigest()[:12] if sql else "",
            })

    def dry_run(self, sql: str, purpose: str) -> int:
        """Return bytes the query would process. Dry runs are free."""
        from google.cloud import bigquery
        cfg = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self.client.query(sql, job_config=cfg)
        processed = int(job.total_bytes_processed)
        self._log(purpose, "dry_run", "query", processed, 0, "ok", sql)
        return processed

    def run(self, sql: str, purpose: str, allow_over_cap: bool = False):
        """Dry-run, refuse above the cap unless explicitly approved, then run and log billed bytes."""
        from google.cloud import bigquery
        processed = self.dry_run(sql, purpose)
        if processed > self.cap_bytes and not allow_over_cap:
            self._log(purpose, "query", "query", processed, None, "refused_over_cap", sql)
            raise OverCapError(
                f"{purpose}: would process {human_bytes(processed)} > cap "
                f"{human_bytes(self.cap_bytes)}. Ask before running.")
        # maximum_bytes_billed makes BigQuery itself enforce the limit as a second line of defence
        limit = processed if allow_over_cap else self.cap_bytes
        cfg = bigquery.QueryJobConfig(maximum_bytes_billed=max(limit, 10_485_760) * 2)
        job = self.client.query(sql, job_config=cfg)
        df = job.result().to_dataframe()
        self._log(purpose, "query", "query", job.total_bytes_processed,
                  job.total_bytes_billed, "ok", sql)
        return df

    def preview_rows(self, table: str, fields: list[str], max_results: int, purpose: str):
        """Read rows via tabledata.list (not billed). No filtering possible."""
        tbl = self.client.get_table(table)
        schema = [f for f in tbl.schema if f.name in fields]
        rows = self.client.list_rows(tbl, selected_fields=schema, max_results=max_results)
        df = rows.to_dataframe()
        self._log(purpose, "tabledata.list", table, 0, 0, f"ok rows={len(df)}")
        return df

    def iter_rows(self, table: str, fields: list[str], purpose: str, max_rows: int,
                  page_size: int = 10_000):
        """Stream rows via tabledata.list (not billed) until the caller stops or max_rows is reached."""
        tbl = self.client.get_table(table)
        schema = [f for f in tbl.schema if f.name in fields]
        n = 0
        try:
            for row in self.client.list_rows(tbl, selected_fields=schema,
                                             max_results=max_rows, page_size=page_size):
                n += 1
                yield row
        finally:
            self._log(purpose, "tabledata.list", table, 0, 0, f"ok rows={n}")

    def month_total_billed(self, month: str | None = None) -> int:
        """Sum of bytes billed in the log for a YYYY-MM month (default: current month)."""
        month = month or datetime.now(timezone.utc).strftime("%Y-%m")
        if not self.log_path.exists():
            return 0
        with open(self.log_path) as f:
            return sum(int(r["bytes_billed"] or 0) for r in csv.DictReader(f)
                       if r["timestamp_utc"].startswith(month) and r["kind"] == "query")
