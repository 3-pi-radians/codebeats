"""Guard behaviour with a fake client: no credentials or network needed."""
import csv
from types import SimpleNamespace

import pytest

from codebeat.ingest.bq import BigQueryGuard, OverCapError


class FakeClient:
    def __init__(self, bytes_processed):
        self.bytes_processed = bytes_processed
        self.real_queries = 0

    def query(self, sql, job_config):
        if not job_config.dry_run:
            self.real_queries += 1
        return SimpleNamespace(total_bytes_processed=self.bytes_processed,
                               total_bytes_billed=self.bytes_processed,
                               result=lambda: SimpleNamespace(to_dataframe=lambda: "df"))


def rows(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def test_dry_run_is_logged_with_zero_billed(tmp_path):
    g = BigQueryGuard(FakeClient(5_000), tmp_path / "log.csv", cap_bytes=20_000)
    assert g.dry_run("SELECT 1", "t") == 5_000
    (r,) = rows(tmp_path / "log.csv")
    assert r["kind"] == "dry_run" and r["bytes_billed"] == "0"


def test_over_cap_refused_and_never_executed(tmp_path):
    client = FakeClient(30_000_000_000)
    g = BigQueryGuard(client, tmp_path / "log.csv", cap_bytes=20_000_000_000)
    with pytest.raises(OverCapError):
        g.run("SELECT payload FROM t", "big")
    assert client.real_queries == 0
    assert rows(tmp_path / "log.csv")[-1]["status"] == "refused_over_cap"


def test_under_cap_runs_and_counts_toward_month(tmp_path):
    client = FakeClient(1_000)
    g = BigQueryGuard(client, tmp_path / "log.csv", cap_bytes=20_000)
    assert g.run("SELECT 1", "small") == "df"
    assert client.real_queries == 1
    assert g.month_total_billed() == 1_000  # dry runs not counted
