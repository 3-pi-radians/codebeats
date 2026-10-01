import re
from datetime import date

from codebeat.config import DEFAULT_SETTINGS, load_settings


def test_span_is_45_months_and_before_regime_change():
    s = load_settings()["study"]
    t0, end = date.fromisoformat(s["t0"]), date.fromisoformat(s["data_end"])
    assert (end.year - t0.year) * 12 + end.month - t0.month + 1 == 45
    assert end < date.fromisoformat(s["regime_change_date"])


def test_settings_contain_no_secret_values():
    text = DEFAULT_SETTINGS.read_text()
    assert not re.search(r"gh[pousr]_[A-Za-z0-9]{20,}|github_pat_", text)
    assert not re.search(r"(token|key|secret|salt)\s*:\s*\S", text, re.I)  # only *_env names allowed
