from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from server.config import ServerSettings

REPO = Path(__file__).resolve().parents[2]


def test_api_key_is_mandatory_and_must_be_long(monkeypatch):
    for var in ("SERVER_API_KEY", "POS_WRITE_MODE"):
        monkeypatch.delenv(var, raising=False)  # don't let the ambient environment decide
    with pytest.raises(ValidationError):
        ServerSettings(_env_file=None)
    with pytest.raises(ValidationError):
        ServerSettings(server_api_key="short", _env_file=None)
    assert ServerSettings(server_api_key="x" * 16, _env_file=None).pos_write_mode == "dry_run"  # safe default


def test_shared_env_file_with_server_keys_does_not_break_the_etl_settings(tmp_path):
    """The ETL's Settings used to reject unknown .env keys; the server adds its own."""
    (tmp_path / ".env").write_text(
        "POS_BASE_URL=https://pos.example\nPOS_USERNAME=u\nPOS_PASSWORD=p\n"
        "SUPABASE_URL=https://x.supabase.co\nSUPABASE_KEY=k\n"
        "SERVER_API_KEY=" + "a" * 20 + "\nPOS_WRITE_MODE=dry_run\n"
    )
    code = "from pos4africa.config.settings import settings; print('ok', settings.pos_username)"
    out = subprocess.run(
        [sys.executable, "-I", "-c", code], cwd=tmp_path, capture_output=True, text=True,
        env={"PYTHONPATH": str(REPO), "PATH": "/usr/bin:/bin"},
    )
    # -I ignores PYTHONPATH, so go through a tiny runner instead
    if "No module named 'pos4africa'" in out.stderr:
        out = subprocess.run(
            [sys.executable, "-c", f"import sys; sys.path.insert(0, {str(REPO)!r}); {code}"],
            cwd=tmp_path, capture_output=True, text=True, env={"PATH": "/usr/bin:/bin"},
        )
    assert out.returncode == 0 and "ok u" in out.stdout, out.stderr
