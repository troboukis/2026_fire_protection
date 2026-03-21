from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_fetch_copernicus_imports_as_package():
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-c", "import src.fetch_copernicus"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
