from __future__ import annotations

import os
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def test_import_shapefiles_no_src_imports():
    """scripts/import_shapefiles.py must not import from src/ modules."""
    script_path = SCRIPTS_DIR / "import_shapefiles.py"
    if not script_path.exists():
        pytest.skip("scripts/import_shapefiles.py not yet created")

    content = script_path.read_text()
    # Check actual import lines, not docstrings/comments
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'"):
            continue
        assert not stripped.startswith("from src"), (
            "import_shapefiles.py must be standalone — no imports from src/"
        )
        assert not stripped.startswith("import src"), (
            "import_shapefiles.py must be standalone — no imports from src/"
        )


def test_generate_mock_data_no_src_imports():
    """scripts/generate_mock_data.py must not import from src/ modules."""
    script_path = SCRIPTS_DIR / "generate_mock_data.py"
    if not script_path.exists():
        pytest.skip("scripts/generate_mock_data.py not yet created")

    content = script_path.read_text()
    # Check actual import lines, not docstrings/comments
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'"):
            continue
        assert not stripped.startswith("from src"), (
            "generate_mock_data.py must be standalone — no imports from src/"
        )
        assert not stripped.startswith("import src"), (
            "generate_mock_data.py must be standalone — no imports from src/"
        )
