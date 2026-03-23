"""
SPA-104: conftest for Python binding e2e tests.

Attempts to build the sparrowdb extension if it is not yet importable.
"""

import os
import subprocess
import sys


def pytest_configure(config):
    """Build the Python binding before running tests if needed."""
    try:
        import sparrowdb  # noqa: F401
    except ImportError:
        repo_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        python_crate = os.path.join(repo_root, "crates", "sparrowdb-python")
        subprocess.run(
            [sys.executable, "-m", "maturin", "develop", "--release"],
            cwd=python_crate,
            check=False,
        )
