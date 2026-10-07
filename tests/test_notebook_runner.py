"""Offline notebook checks: no dbt process or Databricks writes are performed."""

import contextlib
import io
import json
from pathlib import Path
import runpy
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "databricks/notebooks/run_dbt_build.py"
WATERMARKS = {
    f"{source}_watermark_{bound}": timestamp
    for source in ("sales", "customer", "product")
    for bound, timestamp in (
        ("start", "2026-10-01T00:00:00Z"),
        ("end", "2026-10-02T00:00:00Z"),
    )
}


class Widgets:
    def __init__(self, values):
        self.values = values.copy()

    def text(self, name, default):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


class NotebookRunnerTests(unittest.TestCase):
    def execute(self, values, return_code=0, cwd=ROOT):
        process = SimpleNamespace(stdout=iter(["mock dbt output\n"]),
                                  wait=lambda: return_code)
        with (
            patch("os.environ", {"DBT_ACCESS_TOKEN": "offline-test-token"}),
            patch("pathlib.Path.cwd", return_value=cwd),
            patch("pathlib.Path.mkdir"),
            patch("shutil.which", return_value="/mock/dbt"),
            patch("subprocess.run", return_value=SimpleNamespace(
                stdout="mock dbt version\n", returncode=0)),
            patch("subprocess.Popen", return_value=process) as launch,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            runpy.run_path(str(NOTEBOOK), init_globals={
                "dbutils": SimpleNamespace(widgets=Widgets(values)),
            })
            return launch.call_args

    def test_explicit_project_root_and_six_variables(self):
        call = self.execute({**WATERMARKS, "project_root": str(ROOT)},
                            cwd=Path("/unrelated/notebook"))
        command = call.args[0]
        self.assertEqual(call.kwargs["cwd"], ROOT)
        self.assertEqual(json.loads(command[command.index("--vars") + 1]),
                         WATERMARKS)
        self.assertEqual(command[command.index("--target") + 1], "job")
        self.assertEqual(command[command.index("--select") + 1],
                         "pentaho_project_1")
        self.assertNotIn("shell", call.kwargs)

    def test_legacy_parent_directory_discovery(self):
        call = self.execute(WATERMARKS, cwd=NOTEBOOK.parent)
        self.assertEqual(call.kwargs["cwd"], ROOT)

    def test_invalid_explicit_root_fails_without_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "not a complete dbt project"):
                self.execute({**WATERMARKS, "project_root": directory})

    def test_dbt_failure_propagates(self):
        with self.assertRaisesRegex(RuntimeError, "exit code 2"):
            self.execute(WATERMARKS, return_code=2)

    def test_retry_reuses_original_window(self):
        with self.assertRaises(RuntimeError):
            self.execute(WATERMARKS, return_code=2)
        call = self.execute(WATERMARKS)
        command = call.args[0]
        self.assertEqual(json.loads(command[command.index("--vars") + 1]),
                         WATERMARKS)

    def test_missing_and_backward_windows_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "Missing required"):
            self.execute({})
        with self.assertRaisesRegex(ValueError, "moves backwards"):
            self.execute({**WATERMARKS,
                          "sales_watermark_end": "2026-09-01T00:00:00Z"})


if __name__ == "__main__":
    unittest.main()
