import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src import tasks
from src import ual_timeliner as ual_timeliner_module
from src.ual_timeliner import TimelineEvent


DUMMY_OUTPUT_A = (
    "RoleGuid (RoleName)||TenantId||TotalAccesses||InsertDate||LastAccess"
    "||RawAddress||ConvertedAddress (Correlated_HostName(s))"
    "||AuthenticatedUserName||DatesAndAccesses||\r\n"
    "{10A9226F-50EE-49D8-A393-9A501D47CE04} (File Server)"
    "||{00000000-0000-0000-0000-000000000000}||1"
    "||2021-07-04 16:28:06.547340||2021-07-04 16:28:06.547340"
    "||0A0000F0||10.0.0.240 (No Match)||user_a ||2021-07-04: 1, ||\r\n"
)

DUMMY_OUTPUT_B = (
    "RoleGuid (RoleName)||TenantId||TotalAccesses||InsertDate||LastAccess"
    "||RawAddress||ConvertedAddress (Correlated_HostName(s))"
    "||AuthenticatedUserName||DatesAndAccesses||\r\n"
    # Same row as A (will be deduped)
    "{10A9226F-50EE-49D8-A393-9A501D47CE04} (File Server)"
    "||{00000000-0000-0000-0000-000000000000}||1"
    "||2021-07-04 16:28:06.547340||2021-07-04 16:28:06.547340"
    "||0A0000F0||10.0.0.240 (No Match)||user_a ||2021-07-04: 1, ||\r\n"
    # Unique row
    "{D6256CF7-98FB-4EB4-AA18-303F1DA1F770} (Web Server)"
    "||{00000000-0000-0000-0000-000000000000}||3"
    "||2021-08-01 10:00:00.000000||2021-08-01 12:00:00.000000"
    "||0A000001||10.0.0.1 (No Match)||user_b ||2021-08-01: 3, ||\r\n"
)


def dummy_parse_mdb(mdb_path: str) -> str:
    if "second" in mdb_path.lower() or "guid" in mdb_path.lower():
        return DUMMY_OUTPUT_B
    return DUMMY_OUTPUT_A


@pytest.fixture
def _patch_parser(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(tasks, "parse_mdb", dummy_parse_mdb)


class TestCommandParsing:
    """Tests for the KStrike worker command task."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path: Path, _patch_parser):
        self.output_dir = tmp_path / "output"
        self.output_dir.mkdir()
        self.sample_mdb = str((Path("tests") / "Sample_UAL" / "Current.mdb").resolve())

    def _run(self, input_files, task_config=None):
        return tasks.command.run(
            pipe_result=None,
            input_files=input_files,
            output_path=str(self.output_dir),
            workflow_id="test-workflow",
            task_config=task_config or {},
        )

    def test_basic_parsing(self):
        result = self._run([{"display_name": "Current.mdb", "path": self.sample_mdb}])
        assert isinstance(result, str)

        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 1

        content = output_files[0].read_text()
        assert "RoleGuid (RoleName)" in content
        assert "File Server" in content

    def test_output_prefix(self):
        self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={"File prefix": "SERVER01"},
        )
        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 1
        assert output_files[0].stat().st_size > 0

    def test_non_matching_files_rejected(self):
        """Non-.mdb and non-.txt files should be filtered; if nothing remains, raise."""
        with pytest.raises(RuntimeError, match="No .mdb or .txt files found"):
            self._run([{"display_name": "notes.csv", "path": "/tmp/notes.csv"}])

    def test_failed_file_does_not_crash_others(self, monkeypatch):
        """A bad file should be reported, not crash the whole task."""
        original = dummy_parse_mdb

        def mixed_parser(path):
            if "bad" in path.lower():
                raise Exception("Not a UAL database")
            return original(path)

        monkeypatch.setattr(tasks, "parse_mdb", mixed_parser)

        result = self._run([
            {"display_name": "Current.mdb", "path": self.sample_mdb},
            {"display_name": "Bad.mdb", "path": "/tmp/bad.mdb"},
        ])
        assert isinstance(result, str)
        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 1

    def test_combine_and_dedupe(self):
        """Combine mode should produce one file with duplicates removed."""
        result = self._run(
            [
                {"display_name": "Current.mdb", "path": self.sample_mdb},
                {"display_name": "Second.mdb", "path": "/tmp/second.mdb"},
            ],
            task_config={"Combine & dedup": True},
        )
        assert isinstance(result, str)

        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 1

        content = output_files[0].read_text()
        lines = [l for l in content.splitlines() if l.strip()]

        assert lines[0].startswith("RoleGuid (RoleName)||")
        assert sum(1 for l in lines if "user_a" in l) == 1
        assert sum(1 for l in lines if "user_b" in l) == 1
        assert len(lines) == 3

    def test_combine_with_prefix(self):
        self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={"File prefix": "DC01", "Combine & dedup": True},
        )
        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 1
        assert output_files[0].stat().st_size > 0

    def test_none_config_values_handled(self):
        """UI sends None for unconfigured fields — should not crash."""
        result = self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={
                "File prefix": None,
                "Combine & dedup": None,
                "Output split enabled": None,
            },
        )
        assert isinstance(result, str)

    def test_per_file_split(self, monkeypatch):
        """Splitting should work on individual file output, not just combined."""
        header = (
            "RoleGuid (RoleName)||TenantId||TotalAccesses||InsertDate||LastAccess"
            "||RawAddress||ConvertedAddress||AuthenticatedUserName||DatesAndAccesses||\r\n"
        )
        rows = "".join(f"row_{i}||tenant||1||2021-01-01||2021-01-01||addr||addr||user||dates||\r\n" for i in range(5))

        monkeypatch.setattr(tasks, "parse_mdb", lambda path: header + rows)

        result = self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={"Output split enabled": "2"},
        )
        assert isinstance(result, str)

        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 3  # 5 rows / 2 per file = 3 files
        for f in output_files:
            content = f.read_text()
            assert content.startswith("RoleGuid (RoleName)||")


class TestCombineOnly:
    """Tests for combine-only mode with .txt input files."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path: Path):
        self.output_dir = tmp_path / "output"
        self.output_dir.mkdir()
        self.tmp_path = tmp_path

        self.txt_a = tmp_path / "server_a.txt"
        self.txt_a.write_text(DUMMY_OUTPUT_A, encoding="utf-8")

        self.txt_b = tmp_path / "server_b.txt"
        self.txt_b.write_text(DUMMY_OUTPUT_B, encoding="utf-8")

    def _run(self, input_files, task_config=None):
        return tasks.command.run(
            pipe_result=None,
            input_files=input_files,
            output_path=str(self.output_dir),
            workflow_id="test-workflow",
            task_config=task_config or {},
        )

    def test_combine_only_auto_detected(self):
        """When only .txt files are provided, combine-only mode activates automatically."""
        result = self._run([
            {"display_name": "server_a.txt", "path": str(self.txt_a)},
            {"display_name": "server_b.txt", "path": str(self.txt_b)},
        ])
        assert isinstance(result, str)

        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 1

        content = output_files[0].read_text()
        lines = [l for l in content.splitlines() if l.strip()]
        assert lines[0].startswith("RoleGuid (RoleName)||")
        assert sum(1 for l in lines if "user_a" in l) == 1
        assert sum(1 for l in lines if "user_b" in l) == 1
        assert len(lines) == 3

    def test_combine_only_rejects_non_kstrike_txt(self):
        """A .txt file without KStrike header should be flagged as failed."""
        bad_txt = self.tmp_path / "random.txt"
        bad_txt.write_text("this is not kstrike output\n", encoding="utf-8")

        result = self._run([
            {"display_name": "server_a.txt", "path": str(self.txt_a)},
            {"display_name": "random.txt", "path": str(bad_txt)},
        ])
        assert isinstance(result, str)
        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 1

    def test_combine_only_with_prefix(self):
        self._run(
            [{"display_name": "server_a.txt", "path": str(self.txt_a)}],
            task_config={"File prefix": "CASE01"},
        )
        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 1
        assert output_files[0].stat().st_size > 0

    def test_split_by_max_rows(self):
        """When max rows is exceeded, output should be split into multiple files."""
        # Create a file with 5 unique data rows
        header = "RoleGuid (RoleName)||TenantId||Data||\r\n"
        rows = "".join(f"row_{i}||tenant||data||\r\n" for i in range(5))
        big_txt = self.tmp_path / "big.txt"
        big_txt.write_text(header + rows, encoding="utf-8")

        result = self._run(
            [{"display_name": "big.txt", "path": str(big_txt)}],
            task_config={"Output split enabled": "2"},
        )
        assert isinstance(result, str)

        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 3  # 5 rows / 2 per file = 3 files

        # Each file should have the header
        for f in output_files:
            content = f.read_text()
            assert content.startswith("RoleGuid (RoleName)||")

    def test_no_split_when_unlimited(self):
        """Max rows = 0 means no splitting."""
        header = "RoleGuid (RoleName)||TenantId||Data||\r\n"
        rows = "".join(f"row_{i}||tenant||data||\r\n" for i in range(10))
        txt = self.tmp_path / "many.txt"
        txt.write_text(header + rows, encoding="utf-8")

        self._run(
            [{"display_name": "many.txt", "path": str(txt)}],
            task_config={"Output split enabled": "0"},
        )
        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 1


# --- UAL Timeliner Task Tests ---

DUMMY_TIMELINE_EVENTS_A = [
    TimelineEvent(
        timestamp=datetime(2021, 7, 4, 16, 28, 6, 547340, tzinfo=timezone.utc),
        timestamp_description="InsertDate",
        source_table="CLIENTS",
        source_file=Path("/tmp/Current.mdb"),
        role_guid="{10A9226F-50EE-49D8-A393-9A501D47CE04}",
        role_name="File Server",
        authenticated_user="DOMAIN\\user_a",
        ip_address="10.0.0.240",
        user="user_a",
        total_accesses=1,
    ),
]

DUMMY_TIMELINE_EVENTS_B = [
    # Same key fields as event A (will be deduped).
    TimelineEvent(
        timestamp=datetime(2021, 7, 4, 16, 28, 6, 547340, tzinfo=timezone.utc),
        timestamp_description="InsertDate",
        source_table="CLIENTS",
        source_file=Path("/tmp/GUID.mdb"),
        role_guid="{10A9226F-50EE-49D8-A393-9A501D47CE04}",
        role_name="File Server",
        authenticated_user="DOMAIN\\user_a",
        ip_address="10.0.0.240",
        user="user_a",
        total_accesses=1,
    ),
    # Unique event.
    TimelineEvent(
        timestamp=datetime(2021, 8, 1, 10, 0, 0, tzinfo=timezone.utc),
        timestamp_description="InsertDate",
        source_table="CLIENTS",
        source_file=Path("/tmp/GUID.mdb"),
        role_guid="{D6256CF7-98FB-4EB4-AA18-303F1DA1F770}",
        role_name="Web Server",
        authenticated_user="DOMAIN\\user_b",
        ip_address="10.0.0.1",
        user="user_b",
        total_accesses=3,
    ),
]


def dummy_read_mdb(path, anchor_preference="insert_then_last", full_output=False):
    """Return pre-built events based on the filename."""
    name = path.name.lower() if hasattr(path, "name") else str(path).lower()
    if "guid" in name or "second" in name:
        return list(DUMMY_TIMELINE_EVENTS_B)
    return list(DUMMY_TIMELINE_EVENTS_A)


@pytest.fixture
def _patch_ual_timeliner(monkeypatch):
    monkeypatch.setattr(ual_timeliner_module, "_read_mdb", dummy_read_mdb)


class TestUALTimelineCommand:
    """Tests for the UAL Timeliner worker command task."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path: Path, _patch_ual_timeliner):
        self.output_dir = tmp_path / "output"
        self.output_dir.mkdir()
        self.sample_mdb = str(
            (Path("tests") / "Sample_UAL" / "Current.mdb").resolve()
        )

    def _run(self, input_files, task_config=None):
        return tasks.ual_timeline_command.run(
            pipe_result=None,
            input_files=input_files,
            output_path=str(self.output_dir),
            workflow_id="test-workflow",
            task_config=task_config or {},
        )

    def test_basic_csv_output(self):
        result = self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
        )
        assert isinstance(result, str)

        output_files = list(self.output_dir.glob("*.csv"))
        assert len(output_files) == 1

        content = output_files[0].read_text()
        assert "timestamp (UTC)" in content
        assert "user_a" in content

    def test_output_prefix(self):
        self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={"File prefix": "SERVER01"},
        )
        output_files = list(self.output_dir.glob("*.csv"))
        assert len(output_files) == 1
        assert output_files[0].stat().st_size > 0

    def test_no_mdb_files_rejected(self):
        with pytest.raises(RuntimeError, match="No .mdb files found"):
            self._run(
                [{"display_name": "notes.csv", "path": "/tmp/notes.csv"}],
            )

    def test_failed_file_does_not_crash_others(self, monkeypatch):
        original = dummy_read_mdb

        def mixed_reader(path, **kwargs):
            name = path.name.lower() if hasattr(path, "name") else str(path).lower()
            if "bad" in name:
                raise Exception("Not a UAL database")
            return original(path, **kwargs)

        monkeypatch.setattr(ual_timeliner_module, "_read_mdb", mixed_reader)

        result = self._run([
            {"display_name": "Current.mdb", "path": self.sample_mdb},
            {"display_name": "Bad.mdb", "path": "/tmp/bad.mdb"},
        ])
        assert isinstance(result, str)
        output_files = list(self.output_dir.glob("*.csv"))
        assert len(output_files) == 1

    def test_dedup_enabled_by_default(self):
        """Two files with overlapping events — dedup should remove the duplicate."""
        result = self._run([
            {"display_name": "Current.mdb", "path": self.sample_mdb},
            {"display_name": "GUID.mdb", "path": "/tmp/guid.mdb"},
        ])
        assert isinstance(result, str)

        output_files = list(self.output_dir.glob("*.csv"))
        assert len(output_files) == 1

        content = output_files[0].read_text()
        lines = [l for l in content.splitlines() if l.strip()]
        # Header + 2 unique events (user_a duplicate removed).
        assert len(lines) == 3

    def test_dedup_disabled(self):
        """With dedup disabled, all rows (including duplicates) should be kept."""
        result = self._run(
            [
                {"display_name": "Current.mdb", "path": self.sample_mdb},
                {"display_name": "GUID.mdb", "path": "/tmp/guid.mdb"},
            ],
            task_config={"Dedup": "false"},
        )
        assert isinstance(result, str)

        output_files = list(self.output_dir.glob("*.csv"))
        content = output_files[0].read_text()
        lines = [l for l in content.splitlines() if l.strip()]
        # Header + 3 rows (1 from A + 2 from B, no dedup).
        assert len(lines) == 4

    def test_xlsx_format(self):
        self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={"Output format": ["xlsx"]},
        )
        output_files = list(self.output_dir.glob("*.xlsx"))
        assert len(output_files) == 1
        assert output_files[0].stat().st_size > 0

    def test_sqlite_format(self):
        self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={"Output format": ["sqlite"]},
        )
        output_files = list(self.output_dir.glob("*.sqlite"))
        assert len(output_files) == 1

        conn = sqlite3.connect(output_files[0])
        rows = conn.execute("SELECT COUNT(*) FROM timeline").fetchone()
        conn.close()
        assert rows[0] == 1

    def test_k2t_format(self):
        self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={"Output format": ["k2t"]},
        )
        output_files = list(self.output_dir.glob("*.jsonl"))
        assert len(output_files) == 1

        lines = output_files[0].read_text().strip().splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert "message" in record
        assert "datetime" in record
        assert "timestamp_desc" in record

    def test_parquet_format(self):
        import polars as pl

        self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={"Output format": ["parquet"]},
        )
        output_files = list(self.output_dir.glob("*.parquet"))
        assert len(output_files) == 1

        df = pl.read_parquet(output_files[0])
        assert df.height == 1
        assert "timestamp (UTC)" in df.columns

    def test_multi_format_output(self):
        """Selecting multiple formats should produce one output per format."""
        self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={"Output format": ["csv", "xlsx", "sqlite"]},
        )
        assert len(list(self.output_dir.glob("*.csv"))) == 1
        assert len(list(self.output_dir.glob("*.xlsx"))) == 1
        assert len(list(self.output_dir.glob("*.sqlite"))) == 1

    def test_csv_split(self):
        """Splitting should produce multiple CSV files when row count exceeds limit."""

        def many_events(path, **kwargs):
            return [
                TimelineEvent(
                    timestamp=datetime(2021, 1, 1, i, 0, 0, tzinfo=timezone.utc),
                    timestamp_description="InsertDate",
                    source_table="CLIENTS",
                    source_file=path,
                    authenticated_user=f"DOMAIN\\user_{i}",
                    ip_address="10.0.0.1",
                    user=f"user_{i}",
                    total_accesses=1,
                )
                for i in range(5)
            ]

        from tests import test_tasks as this_module
        this_module  # silence unused

        from unittest.mock import patch
        with patch.object(ual_timeliner_module, "_read_mdb", many_events):
            result = self._run(
                [{"display_name": "Current.mdb", "path": self.sample_mdb}],
                task_config={"Output split enabled": "2"},
            )
        assert isinstance(result, str)

        output_files = list(self.output_dir.glob("*.csv"))
        assert len(output_files) == 3  # 5 rows / 2 per file = 3 files

    def test_none_config_values_handled(self):
        """UI sends None for unconfigured fields — should not crash."""
        result = self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={
                "File prefix": None,
                "Output format": None,
                "Full output": None,
                "Dedup": None,
                "Output split enabled": None,
            },
        )
        assert isinstance(result, str)

    def test_string_format_fallback(self):
        """A single string (not list) for Output format should still work."""
        result = self._run(
            [{"display_name": "Current.mdb", "path": self.sample_mdb}],
            task_config={"Output format": "csv"},
        )
        assert isinstance(result, str)
        assert len(list(self.output_dir.glob("*.csv"))) == 1
