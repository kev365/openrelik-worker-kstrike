from pathlib import Path
import pytest

from src import tasks


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
                "Max rows per file": None,
            },
        )
        assert isinstance(result, str)


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
            task_config={"Max rows per file": "2"},
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
            task_config={"Max rows per file": "0"},
        )
        output_files = list(self.output_dir.glob("*.txt"))
        assert len(output_files) == 1
