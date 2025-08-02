from pathlib import Path
import subprocess
import pytest

from src import kstrike, tasks


def dummy_parse_ual(mdb_path: str) -> str:
    """
    Dummy parser function to simulate successful output.
    """
    return f"Parsed output for {mdb_path} || SUCCESS"


def dummy_run(cmd, **kwargs):
    """
    Dummy subprocess.run function.
    Extracts the MDB file path from the command (assumed to be the last element)
    and returns a CompletedProcess with dummy stdout from dummy_parse_ual.
    """
    mdb_path = cmd[-1]
    stdout_text = dummy_parse_ual(mdb_path)
    # Simulate successful process execution (returncode 0)
    return subprocess.CompletedProcess(cmd, 0, stdout=stdout_text, stderr="")


def test_command_parsing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the worker's 'command' task function using a sample MDB file.

    This test monkeypatches subprocess.run to simulate the call to the KStrike parser.
    It then calls the task with a sample input file and checks that an output text file is
    created under a temporary output directory and its contents match the expected output.
    """
    # Override subprocess.run with our dummy_run
    monkeypatch.setattr(subprocess, "run", dummy_run)

    # Create a dummy 'self' object for the task (celery passes its own context here)
    dummy_self = object()

    # Set up input file info pointing to the sample MDB file
    sample_mdb_path = str((Path("tests") / "Sample_UAL" / "Current.mdb").resolve())
    input_files = [
        {
            "display_name": "Current",
            "path": sample_mdb_path,
        }
    ]

    # Create a temporary output directory
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    workflow_id = "test-workflow"

    # Call the task function using .run() to bypass celery's self binding
    result = tasks.command.run(
        pipe_result=None,
        input_files=input_files,
        output_path=str(output_dir),
        workflow_id=workflow_id,
        task_config={},
    )

    # Check the task result is a string (returned by create_task_result)
    assert isinstance(result, str), "The task result should be a string."

    # There should be exactly one output file in the output directory
    output_files = list(output_dir.glob("*.txt"))
    assert len(output_files) == 1, "Expected one output file to be created."

    # Verify that the output file contains the expected parsed content
    content = output_files[0].read_text()
    expected_text = f"Parsed output for {sample_mdb_path} || SUCCESS"
    assert expected_text in content, (
        f"Output file content does not match. Expected to find: {expected_text}"
    )
