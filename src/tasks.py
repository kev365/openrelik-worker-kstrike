import subprocess
from typing import Optional, List, Dict, Any
# API docs - https://openrelik.github.io/openrelik-worker-common/openrelik_worker_common/index.html
from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.logging import Logger
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery
from celery import signals

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-kstrike.tasks.ual-parse"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "openrelik-worker-kstrike",
    "description": "A stand-alone parser for User Access Logging from Server 2012 and newer systems.",
    # Configuration that will be rendered as a web UI for input configuration.
    "task_config": [
        {
            "name": "UAL mdb file parser",
            "label": "Select a current.mdb or GUID.mdb file to parse",
            "description": "Select a MDB file to parse.",
            "type": "textarea",  # Supported types: text, textarea, checkbox.
            "required": True,
        },
    ],
}

log = Logger()
logger = log.get_logger(__name__)

@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_) -> None:
    """
    Signal handler before task execution to bind log context.

    Args:
        sender: The signal sender.
        task_id: The ID of the task.
        task: The task instance.
        args: Positional arguments.
        kwargs: Keyword arguments.
        **_: Additional parameters.
    """
    log.bind(task_id=task_id, task_name=task.name, worker_name=TASK_METADATA.get("display_name"))

@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def command(
    self,
    pipe_result: Optional[str] = None,
    input_files: Optional[List[Dict[str, Any]]] = None,
    output_path: Optional[str] = None,
    workflow_id: Optional[str] = None,
    task_config: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Run the KStrike.py on input files.

    Args:
        pipe_result (Optional[str]): Base64-encoded result from the previous Celery task, if any.
        input_files (Optional[List[Dict[str, Any]]]): List of input file dictionaries.
        output_path (Optional[str]): Path to the output directory.
        workflow_id (Optional[str]): ID of the workflow.
        task_config (Optional[Dict[str, Any]]): User configuration for the task.

    Returns:
        str: Base64-encoded dictionary containing task results.
    """
    # Setup logger with workflow context
    log.bind(workflow_id=workflow_id)
    logger.debug(f"Starting {TASK_NAME} for workflow {workflow_id}")

    input_files_list = get_input_files(pipe_result, input_files or [])
    output_files = []
    # Set the command to call the KStrike parser located at src/kstrike.py
    base_command = ["python", "src/kstrike.py"]
    base_command_string = " ".join(base_command)

    for input_file in input_files_list:
        output_file = create_output_file(
            output_path,
            display_name=input_file.get("display_name"),
            extension="txt",
            data_type="ual_log",
        )
        command_line = base_command + [input_file.get("path")]
        result = subprocess.run(command_line, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"KStrike parser failed: {result.stderr}")
        with open(output_file.path, "w") as fh:
            fh.write(result.stdout)
        output_files.append(output_file.to_dict())

    if not output_files:
        raise RuntimeError("KStrike parser did not produce any output files.")

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=base_command_string,
        meta={},
    )