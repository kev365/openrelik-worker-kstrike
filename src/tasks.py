import os
from typing import Optional, List, Dict, Any

from openrelik_worker_common.file_utils import (
    create_output_file,
    is_disk_image,
)
from openrelik_worker_common.logging import Logger
from openrelik_worker_common.reporting import Report, Priority, MarkdownTable
from openrelik_worker_common.task_utils import (
    create_task_result,
    get_input_files,
)

from .app import celery
from .kstrike import parse_mdb
from celery import signals

# --- KStrike Task ---

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-kstrike.tasks.ual-parse"

# Expected KStrike output header — used to validate .txt files for combine-only mode.
KSTRIKE_HEADER_PREFIX = "RoleGuid (RoleName)||"

# Filter to auto-select .mdb files from loose file inputs.
COMPATIBLE_INPUTS_MDB = {
    "filenames": ["*.mdb"],
}
COMPATIBLE_INPUTS_TXT = {
    "filenames": ["*.txt"],
}

# Default row limit per output file when splitting combined output.
DEFAULT_MAX_ROWS_PER_FILE = 500000

# Known UAL paths inside Windows disk images.
UAL_PATHS = [
    "Windows/System32/LogFiles/SUM",
    "windows/system32/logfiles/sum",
]

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "KStrike UAL Parser",
    "description": "Parses Windows Server User Access Logging (UAL) .mdb files using KStrike by Brian Moran.",
    "task_config": [
        {
            "name": "File prefix",
            "label": "Output filename prefix (optional)",
            "description": (
                "\U0001f4dd Set a prefix to name output files as <prefix>_filename.txt, or leave blank for filename.txt. "
                "Output is double-pipe '||' delimited, UTF-8 encoded text."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "Combine & dedup",
            "label": "Combine all output into a single deduplicated file.",
            "description": (
                "\U0001f4be When enabled, all parsed output is merged into one file with duplicate rows removed. "
                "Also accepts previously parsed KStrike .txt files for combine-only mode."
            ),
            "type": "checkbox",
            "required": False,
        },
        {
            "name": "Output split enabled",
            "label": "Max rows per output file (default 500000, 0 = no limit)",
            "description": (
                "\U0001f4d1 When combining, split the output into multiple files if the row count exceeds this limit. "
                "Each split file includes the header. Only affects output; all input is still fully loaded and deduped. "
                "Default: 500000. Set to 0 for no limit."
            ),
            "type": "text",
            "required": False,
        },
    ],
}

log = Logger()
logger = log.get_logger(__name__)

# Registry of task name -> display name, populated after each TASK_METADATA definition.
_TASK_DISPLAY_NAMES: dict[str, str] = {
    TASK_NAME: TASK_METADATA["display_name"],
}


def _safe_str_config(task_config: dict, key: str, default: str = "") -> str:
    """Get a string config value, handling None from the UI."""
    return (task_config.get(key, default) or "").strip()


def _safe_int_config(task_config: dict, key: str, default: int) -> int:
    """Parse an integer config value, returning default if empty/invalid/None."""
    raw = _safe_str_config(task_config, key)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_) -> None:
    log.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=_TASK_DISPLAY_NAMES.get(task.name, task.name),
    )


def _collect_mdb_files_from_image(input_file: dict, output_path: str) -> list[dict]:
    """Mount a disk image and return dicts for any UAL .mdb files found."""
    try:
        from openrelik_worker_common.mount_utils import BlockDevice
    except ImportError:
        logger.warning("mount_utils not available; skipping disk image %s", input_file.get("display_name"))
        return []

    mdb_files = []
    bd = BlockDevice(input_file.get("path"))
    try:
        bd.setup()
        mountpoints = bd.mount()
        for mp in mountpoints:
            for ual_path in UAL_PATHS:
                sum_dir = os.path.join(mp, ual_path)
                if not os.path.isdir(sum_dir):
                    continue
                for fname in os.listdir(sum_dir):
                    if fname.lower().endswith(".mdb"):
                        full_path = os.path.join(sum_dir, fname)
                        mdb_files.append({
                            "display_name": fname,
                            "path": full_path,
                            "id": input_file.get("id"),
                            "original_path": os.path.join(ual_path, fname),
                        })
    except Exception as e:
        logger.error("Failed to mount disk image %s: %s", input_file.get("display_name"), e)
    finally:
        try:
            bd.umount()
        except Exception:
            pass

    return mdb_files


def _combine_and_dedupe(parsed_outputs: list[str]) -> tuple[str, list[str], int]:
    """Combine multiple KStrike outputs, keeping one header and deduplicating data rows.

    Returns:
        Tuple of (header line or None, list of unique data rows, total unique row count).
    """
    header = None
    seen = set()
    unique_rows = []

    for output in parsed_outputs:
        for line in output.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith(KSTRIKE_HEADER_PREFIX):
                if header is None:
                    header = stripped
                continue
            if stripped not in seen:
                seen.add(stripped)
                unique_rows.append(stripped)

    return header, unique_rows, len(unique_rows)


def _write_combined_files(
    header: str,
    unique_rows: list[str],
    output_path: str,
    base_name: str,
    max_rows: int,
) -> list[dict]:
    """Write combined output, splitting into multiple files if max_rows > 0 and exceeded.

    Returns list of OutputFile dicts.
    """
    output_files = []

    if max_rows <= 0 or len(unique_rows) <= max_rows:
        # Single file
        parts = []
        if header:
            parts.append(header)
        parts.extend(unique_rows)
        content = "\r\n".join(parts) + "\r\n"

        out = create_output_file(
            output_path,
            display_name=base_name,
            extension="txt",
            data_type="openrelik:worker:kstrike:ual_log",
        )
        with open(out.path, "w", encoding="utf-8") as fh:
            fh.write(content)
        output_files.append(out.to_dict())
    else:
        # Split into chunks
        for chunk_idx, start in enumerate(range(0, len(unique_rows), max_rows), start=1):
            chunk = unique_rows[start:start + max_rows]
            parts = []
            if header:
                parts.append(header)
            parts.extend(chunk)
            content = "\r\n".join(parts) + "\r\n"

            chunk_name = f"{base_name}_part{chunk_idx}"
            out = create_output_file(
                output_path,
                display_name=chunk_name,
                extension="txt",
                data_type="openrelik:worker:kstrike:ual_log",
            )
            with open(out.path, "w", encoding="utf-8") as fh:
                fh.write(content)
            output_files.append(out.to_dict())

    return output_files


def _validate_kstrike_txt(file_path: str) -> bool:
    """Check if a .txt file looks like KStrike output by inspecting the first line."""
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            first_line = fh.readline().strip()
            return first_line.startswith(KSTRIKE_HEADER_PREFIX)
    except Exception:
        return False


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def command(
    self,
    pipe_result: Optional[str] = None,
    input_files: Optional[List[Dict[str, Any]]] = None,
    output_path: Optional[str] = None,
    workflow_id: Optional[str] = None,
    task_config: Optional[Dict[str, Any]] = None,
) -> str:
    """Run the KStrike UAL parser on input files.

    Handles three scenarios:
    1. .mdb files — parse with KStrike, optionally combine.
    2. .txt files only — combine-only mode (merge previous KStrike output).
    3. Disk images — mount and look for UAL files at known Windows paths.
    """
    log.bind(workflow_id=workflow_id)
    logger.debug("Starting %s for workflow %s", TASK_NAME, workflow_id)

    task_config = task_config or {}
    output_prefix = _safe_str_config(task_config, "File prefix")
    combine = bool(task_config.get("Combine & dedup", False))
    max_rows = _safe_int_config(task_config, "Output split enabled", DEFAULT_MAX_ROWS_PER_FILE)

    # Separate inputs by type: disk images, loose files.
    all_input_files = get_input_files(pipe_result or "", input_files or [])
    loose_files = []
    image_files = []
    for f in all_input_files:
        try:
            if is_disk_image(f):
                image_files.append(f)
            else:
                loose_files.append(f)
        except RuntimeError:
            loose_files.append(f)

    # Split loose files into .mdb and .txt categories.
    mdb_files = get_input_files("", loose_files, filter=COMPATIBLE_INPUTS_MDB)
    txt_files = get_input_files("", loose_files, filter=COMPATIBLE_INPUTS_TXT)

    # Add .mdb files found inside disk images.
    for img in image_files:
        mdb_files.extend(_collect_mdb_files_from_image(img, output_path or ""))

    # Determine operating mode.
    has_mdb = len(mdb_files) > 0
    has_txt = len(txt_files) > 0
    combine_only = not has_mdb and has_txt

    if not has_mdb and not has_txt:
        raise RuntimeError(
            "No .mdb or .txt files found. Provide UAL .mdb files to parse, "
            "or previously parsed KStrike .txt files to combine."
        )

    output_files = []
    failed_files = []
    parsed_outputs = []  # Strings to feed into combine
    parsed_file_names = []  # Track names for the report

    # --- Phase 1: Parse .mdb files (if any) ---
    if has_mdb:
        for input_file in mdb_files:
            file_display = input_file.get("display_name", "unknown")
            file_stem = os.path.splitext(file_display)[0]

            if output_prefix:
                out_name = f"{output_prefix}_{file_stem}"
            else:
                out_name = file_stem

            try:
                result = parse_mdb(input_file.get("path") or "")
            except Exception as e:
                logger.warning("KStrike failed on %s: %s", file_display, e)
                failed_files.append((file_display, str(e)))
                continue

            parsed_outputs.append(result)
            parsed_file_names.append(file_display)

            if not combine:
                header, rows, _ = _combine_and_dedupe([result])
                split_files = _write_combined_files(
                    header or "", rows, output_path or "", out_name, max_rows,
                )
                output_files.extend(split_files)

    # --- Phase 2: Read .txt files for combining ---
    if has_txt and (combine or combine_only):
        for tf in txt_files:
            file_display = tf.get("display_name", "unknown")
            file_path = tf.get("path", "")

            if not _validate_kstrike_txt(file_path):
                failed_files.append((
                    file_display,
                    "Not a valid KStrike output file (missing expected header)",
                ))
                continue

            try:
                with open(file_path, "r", encoding="utf-8") as fh:
                    parsed_outputs.append(fh.read())
                parsed_file_names.append(file_display)
            except Exception as e:
                logger.warning("Failed to read %s: %s", file_display, e)
                failed_files.append((file_display, str(e)))

    # --- Phase 3: Combine if requested or in combine-only mode ---
    total_rows = 0
    combined_name = ""
    if (combine or combine_only) and parsed_outputs:
        combined_name = f"{output_prefix}_combined_ual" if output_prefix else "combined_ual"
        header, unique_rows, total_rows = _combine_and_dedupe(parsed_outputs)

        combined_files = _write_combined_files(
            header or "",
            unique_rows,
            output_path or "",
            combined_name,
            max_rows,
        )
        output_files.extend(combined_files)

    # --- Build task report ---
    report = Report("KStrike UAL Parser Results")
    report.priority = Priority.INFO

    summary_parts = []
    if has_mdb:
        mdb_names = {f.get("display_name") for f in mdb_files}
        parsed_count = len([n for n in parsed_file_names if n in mdb_names])
        if parsed_count:
            summary_parts.append(f"{parsed_count} .mdb file(s) parsed")
    if has_txt and (combine or combine_only):
        mdb_names = {f.get("display_name") for f in mdb_files} if has_mdb else set()
        txt_count = len([n for n in parsed_file_names if n not in mdb_names])
        if txt_count:
            summary_parts.append(f"{txt_count} .txt file(s) included")
    if failed_files:
        summary_parts.append(f"{len(failed_files)} file(s) failed")
    if (combine or combine_only) and parsed_outputs:
        num_output = len([f for f in output_files
                         if f.get("data_type") == "openrelik:worker:kstrike:ual_log"])
        if combine:
            num_output = len(output_files) - (len(parsed_file_names) if not combine else 0)
        parts_note = f" across {len(combined_files)} file(s)" if len(combined_files) > 1 else ""
        summary_parts.append(f"{total_rows} unique rows combined{parts_note}")
    report.summary = ". ".join(summary_parts) + "." if summary_parts else "No files processed."

    section = report.add_section()
    section.add_header("Summary")

    results_table = MarkdownTable(["File", "Type", "Status"])
    failed_names = {name for name, _ in failed_files}
    for input_file in mdb_files:
        name = input_file.get("display_name", "unknown")
        if name in failed_names:
            err = next(e for n, e in failed_files if n == name)
            results_table.add_row([name, "MDB", f"Failed: {err}"])
        else:
            results_table.add_row([name, "MDB", "Parsed"])
    if has_txt and (combine or combine_only):
        for tf in txt_files:
            name = tf.get("display_name", "unknown")
            if name in failed_names:
                err = next(e for n, e in failed_files if n == name)
                results_table.add_row([name, "TXT", f"Skipped: {err}"])
            else:
                results_table.add_row([name, "TXT", "Included"])
    section.add_table(results_table)

    if (combine or combine_only) and parsed_outputs:
        section.add_header("Combined Output", level=3)
        if len(combined_files) > 1:
            section.add_paragraph(
                f"{total_rows} unique rows split across {len(combined_files)} files "
                f"({max_rows} rows per file)."
            )
        else:
            section.add_paragraph(
                f"All output merged into '{combined_name}.txt' "
                f"with {total_rows} unique data rows (duplicates removed)."
            )

    if failed_files:
        section.add_header("Failed Files", level=3)
        section.add_paragraph(
            "The following files could not be processed."
        )
        for name, err in failed_files:
            section.add_bullet(f"{name}: {err}")

    if not output_files:
        raise RuntimeError(
            "No output was produced. Check that the input files are valid "
            "UAL .mdb databases or KStrike .txt output files."
        )

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id or "",
        command="KStrike UAL Parser",
        meta={},
        task_report=report.to_dict(),
    )


# --- UAL Timeliner Task ---

UAL_TIMELINE_TASK_NAME = "openrelik-worker-kstrike.tasks.ual-timeline"

# Map output format choices to file extensions.
FORMAT_EXTENSIONS = {
    "csv": "csv",
    "xlsx": "xlsx",
    "sqlite": "sqlite",
    "parquet": "parquet",
    "k2t": "jsonl",
}

UAL_TIMELINE_TASK_METADATA = {
    "display_name": "UAL Timeliner",
    "description": (
        "Builds forensic timelines from Windows Server User Access Logging (UAL) "
        ".mdb files using UAL Timeliner by Kevin Stokes."
    ),
    "task_config": [
        {
            "name": "File prefix",
            "label": "Output filename prefix (optional)",
            "description": (
                "\U0001f4dd Set a prefix for output filenames, or leave blank for default naming."
            ),
            "type": "text",
            "required": False,
        },
        {
            "name": "Output format",
            "label": "Select output formats",
            "description": (
                "\U0001f5c2 Select one or more output formats. CSV is produced by default "
                "if none are selected. K2T produces Timesketch-compatible JSONL."
            ),
            "type": "autocomplete",
            "items": ["csv", "xlsx", "sqlite", "parquet", "k2t"],
            "required": False,
        },
        {
            "name": "Full output",
            "label": "Include all columns and parse Day### historical data.",
            "description": (
                "\U0001f4da When enabled, includes access_count, role_guid, tenant_id, "
                "client_name columns and parses Day### daily access data. "
                "Note: This greatly increases the row count."
            ),
            "type": "checkbox",
            "required": False,
        },
        {
            "name": "Dedup",
            "label": "Deduplication",
            "description": (
                "\U0001f4be Removes duplicate entries, preferring data from Current.mdb "
                "over archive databases. (Default: True)"
            ),
            "type": "select",
            "items": ["true", "false"],
            "required": False,
        },
        {
            "name": "Output split enabled",
            "label": "Max rows per output file (default 500000, 0 = no limit)",
            "description": (
                "\U0001f4d1 Split output into multiple files if the row count exceeds this limit. "
                "Applies to CSV, K2T, and XLSX formats. "
                "Set to 0 for no limit. (Default: 500000)"
            ),
            "type": "text",
            "required": False,
        },
    ],
}


_TASK_DISPLAY_NAMES[UAL_TIMELINE_TASK_NAME] = UAL_TIMELINE_TASK_METADATA["display_name"]


def _write_timeline_df(df, path, fmt):
    """Write a Polars DataFrame to the given path in the specified format."""
    from pathlib import Path as _Path
    from .ual_timeliner import _write_xlsx, _write_sqlite, _write_k2t_jsonl

    out = _Path(path)
    if fmt == "csv":
        df.write_csv(out)
    elif fmt == "parquet":
        df.write_parquet(out)
    elif fmt == "xlsx":
        _write_xlsx(df, out)
    elif fmt == "sqlite":
        _write_sqlite(df, out)
    elif fmt == "k2t":
        _write_k2t_jsonl(df, out)


@celery.task(bind=True, name=UAL_TIMELINE_TASK_NAME, metadata=UAL_TIMELINE_TASK_METADATA)
def ual_timeline_command(
    self,
    pipe_result: Optional[str] = None,
    input_files: Optional[List[Dict[str, Any]]] = None,
    output_path: Optional[str] = None,
    workflow_id: Optional[str] = None,
    task_config: Optional[Dict[str, Any]] = None,
) -> str:
    """Build forensic timelines from Windows UAL .mdb files.

    Groups all input .mdb files (loose or from disk images) and processes
    them together through UAL Timeliner, producing a sorted, optionally
    deduplicated timeline in the chosen output format.
    """
    from pathlib import Path
    from .ual_timeliner import _read_mdb, _deduplicate_timeline, TIMELINE_SCHEMA
    import polars as pl

    log.bind(workflow_id=workflow_id)
    logger.debug("Starting %s for workflow %s", UAL_TIMELINE_TASK_NAME, workflow_id)

    task_config = task_config or {}
    output_prefix = _safe_str_config(task_config, "File prefix")
    full_output = bool(task_config.get("Full output", False))
    dedup = _safe_str_config(task_config, "Dedup") or "true"
    enable_dedup = dedup.lower() != "false"
    max_rows = _safe_int_config(
        task_config, "Output split enabled", DEFAULT_MAX_ROWS_PER_FILE,
    )

    # Output format: autocomplete returns a list of selected formats.
    raw_formats = task_config.get("Output format")
    if isinstance(raw_formats, list) and raw_formats:
        formats = [f for f in raw_formats if f in FORMAT_EXTENSIONS]
    elif isinstance(raw_formats, str) and raw_formats.strip():
        formats = [raw_formats.strip()]
    else:
        formats = []
    if not formats:
        formats = ["csv"]

    # --- Collect .mdb input files ---
    all_input_files = get_input_files(pipe_result or "", input_files or [])
    loose_files = []
    image_files = []
    for f in all_input_files:
        try:
            if is_disk_image(f):
                image_files.append(f)
            else:
                loose_files.append(f)
        except RuntimeError:
            loose_files.append(f)

    mdb_files = get_input_files("", loose_files, filter=COMPATIBLE_INPUTS_MDB)

    for img in image_files:
        mdb_files.extend(_collect_mdb_files_from_image(img, output_path or ""))

    if not mdb_files:
        raise RuntimeError(
            "No .mdb files found. Provide UAL .mdb files to build a timeline."
        )

    # --- Process each .mdb with UAL Timeliner ---
    # SystemIdentity.mdb does not contain CLIENTS/DNS tables — skip it.
    SKIPPED_MDB_NAMES = {"systemidentity.mdb"}

    frames: list = []
    failed_files: list[tuple[str, str]] = []
    skipped_files: list[str] = []
    parsed_file_names: list[str] = []
    # Map internal artifact paths to friendly display names.
    path_to_display: dict[str, str] = {}

    for mdb_file in mdb_files:
        file_path = Path(mdb_file.get("path", ""))
        file_display = mdb_file.get("display_name", "unknown")
        path_to_display[str(file_path)] = file_display

        if file_display.lower() in SKIPPED_MDB_NAMES:
            logger.info("Skipping %s (not a supported file)", file_display)
            skipped_files.append(file_display)
            continue

        try:
            events = _read_mdb(
                file_path,
                anchor_preference="insert_then_last",
                full_output=full_output,
            )
            if events:
                rows = [e.to_row() for e in events]
                frames.append(pl.from_dicts(rows, schema=TIMELINE_SCHEMA))
            parsed_file_names.append(file_display)
        except Exception as e:
            logger.warning("UAL Timeliner failed on %s: %s", file_display, e)
            failed_files.append((file_display, str(e)))

    if not frames:
        raise RuntimeError(
            "No timeline data was produced from the input files. "
            "Check that the files are valid UAL .mdb databases."
        )

    df = pl.concat(frames)
    del frames
    if enable_dedup:
        df = _deduplicate_timeline(df)
    df = df.sort(["timestamp", "timestamp_desc"])

    # Replace internal artifact paths with friendly display names.
    if "source_file" in df.columns and path_to_display:
        df = df.with_columns(
            pl.col("source_file").replace(path_to_display)
        )

    if not full_output:
        exclude_cols = ["role_guid", "client_name", "tenant_id", "access_count"]
        df = df.drop([c for c in exclude_cols if c in df.columns])

    # --- Write output files for each selected format ---
    output_files: list[dict] = []
    base_name = f"{output_prefix}_ual_timeline" if output_prefix else "ual_timeline"

    for fmt in formats:
        ext = FORMAT_EXTENSIONS.get(fmt, "csv")

        # Strip timezone for user-facing formats (display as UTC without offset).
        if fmt in ("csv", "xlsx", "sqlite", "parquet") and "timestamp" in df.columns:
            write_df = df.with_columns(
                pl.col("timestamp").dt.replace_time_zone(None)
            ).rename({"timestamp": "timestamp (UTC)"})
        else:
            write_df = df

        fmt_base = base_name

        if max_rows > 0 and fmt in ("csv", "k2t", "xlsx") and write_df.height > max_rows:
            for chunk_idx, start in enumerate(
                range(0, write_df.height, max_rows), start=1,
            ):
                length = min(max_rows, write_df.height - start)
                chunk_df = write_df.slice(start, length)
                chunk_name = f"{fmt_base}_part{chunk_idx}"
                out = create_output_file(
                    output_path,
                    display_name=chunk_name,
                    extension=ext,
                    data_type="openrelik:worker:kstrike:ual_timeline",
                )
                _write_timeline_df(chunk_df, out.path, fmt)
                output_files.append(out.to_dict())
        else:
            out = create_output_file(
                output_path,
                display_name=fmt_base,
                extension=ext,
                data_type="openrelik:worker:kstrike:ual_timeline",
            )
            _write_timeline_df(write_df, out.path, fmt)
            output_files.append(out.to_dict())

    # --- Build task report ---
    report = Report("UAL Timeliner Results")
    report.priority = Priority.INFO

    fmt_labels = ", ".join(f.upper() for f in formats)
    summary_parts = []
    if parsed_file_names:
        summary_parts.append(f"{len(parsed_file_names)} .mdb file(s) processed")
    if failed_files:
        summary_parts.append(f"{len(failed_files)} file(s) failed")
    summary_parts.append(f"{df.height} timeline events")
    summary_parts.append(f"Format: {fmt_labels}")
    if enable_dedup:
        summary_parts.append("Deduplicated")
    if len(output_files) > 1:
        summary_parts.append(f"{len(output_files)} output files")
    report.summary = ". ".join(summary_parts) + "."

    section = report.add_section()
    section.add_header("Summary")

    results_table = MarkdownTable(["File", "Status"])
    failed_names = {name for name, _ in failed_files}
    skipped_set = set(skipped_files)
    for mdb_file in mdb_files:
        name = mdb_file.get("display_name", "unknown")
        if name in skipped_set:
            results_table.add_row([name, "Skipped (not a UAL data file)"])
        elif name in failed_names:
            err = next(e for n, e in failed_files if n == name)
            results_table.add_row([name, f"Failed: {err}"])
        else:
            results_table.add_row([name, "Processed"])
    section.add_table(results_table)

    section.add_header("Output", level=3)
    section.add_paragraph(
        f"{df.height} timeline events in {fmt_labels} format."
    )

    if failed_files:
        section.add_header("Failed Files", level=3)
        for name, err in failed_files:
            section.add_bullet(f"{name}: {err}")

    if not output_files:
        raise RuntimeError(
            "No output was produced. Check that the input files are valid "
            "UAL .mdb databases."
        )

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id or "",
        command="UAL Timeliner",
        meta={},
        task_report=report.to_dict(),
    )
