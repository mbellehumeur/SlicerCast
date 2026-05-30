"""TotalSegmentator Cast onMessage: DICOM or NIfTI in, DICOM RT Struct out.

Inbound strategy:

- ``dicom-send``: STOW batch on ``context.files[]``; stage all files for the
  topic and run TotalSegmentator when the message is received (same pattern as
  ``nifti-send``).
- ``nifti-send``: one ``.nii.gz`` per study; segmentation runs when received.

Cast UI (Service Providers): Product ``TOTALSEG``, Version ``1.0``, hub of your
choice, script path pointing at this file. Subscriber name is auto-generated;
see the hub admin portal.

See ``totalsegmentator-readme.md`` in this folder for setup and input notes.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from Lib.cast_provider_runtime import (
    extract_all_dicom_send_payloads,
    extract_all_nifti_send_payloads,
    get_active_provider_products,
    publish_dicom_send_file,
    publish_nifti_send_file,
    record_dicom_send_received,
    record_nifti_send_received,
)

LOGGER = logging.getLogger("CastInterface.TotalSegmentator")
LOGGER.setLevel(logging.INFO)

DEFAULT_PRODUCT_NAME = "TOTALSEG"
_DICOM_SEND_EVENT = "dicom-send"
_NIFTI_SEND_EVENT = "nifti-send"
TS_TASK = "total"
# ``--fast`` breaks DICOM RT Struct export (mask z vs series slice count).
TS_FAST = False
TS_MULTILABEL = True
OUTPUT_DICOM_NAME = "segmentations.dcm"
OUTPUT_NIFTI_NAME = "segmentations.nii.gz"

_staging_lock = threading.Lock()
_topic_states: Dict[str, "_TopicStaging"] = {}


def _safe_topic_dir_name(topic: str) -> str:
    safe = re.sub(r"[^\w.\-]+", "_", topic.strip())
    return safe or "topic"


@dataclass
class _TopicStaging:
    topic: str
    product_name: str
    input_dir: Path
    processing: bool = False


def onMessage(message: Dict[str, Any], provider: Any) -> None:
    event = message.get("event") or {}
    hub_event = event.get("hub.event")
    if hub_event == "nifti-send":
        _on_nifti_send(message, event, provider)
        return
    if hub_event != "dicom-send":
        return
    _on_dicom_send(message, event, provider)


def _on_dicom_send(
    message: Dict[str, Any], event: Dict[str, Any], provider: Any
) -> None:
    topic = (event.get("hub.topic") or "").strip()
    if not topic:
        LOGGER.warning("TotalSegmentator onMessage: dicom-send missing hub.topic")
        return

    payloads = extract_all_dicom_send_payloads(message)
    if not payloads:
        LOGGER.warning(
            "TotalSegmentator onMessage: no DICOM payload id=%s topic=%s",
            message.get("id", ""),
            topic,
        )
        return

    product_name = getattr(provider, "product_name", "") or DEFAULT_PRODUCT_NAME
    LOGGER.info(
        "TotalSegmentator onMessage: dicom-send id=%s topic=%s files=%d",
        message.get("id", ""),
        topic,
        len(payloads),
    )

    staging = _get_or_create_staging(topic, product_name)
    for file_name, data in payloads:
        record_dicom_send_received(topic, len(data))
        _stage_file(staging, file_name, data)
    _start_topic_segmentation(topic, _DICOM_SEND_EVENT)


def _on_nifti_send(
    message: Dict[str, Any], event: Dict[str, Any], provider: Any
) -> None:
    topic = (event.get("hub.topic") or "").strip()
    if not topic:
        LOGGER.warning("TotalSegmentator onMessage: nifti-send missing hub.topic")
        return

    payloads = extract_all_nifti_send_payloads(message)
    if not payloads:
        LOGGER.warning(
            "TotalSegmentator onMessage: no NIfTI payload id=%s topic=%s",
            message.get("id", ""),
            topic,
        )
        return

    product_name = getattr(provider, "product_name", "") or DEFAULT_PRODUCT_NAME
    LOGGER.info(
        "TotalSegmentator onMessage: nifti-send id=%s topic=%s files=%d",
        message.get("id", ""),
        topic,
        len(payloads),
    )

    staging = _get_or_create_staging(topic, product_name)
    for file_name, data in payloads:
        record_nifti_send_received(topic, len(data))
        _stage_file(staging, file_name, data)
    _start_topic_segmentation(topic, _NIFTI_SEND_EVENT)


def _get_or_create_staging(topic: str, product_name: str) -> _TopicStaging:
    with _staging_lock:
        state = _topic_states.get(topic)
        if state is None:
            base = (
                Path(tempfile.gettempdir())
                / "cast-totalseg"
                / _safe_topic_dir_name(topic)
            )
            input_dir = base / "input"
            input_dir.mkdir(parents=True, exist_ok=True)
            state = _TopicStaging(topic=topic, product_name=product_name, input_dir=input_dir)
            _topic_states[topic] = state
        else:
            state.product_name = product_name
        return state


def _stage_file(staging: _TopicStaging, file_name: str, data: bytes) -> None:
    name = os.path.basename(file_name.strip()) or "dicom-send.dcm"
    if name.lower().endswith(".zip"):
        zip_path = staging.input_dir / name
        zip_path.write_bytes(data)
        try:
            with zipfile.ZipFile(zip_path, "r") as archive:
                members = [
                    info.filename
                    for info in archive.infolist()
                    if info.filename and not info.filename.endswith("/")
                ]
                archive.extractall(staging.input_dir)
            zip_path.unlink(missing_ok=True)
            LOGGER.info(
                "TotalSegmentator extracted %d file(s) from zip into %s",
                len(members),
                staging.input_dir,
            )
        except zipfile.BadZipFile:
            LOGGER.exception("TotalSegmentator invalid zip: %s", zip_path)
        return

    dest = staging.input_dir / name
    dest.write_bytes(data)


def _start_topic_segmentation(topic: str, hub_event: str) -> None:
    with _staging_lock:
        staging = _topic_states.get(topic)
        if staging is None:
            return
        if staging.processing:
            LOGGER.info(
                "TotalSegmentator deferring topic=%s (job already running)",
                topic,
            )
            return
        staging.processing = True
        staged_files = _count_input_files(staging.input_dir)

    LOGGER.info(
        "TotalSegmentator starting segmentation topic=%s hub.event=%s staged_files=%d",
        topic,
        hub_event,
        staged_files,
    )
    worker = threading.Thread(
        target=_run_segmentation_job,
        args=(topic, hub_event),
        name=f"CastTotalSeg-{topic}",
        daemon=True,
    )
    worker.start()


def _run_segmentation_job(topic: str, hub_event: str) -> None:
    with _staging_lock:
        staging = _topic_states.get(topic)
        if staging is None:
            return
        product_name = staging.product_name
        input_dir = staging.input_dir

    try:
        _run_segmentation_job_body(
            topic,
            product_name,
            input_dir,
            hub_event,
        )
    finally:
        with _staging_lock:
            staging = _topic_states.get(topic)
            if staging is not None:
                staging.processing = False


def _run_segmentation_job_body(
    topic: str, product_name: str, input_dir: Path, hub_event: str
) -> None:
    job_dir: Optional[Path] = None
    output_file: Optional[Path] = None
    try:
        staged_count = _count_input_files(input_dir)
        if staged_count < 1:
            LOGGER.warning(
                "TotalSegmentator: no input files for topic=%s in %s",
                topic,
                input_dir,
            )
            return

        stamp = int(time.time() * 1000)
        job_dir = (
            Path(tempfile.gettempdir())
            / "cast-totalseg-jobs"
            / f"{_safe_topic_dir_name(topic)}-{stamp}"
        )
        job_input = job_dir / "input"
        job_output = job_dir / "output"
        LOGGER.info(
            "TotalSegmentator starting job topic=%s files=%d staging=%s job=%s",
            topic,
            staged_count,
            input_dir,
            job_dir,
        )
        shutil.copytree(input_dir, job_input)
        _clear_staging_input(input_dir)
        cli_input = _cli_input_path(job_input, hub_event)
        LOGGER.info(
            "TotalSegmentator copied %d file(s) to job input %s; cli -i=%s hub.event=%s",
            staged_count,
            job_input,
            cli_input,
            hub_event,
        )

        output_file = _run_totalsegmentator(cli_input, job_output, hub_event)
        if not output_file:
            LOGGER.error(
                "TotalSegmentator: no output for hub.event=%s topic=%s (job dir kept: %s)",
                hub_event,
                topic,
                job_dir,
            )
            return

        LOGGER.info(
            "TotalSegmentator output hub.event=%s topic=%s: %s",
            hub_event,
            topic,
            output_file,
        )
        if hub_event == _NIFTI_SEND_EVENT:
            published = publish_nifti_send_file(
                product_name, topic, str(output_file)
            )
        else:
            published = publish_dicom_send_file(
                product_name, topic, str(output_file)
            )
        if published:
            LOGGER.info(
                "TotalSegmentator published %s to topic=%s product=%s",
                output_file,
                topic,
                product_name,
            )
        else:
            LOGGER.warning(
                "TotalSegmentator failed to publish %s topic=%s product=%s (active: %s)",
                output_file,
                topic,
                product_name,
                ", ".join(get_active_provider_products()) or "(none)",
            )
    except Exception as exc:
        LOGGER.exception(
            "TotalSegmentator job failed topic=%s: %s", topic, exc
        )
    finally:
        if job_dir and job_dir.is_dir():
            if output_file and output_file.is_file():
                try:
                    shutil.rmtree(job_dir)
                except OSError as exc:
                    LOGGER.warning("TotalSegmentator cleanup failed: %s", exc)
            else:
                LOGGER.warning(
                    "TotalSegmentator keeping job dir for inspection: %s", job_dir
                )


def _count_input_files(input_dir: Path) -> int:
    if not input_dir.is_dir():
        return 0
    return sum(
        1
        for path in input_dir.rglob("*")
        if path.is_file() and not path.name.startswith(".")
    )


def _has_dicom_input(input_dir: Path) -> bool:
    return _count_input_files(input_dir) > 0


def _cli_input_path(input_dir: Path, hub_event: str) -> Path:
    """Resolve TotalSegmentator ``-i`` from Cast ``hub.event``."""
    if hub_event == _NIFTI_SEND_EVENT:
        for pattern in ("*.nii.gz", "*.nii", "*.nhdr", "*.nrrd"):
            matches = sorted(input_dir.glob(pattern))
            if matches:
                if len(matches) > 1:
                    LOGGER.warning(
                        "TotalSegmentator: multiple volume files under %s, using %s",
                        input_dir,
                        matches[0],
                    )
                return matches[0]
        raise FileNotFoundError(f"No NIfTI/volume file under {input_dir}")
    if hub_event == _DICOM_SEND_EVENT:
        return input_dir
    raise ValueError(f"Unsupported hub.event for segmentation: {hub_event!r}")


def _cli_output_path(output_dir: Path, hub_event: str) -> Path:
    """Resolve TotalSegmentator ``-o`` from Cast ``hub.event``."""
    if hub_event == _NIFTI_SEND_EVENT:
        return output_dir / OUTPUT_NIFTI_NAME
    if hub_event == _DICOM_SEND_EVENT:
        return output_dir / OUTPUT_DICOM_NAME
    raise ValueError(f"Unsupported hub.event for segmentation: {hub_event!r}")


def _clear_staging_input(input_dir: Path) -> None:
    if not input_dir.is_dir():
        return
    for entry in input_dir.iterdir():
        if entry.is_file():
            entry.unlink()
        elif entry.is_dir():
            shutil.rmtree(entry, ignore_errors=True)


def _ts_executable_name(name: str) -> str:
    return name + ".exe" if os.name == "nt" else name


def _total_segmentator_launch_command() -> Optional[list[str]]:
    """PythonSlicer + TotalSegmentator CLI (same pattern as Slicer extension)."""
    import shutil

    python_slicer = shutil.which("PythonSlicer")
    if not python_slicer:
        LOGGER.error("TotalSegmentator: PythonSlicer not found on PATH")
        return None

    scripts_dir = sysconfig.get_path("scripts")
    ts_script = os.path.join(
        scripts_dir, _ts_executable_name("TotalSegmentator")
    )
    if not os.path.isfile(ts_script):
        LOGGER.error(
            "TotalSegmentator: CLI not found at %s (install TotalSegmentator extension)",
            ts_script,
        )
        return None

    return [python_slicer, ts_script]


def _total_segmentator_cli_options(
    input_path: Path, output_path: Path, device: str, hub_event: str
) -> list[str]:
    options = [
        "-i",
        str(input_path.resolve()),
        "-o",
        str(output_path.resolve()),
        "--task",
        TS_TASK,
        "-d",
        device,
        "-nr",
        "1",
        "-ns",
        "1",
    ]
    if hub_event == _DICOM_SEND_EVENT:
        options.extend(["-ot", "dicom"])
    if TS_MULTILABEL:
        options.append("--ml")
    if TS_FAST:
        options.append("--fast")
    return options


def _log_subprocess_output(proc: Any) -> None:
    while True:
        try:
            line = proc.stdout.readline()
        except UnicodeDecodeError:
            continue
        if not line:
            break
        text = line.rstrip() if isinstance(line, str) else line.decode(
            "utf-8", errors="replace"
        ).rstrip()
        if text:
            LOGGER.info("TotalSegmentator: %s", text)


def _run_totalsegmentator_subprocess(
    command: list[str], options: list[str]
) -> bool:
    from subprocess import CalledProcessError

    cmd = command + options
    LOGGER.info("TotalSegmentator launch: %s", " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    _log_subprocess_output(proc)
    proc.wait()
    if proc.returncode != 0:
        raise CalledProcessError(proc.returncode, cmd)
    return True


def _run_totalsegmentator(
    input_path: Path, output_dir: Path, hub_event: str
) -> Optional[Path]:
    command = _total_segmentator_launch_command()
    if not command:
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = _cli_output_path(output_dir, hub_event)

    for device in ("gpu", "cpu"):
        try:
            LOGGER.info(
                "TotalSegmentator running task=%s fast=%s device=%s hub.event=%s input=%s output=%s",
                TS_TASK,
                TS_FAST,
                device,
                hub_event,
                input_path,
                output_path,
            )
            started = time.monotonic()
            options = _total_segmentator_cli_options(
                input_path, output_path, device, hub_event
            )
            _run_totalsegmentator_subprocess(command, options)
            elapsed = time.monotonic() - started
            LOGGER.info(
                "TotalSegmentator finished device=%s in %.1fs hub.event=%s output=%s",
                device,
                elapsed,
                hub_event,
                output_path,
            )
            result = _find_segmentation_output(output_dir, hub_event, output_path)
            if result:
                return result
            LOGGER.warning(
                "TotalSegmentator exited OK but no output for hub.event=%s under %s",
                hub_event,
                output_dir,
            )
        except Exception as exc:
            LOGGER.warning(
                "TotalSegmentator failed device=%s: %s", device, exc
            )
            if output_dir.is_dir():
                for entry in output_dir.iterdir():
                    if entry.is_file():
                        entry.unlink()
                    elif entry.is_dir():
                        shutil.rmtree(entry, ignore_errors=True)
    return None


def _find_segmentation_output(
    output_dir: Path, hub_event: str, expected_path: Path
) -> Optional[Path]:
    if expected_path.is_file():
        return expected_path
    if hub_event == _NIFTI_SEND_EVENT:
        return _find_output_nifti(output_dir)
    return _find_output_dicom(output_dir)


def _find_output_nifti(output_dir: Path) -> Optional[Path]:
    primary = output_dir / OUTPUT_NIFTI_NAME
    if primary.is_file():
        return primary
    candidates = sorted(
        path
        for path in output_dir.rglob("*")
        if path.is_file()
        and path.suffix.lower() in (".gz", ".nii", ".nhdr", ".nrrd")
        and "input" not in path.parts
    )
    if not candidates:
        LOGGER.warning(
            "TotalSegmentator: no NIfTI under %s (contents: %s)",
            output_dir,
            list(output_dir.rglob("*"))[:20],
        )
        return None
    if len(candidates) > 1:
        LOGGER.warning(
            "TotalSegmentator: multiple NIfTI outputs, using %s (all: %s)",
            candidates[0],
            [str(p) for p in candidates],
        )
    return candidates[0]


def _find_output_dicom(output_dir: Path) -> Optional[Path]:
    primary = output_dir / OUTPUT_DICOM_NAME
    if primary.is_file():
        return primary
    candidates = [
        path
        for path in output_dir.rglob("*.dcm")
        if path.is_file() and "input" not in path.parts
    ]
    if not candidates:
        LOGGER.warning(
            "TotalSegmentator: no .dcm under %s (contents: %s)",
            output_dir,
            list(output_dir.rglob("*"))[:20],
        )
        return None
    if len(candidates) > 1:
        LOGGER.warning(
            "TotalSegmentator: multiple .dcm outputs, using %s (all: %s)",
            candidates[0],
            [str(p) for p in candidates],
        )
    return candidates[0]
