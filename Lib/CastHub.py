"""Cast Interface — Hub subsection (embedded Cast hub server UI)."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from typing import List, Optional

import qt

from slicer.i18n import tr as _
import slicer

DEFAULT_HUB_PORT = 2018

_STATUS_TEXT_STYLE_IDLE = "color: palette(text);"
_STATUS_TEXT_STYLE_RUNNING = "color: palette(link);"
_STATUS_TEXT_STYLE_ERROR = "color: palette(negative);"

def _extension_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _cast_api_dir() -> str:
    return os.path.join(_extension_root(), "cast_api")


# pip spec -> import name (Slicer pip_install accepts one package per call).
_HUB_PIP_PACKAGES = (
    ("fastapi", "fastapi"),
    ("uvicorn[standard]", "uvicorn"),
    ("python-multipart", "multipart"),
    ("aiohttp", "aiohttp"),
    ("psutil", "psutil"),
)


def _ensure_hub_deps() -> None:
    """Install hub deps via Slicer pip (one package per call)."""
    missing = []
    for pip_spec, import_name in _HUB_PIP_PACKAGES:
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pip_spec)
    if not missing:
        return
    for pip_spec in missing:
        slicer.util.pip_install(pip_spec)
    import importlib

    for _pip_spec, import_name in _HUB_PIP_PACKAGES:
        importlib.import_module(import_name)


def _is_windows() -> bool:
    return sys.platform == "win32"


def _hub_port_from_spin(spin: Optional[qt.QSpinBox]) -> int:
    if not spin:
        return DEFAULT_HUB_PORT
    return int(spin.value)


def _port_is_listening(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.4):
            return True
    except OSError:
        return False


def _win_kill_process_tree(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        result = subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _win_pids_listening_on_port(port: int) -> List[int]:
    pids: List[int] = []
    try:
        completed = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        output = completed.stdout or ""
    except (OSError, subprocess.TimeoutExpired):
        return pids
    port_suffix = f":{port}"
    for line in output.splitlines():
        upper = line.upper()
        if "LISTENING" not in upper or port_suffix not in line:
            continue
        parts = line.split()
        if not parts:
            continue
        try:
            pids.append(int(parts[-1]))
        except ValueError:
            continue
    seen = set()
    unique: List[int] = []
    for pid in pids:
        if pid not in seen:
            seen.add(pid)
            unique.append(pid)
    return unique


def _force_stop_qprocess(proc: qt.QProcess) -> None:
    """Stop hub child process; on Windows use taskkill on the process tree."""
    pid = int(proc.processId()) if proc else 0
    if proc.state() == qt.QProcess.NotRunning:
        if _is_windows() and pid > 0:
            _win_kill_process_tree(pid)
        return
    if _is_windows():
        if pid > 0:
            _win_kill_process_tree(pid)
        proc.kill()
        proc.waitForFinished(5000)
        return
    proc.terminate()
    if not proc.waitForFinished(5000):
        proc.kill()
        proc.waitForFinished(3000)


def _stop_listeners_on_port(port: int, known_pid: int = 0) -> None:
    if known_pid > 0:
        _win_kill_process_tree(known_pid)
    if not _is_windows():
        return
    for pid in _win_pids_listening_on_port(port):
        _win_kill_process_tree(pid)


class CastHubWidget:
    """Hub section UI: start/stop local cast_api subprocess."""

    def __init__(self) -> None:
        self._section: Optional[qt.QWidget] = None
        self._setup_complete = False
        self._hub_process: Optional[qt.QProcess] = None
        self._hub_pid: int = 0
        self._hub_port: int = DEFAULT_HUB_PORT

        self.portSpinBox: Optional[qt.QSpinBox] = None
        self.startButton: Optional[qt.QPushButton] = None
        self.stopButton: Optional[qt.QPushButton] = None
        self.openAdminButton: Optional[qt.QPushButton] = None
        self.statusLabel: Optional[qt.QLabel] = None

    def setup(self, section: qt.QWidget) -> None:
        if self._setup_complete:
            return
        self._setup_complete = True
        self._section = section
        layout = qt.QFormLayout(section)
        layout.setLabelAlignment(qt.Qt.AlignLeft)

        self.portSpinBox = qt.QSpinBox()
        self.portSpinBox.setRange(1024, 65535)
        self.portSpinBox.setValue(DEFAULT_HUB_PORT)
        self.portSpinBox.setToolTip(_("TCP port for the local Cast hub server"))

        self.startButton = qt.QPushButton(_("Start"))
        self.stopButton = qt.QPushButton(_("Stop"))
        self.stopButton.enabled = False
        self.openAdminButton = qt.QPushButton(_("Open Hub Portal"))
        self.openAdminButton.enabled = False
        self.openAdminButton.setToolTip(
            _("Open the hub portal in your default browser")
        )

        port_row = qt.QWidget()
        port_row_layout = qt.QHBoxLayout(port_row)
        port_row_layout.setContentsMargins(0, 0, 0, 0)
        port_row_layout.addWidget(self.portSpinBox)
        port_row_layout.addWidget(self.startButton)
        port_row_layout.addWidget(self.stopButton)
        port_row_layout.addWidget(self.openAdminButton)
        port_row_layout.addStretch(1)
        layout.addRow(_("Port:"), port_row)

        self.statusLabel = qt.QLabel(_("Stopped"))
        self.statusLabel.setStyleSheet(_STATUS_TEXT_STYLE_IDLE)
        layout.addRow(_("Status:"), self.statusLabel)

        self.startButton.connect("clicked()", self._on_start)
        self.stopButton.connect("clicked()", self._on_stop)
        self.openAdminButton.connect("clicked()", self._on_open_admin)

    def _set_status(self, text: str, style: str = _STATUS_TEXT_STYLE_IDLE) -> None:
        if self.statusLabel:
            self.statusLabel.setText(text)
            self.statusLabel.setStyleSheet(style)

    def _hub_script_path(self) -> str:
        return os.path.join(_cast_api_dir(), "cast_api.py")

    def _is_running(self) -> bool:
        return self._hub_process is not None and self._hub_process.state() != qt.QProcess.NotRunning

    def _on_start(self) -> None:
        if self._is_running():
            return
        cast_api_dir = _cast_api_dir()
        script = self._hub_script_path()
        if not os.path.isfile(script):
            self._set_status(_("cast_api.py not found"), _STATUS_TEXT_STYLE_ERROR)
            return
        port = _hub_port_from_spin(self.portSpinBox)
        self._hub_port = port
        try:
            _ensure_hub_deps()
        except Exception as exc:
            self._set_status(
                _("Dependency install failed: {0}").format(exc),
                _STATUS_TEXT_STYLE_ERROR,
            )
            return

        proc = qt.QProcess()
        proc.setProgram(sys.executable)
        proc.setArguments([script, "--port", str(port)])
        proc.setWorkingDirectory(cast_api_dir)
        proc.setProcessChannelMode(qt.QProcess.MergedChannels)
        proc.readyReadStandardOutput.connect(self._on_hub_output)
        proc.finished.connect(self._on_hub_finished)
        proc.start()
        if not proc.waitForStarted(15000):
            self._set_status(_("Failed to start hub"), _STATUS_TEXT_STYLE_ERROR)
            proc.deleteLater()
            return

        self._hub_process = proc
        self._hub_pid = int(proc.processId())
        if self.startButton:
            self.startButton.enabled = False
        if self.stopButton:
            self.stopButton.enabled = True
        if self.openAdminButton:
            self.openAdminButton.enabled = True
        if self.portSpinBox:
            self.portSpinBox.enabled = False
        self._set_status(
            _("Running on port {0}").format(port),
            _STATUS_TEXT_STYLE_RUNNING,
        )

    def _reset_ui_stopped(self) -> None:
        if self.startButton:
            self.startButton.enabled = True
        if self.stopButton:
            self.stopButton.enabled = False
        if self.openAdminButton:
            self.openAdminButton.enabled = False
        if self.portSpinBox:
            self.portSpinBox.enabled = True

    def _on_stop(self) -> None:
        port = self._hub_port or _hub_port_from_spin(self.portSpinBox)
        known_pid = self._hub_pid
        proc = self._hub_process
        if proc is not None:
            _force_stop_qprocess(proc)
            proc.deleteLater()
            self._hub_process = None
        elif known_pid > 0 and _is_windows():
            _win_kill_process_tree(known_pid)
        if _port_is_listening("127.0.0.1", port):
            _stop_listeners_on_port(port, known_pid)
        self._hub_pid = 0
        self._reset_ui_stopped()
        if _port_is_listening("127.0.0.1", port):
            self._set_status(
                _("Port {0} still in use — close other hub processes").format(port),
                _STATUS_TEXT_STYLE_ERROR,
            )
            return
        self._set_status(_("Stopped"), _STATUS_TEXT_STYLE_IDLE)

    def _on_open_admin(self) -> None:
        port = _hub_port_from_spin(self.portSpinBox)
        qt.QDesktopServices.openUrl(qt.QUrl(f"http://127.0.0.1:{port}/api/hub/admin"))

    def _on_hub_output(self) -> None:
        if not self._hub_process:
            return
        data = bytes(self._hub_process.readAllStandardOutput()).decode("utf-8", errors="replace")
        if data.strip():
            print(f"[CastHub] {data.rstrip()}")

    def _on_hub_finished(self, exit_code: int, exit_status: qt.QProcess.ExitStatus) -> None:
        self._hub_process = None
        self._hub_pid = 0
        self._reset_ui_stopped()
        if exit_status == qt.QProcess.NormalExit and exit_code == 0:
            self._set_status(_("Stopped"), _STATUS_TEXT_STYLE_IDLE)
        else:
            self._set_status(
                _("Hub exited ({0})").format(exit_code),
                _STATUS_TEXT_STYLE_ERROR,
            )

    def cleanup(self) -> None:
        self._on_stop()

    def enter(self) -> None:
        pass

    def exit(self) -> None:
        self._on_stop()
