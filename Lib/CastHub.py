"""Cast Interface — Hub subsection (embedded Cast hub server UI)."""

from __future__ import annotations

import os
import sys
from typing import Optional

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


class CastHubWidget:
    """Hub section UI: start/stop local cast_api subprocess."""

    def __init__(self) -> None:
        self._section: Optional[qt.QWidget] = None
        self._setup_complete = False
        self._hub_process: Optional[qt.QProcess] = None

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
        layout.addRow(_("Port:"), self.portSpinBox)

        self.startButton = qt.QPushButton(_("Start"))
        self.stopButton = qt.QPushButton(_("Stop"))
        self.stopButton.enabled = False
        self.openAdminButton = qt.QPushButton(_("Open admin portal"))
        self.openAdminButton.setToolTip(
            _("Open the hub admin page in your default browser")
        )

        button_row = qt.QWidget()
        button_row_layout = qt.QHBoxLayout(button_row)
        button_row_layout.setContentsMargins(0, 0, 0, 0)
        button_row_layout.addWidget(self.startButton)
        button_row_layout.addWidget(self.stopButton)
        button_row_layout.addWidget(self.openAdminButton)
        button_row_layout.addStretch(1)

        self.statusLabel = qt.QLabel(_("Stopped"))
        self.statusLabel.setStyleSheet(_STATUS_TEXT_STYLE_IDLE)
        status_row = qt.QWidget()
        status_row_layout = qt.QHBoxLayout(status_row)
        status_row_layout.setContentsMargins(0, 0, 0, 0)
        status_heading = qt.QLabel(_("Status:"))
        status_row_layout.addWidget(status_heading)
        status_row_layout.addWidget(self.statusLabel, 1)
        layout.addRow(button_row, status_row)

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
        port = int(self.portSpinBox.value) if self.portSpinBox else DEFAULT_HUB_PORT
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
        if self.startButton:
            self.startButton.enabled = False
        if self.stopButton:
            self.stopButton.enabled = True
        if self.portSpinBox:
            self.portSpinBox.enabled = False
        self._set_status(
            _("Running on port {0}").format(port),
            _STATUS_TEXT_STYLE_RUNNING,
        )

    def _on_stop(self) -> None:
        if not self._hub_process:
            return
        if self._hub_process.state() != qt.QProcess.NotRunning:
            self._hub_process.terminate()
            if not self._hub_process.waitForFinished(5000):
                self._hub_process.kill()
                self._hub_process.waitForFinished(3000)
        self._hub_process.deleteLater()
        self._hub_process = None
        if self.startButton:
            self.startButton.enabled = True
        if self.stopButton:
            self.stopButton.enabled = False
        if self.portSpinBox:
            self.portSpinBox.enabled = True
        self._set_status(_("Stopped"), _STATUS_TEXT_STYLE_IDLE)

    def _on_open_admin(self) -> None:
        port = int(self.portSpinBox.value) if self.portSpinBox else DEFAULT_HUB_PORT
        qt.QDesktopServices.openUrl(qt.QUrl(f"http://127.0.0.1:{port}/api/hub/admin"))

    def _on_hub_output(self) -> None:
        if not self._hub_process:
            return
        data = bytes(self._hub_process.readAllStandardOutput()).decode("utf-8", errors="replace")
        if data.strip():
            print(f"[CastHub] {data.rstrip()}")

    def _on_hub_finished(self, exit_code: int, exit_status: qt.QProcess.ExitStatus) -> None:
        self._hub_process = None
        if self.startButton:
            self.startButton.enabled = True
        if self.stopButton:
            self.stopButton.enabled = False
        if self.portSpinBox:
            self.portSpinBox.enabled = True
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
