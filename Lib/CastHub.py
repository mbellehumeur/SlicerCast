"""Cast Interface — Hub subsection (embedded Cast hub server UI)."""

from __future__ import annotations

from typing import Optional

import qt

from slicer.i18n import tr as _

DEFAULT_HUB_PORT = 2017

_STATUS_TEXT_STYLE_IDLE = "color: palette(text);"


class CastHubWidget:
    """Hub section UI (start/stop wiring deferred)."""

    def __init__(self) -> None:
        self._section: Optional[qt.QWidget] = None
        self._setup_complete = False

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

    def cleanup(self) -> None:
        pass

    def enter(self) -> None:
        pass

    def exit(self) -> None:
        pass
