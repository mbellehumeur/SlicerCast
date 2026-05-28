"""Cast Interface — Image Display Client subsection."""

from __future__ import annotations

import logging
import queue
from typing import Any, Callable, Dict, Optional

import qt

import slicer
from slicer.i18n import tr as _

from .cast_client import generate_subscriber_name
from .CastServiceProviders import (
    DEFAULT_HUB_NAME,
    HUBS,
    hub_admin_url_for_name,
    MAIN_QUEUE_TIMER_MS,
)
from .image_display_client_hub import (
    DISPLAY_PRODUCT_NAME,
    DISPLAY_PRODUCT_VERSION,
    ImageDisplayClientConnection,
    disconnect_image_display_client,
)

LOGGER = logging.getLogger("CastInterface.ImageDisplay")
LOGGER.setLevel(logging.INFO)

DEFAULT_DISPLAY_TOPIC = "USER-1"

_SETTINGS_GROUP = "CastInterface"
_SETTINGS_KEY_HUB = "imageDisplayHub"
_SETTINGS_KEY_TOPIC = "imageDisplayTopic"
_SETTINGS_KEY_PRODUCT = "imageDisplayProductName"
_SETTINGS_KEY_VERSION = "imageDisplayProductVersion"

_STATUS_TEXT_STYLE_IDLE = "color: palette(text);"
_STATUS_TEXT_STYLE_CONNECTED = "color: #2e7d32; font-weight: bold;"
_STATUS_TEXT_STYLE_ACTIVE = "color: #1a5f9e; font-weight: bold;"
_STATUS_TEXT_STYLE_ERROR = "color: #c45c26; font-weight: bold;"


def _connected_status_text(message_count: int) -> str:
    if message_count == 1:
        return _("Connected (1 message received)")
    return _("Connected ({count} messages received)").format(count=message_count)


class CastImageDisplayClientWidget:
    """UI and hub connection for the Image Display Client section."""

    def __init__(self) -> None:
        self._section: Optional[qt.QWidget] = None
        self._hub = ImageDisplayClientConnection(self.post_ui)
        self._main_queue: queue.Queue[Callable[[], None]] = queue.Queue()
        self._main_queue_running = False
        self._setup_complete = False
        self._main_queue_timer = qt.QTimer()
        self._main_queue_timer.setInterval(MAIN_QUEUE_TIMER_MS)
        self._main_queue_timer.timeout.connect(self._main_queue_process)

        self.productNameEdit: Optional[qt.QLineEdit] = None
        self.versionEdit: Optional[qt.QLineEdit] = None
        self.hubComboBox: Optional[qt.QComboBox] = None
        self.openHubButton: Optional[qt.QPushButton] = None
        self.topicEdit: Optional[qt.QLineEdit] = None
        self.connectButton: Optional[qt.QPushButton] = None
        self.disconnectButton: Optional[qt.QPushButton] = None
        self.statusLabel: Optional[qt.QLabel] = None

    def setup(self, section: qt.QWidget) -> None:
        if self._setup_complete:
            return
        self._setup_complete = True
        self._section = section
        layout = qt.QFormLayout(section)
        layout.setLabelAlignment(qt.Qt.AlignLeft)
        layout.setHorizontalSpacing(84)

        product_row = qt.QWidget()
        product_row_layout = qt.QHBoxLayout(product_row)
        product_row_layout.setContentsMargins(0, 0, 0, 0)
        self.productNameEdit = qt.QLineEdit(
            self._load_setting(_SETTINGS_KEY_PRODUCT, DISPLAY_PRODUCT_NAME)
        )
        self.productNameEdit.setPlaceholderText(_("Product name"))
        self.productNameEdit.setToolTip(_("subscriber.product.name on the hub"))
        version_label = qt.QLabel(_("Version:"))
        self.versionEdit = qt.QLineEdit(
            self._load_setting(_SETTINGS_KEY_VERSION, DISPLAY_PRODUCT_VERSION)
        )
        self.versionEdit.setMaximumWidth(80)
        self.versionEdit.setToolTip(_("subscriber.product.version on the hub"))
        product_row_layout.addWidget(self.productNameEdit, 1)
        product_row_layout.addWidget(version_label)
        product_row_layout.addWidget(self.versionEdit)
        layout.addRow(_("Product:"), product_row)

        hub_row = qt.QWidget()
        hub_row_layout = qt.QHBoxLayout(hub_row)
        hub_row_layout.setContentsMargins(0, 0, 0, 0)
        self.hubComboBox = qt.QComboBox()
        for hub_name in sorted(HUBS.keys()):
            self.hubComboBox.addItem(hub_name)
        saved_hub = self._load_setting(_SETTINGS_KEY_HUB, DEFAULT_HUB_NAME)
        hub_index = self.hubComboBox.findText(saved_hub)
        if hub_index >= 0:
            self.hubComboBox.setCurrentIndex(hub_index)
        self.openHubButton = qt.QPushButton(_("Open"))
        self.openHubButton.setSizePolicy(
            qt.QSizePolicy.Fixed, qt.QSizePolicy.Fixed
        )
        self.openHubButton.setToolTip(
            _("Open the hub admin portal in your default browser")
        )
        self.openHubButton.clicked.connect(self._on_open_hub)
        self.connectButton = qt.QPushButton(_("Connect"))
        self.connectButton.clicked.connect(self._on_connect)
        self.disconnectButton = qt.QPushButton(_("Disconnect"))
        self.disconnectButton.clicked.connect(self._on_disconnect)
        self.disconnectButton.enabled = False
        metrics = qt.QFontMetrics(self.hubComboBox.font)
        max_hub_text_w = max(
            metrics.horizontalAdvance(name) for name in HUBS.keys()
        )
        hub_combo_pad = 48
        self.hubComboBox.setMaximumWidth(max_hub_text_w + hub_combo_pad)
        hub_row_layout.addWidget(self.hubComboBox, 0)
        hub_row_layout.addWidget(self.openHubButton, 0)
        hub_row_layout.addWidget(self.connectButton, 0)
        hub_row_layout.addWidget(self.disconnectButton, 0)
        hub_row.setSizePolicy(qt.QSizePolicy.Maximum, qt.QSizePolicy.Fixed)
        layout.addRow(_("Hub:"), hub_row)

        self.topicEdit = qt.QLineEdit(
            self._load_setting(_SETTINGS_KEY_TOPIC, DEFAULT_DISPLAY_TOPIC)
        )
        self.topicEdit.setPlaceholderText(_("Hub topic"))
        layout.addRow(_("Topic:"), self.topicEdit)

        self.statusLabel = qt.QLabel(_("Disconnected"))
        self._apply_status_style("idle")
        status_row = qt.QWidget()
        status_row_layout = qt.QHBoxLayout(status_row)
        status_row_layout.setContentsMargins(0, 0, 0, 0)
        status_heading = qt.QLabel(_("Status:"))
        status_row_layout.addWidget(status_heading)
        status_row_layout.addWidget(self.statusLabel, 1)
        layout.addRow(status_row)

    @staticmethod
    def _load_setting(key: str, default: str) -> str:
        settings = qt.QSettings()
        settings.beginGroup(_SETTINGS_GROUP)
        value = settings.value(key, default)
        settings.endGroup()
        return str(value).strip() if value else default

    def _save_settings(self) -> None:
        settings = qt.QSettings()
        settings.beginGroup(_SETTINGS_GROUP)
        if self.hubComboBox:
            settings.setValue(_SETTINGS_KEY_HUB, self.hubComboBox.currentText)
        if self.topicEdit:
            settings.setValue(_SETTINGS_KEY_TOPIC, self.topicEdit.text.strip())
        if self.productNameEdit:
            settings.setValue(
                _SETTINGS_KEY_PRODUCT, self.productNameEdit.text.strip()
            )
        if self.versionEdit:
            settings.setValue(_SETTINGS_KEY_VERSION, self.versionEdit.text.strip())
        settings.endGroup()

    def _product_name(self) -> str:
        if self.productNameEdit:
            name = self.productNameEdit.text.strip()
            if name:
                return name
        return DISPLAY_PRODUCT_NAME

    def _product_version(self) -> str:
        if self.versionEdit:
            version = self.versionEdit.text.strip()
            if version:
                return version
        return DISPLAY_PRODUCT_VERSION

    def cleanup(self) -> None:
        self.exit()
        disconnect_image_display_client()
        self._main_queue_drain()

    def enter(self) -> None:
        self._main_queue_running = True
        self._main_queue_timer.start()
        self._main_queue_drain()
        self._refresh_status()

    def exit(self) -> None:
        self._main_queue_running = False
        self._main_queue_timer.stop()

    def post_ui(self, fn: Callable[[], None]) -> None:
        self._main_queue.put(fn)

    def _main_queue_drain(self) -> None:
        try:
            while not self._main_queue.empty():
                fn = self._main_queue.get_nowait()
                fn()
        except Exception as exc:
            LOGGER.exception("Cast Image Display main queue error: %s", exc)

    def _main_queue_process(self) -> None:
        if not self._main_queue_running:
            return
        self._main_queue_drain()

    def _apply_status_style(self, variant: str) -> None:
        if not self.statusLabel:
            return
        styles = {
            "idle": _STATUS_TEXT_STYLE_IDLE,
            "connected": _STATUS_TEXT_STYLE_CONNECTED,
            "active": _STATUS_TEXT_STYLE_ACTIVE,
            "error": _STATUS_TEXT_STYLE_ERROR,
            "failed": _STATUS_TEXT_STYLE_ERROR,
        }
        self.statusLabel.setStyleSheet(
            styles.get(variant, _STATUS_TEXT_STYLE_IDLE)
        )

    def _set_fields_enabled(self, enabled: bool) -> None:
        if self.productNameEdit:
            self.productNameEdit.setEnabled(enabled)
        if self.versionEdit:
            self.versionEdit.setEnabled(enabled)
        if self.hubComboBox:
            self.hubComboBox.setEnabled(enabled)
        if self.openHubButton:
            self.openHubButton.setEnabled(True)
        if self.topicEdit:
            self.topicEdit.setEnabled(enabled)

    def _on_open_hub(self) -> None:
        if not self.hubComboBox:
            return
        hub_name = self.hubComboBox.currentText
        admin_url = hub_admin_url_for_name(hub_name)
        if not admin_url:
            slicer.util.warningDisplay(
                _("No admin URL for hub {name}").format(name=hub_name)
            )
            return
        if not qt.QDesktopServices.openUrl(qt.QUrl(admin_url)):
            slicer.util.warningDisplay(
                _("Could not open hub admin URL:\n{url}").format(url=admin_url)
            )

    def _on_connect(self) -> None:
        if not self.topicEdit or not self.hubComboBox or not self.productNameEdit:
            return
        topic = self.topicEdit.text.strip()
        if not topic:
            slicer.util.errorDisplay(_("Enter a topic before connecting."))
            return

        product_name = self._product_name()
        if not product_name:
            slicer.util.errorDisplay(_("Enter a product name before connecting."))
            return

        product_version = self._product_version()
        subscriber = generate_subscriber_name(product_name)

        self._save_settings()

        if self._hub.isHubThreadRunning():
            return

        self._set_fields_enabled(False)
        self._apply_status_style("active")
        if self.statusLabel:
            self.statusLabel.text = _("Connecting…")
        if self.connectButton:
            self.connectButton.enabled = False
        if self.disconnectButton:
            self.disconnectButton.enabled = True

        self._hub.connectHub(
            self.hubComboBox.currentText,
            topic,
            subscriber,
            product_name,
            product_version,
            status_callback=self._on_hub_connection_state,
        )

    def _on_disconnect(self) -> None:
        self._hub.disconnectHub()
        self._refresh_status()

    def _on_hub_connection_state(
        self, state: str, detail: Optional[Dict[str, Any]] = None
    ) -> None:
        detail = detail or {}

        if state == "connected":
            self._apply_status_style("connected")
            count = int(detail.get("message_count") or self._hub.get_message_count())
            if self.statusLabel:
                self.statusLabel.text = _connected_status_text(count)
            if self.connectButton:
                self.connectButton.enabled = False
            if self.disconnectButton:
                self.disconnectButton.enabled = True
            self._set_fields_enabled(False)
        elif state == "connecting":
            self._apply_status_style("active")
            if self.statusLabel:
                self.statusLabel.text = _("Connecting…")
            if self.connectButton:
                self.connectButton.enabled = False
            if self.disconnectButton:
                self.disconnectButton.enabled = True
            self._set_fields_enabled(False)
        elif state == "reconnecting":
            self._apply_status_style("active")
            if self.statusLabel:
                self.statusLabel.text = _("Reconnecting…")
            if self.connectButton:
                self.connectButton.enabled = False
            if self.disconnectButton:
                self.disconnectButton.enabled = True
            self._set_fields_enabled(False)
        elif state == "failed":
            self._apply_status_style("failed")
            reason = detail.get("reason") or _("Unknown error")
            if self.statusLabel:
                self.statusLabel.text = _("Cannot connect: {reason}").format(
                    reason=reason
                )
            if self.connectButton:
                self.connectButton.enabled = True
            if self.disconnectButton:
                self.disconnectButton.enabled = False
            self._set_fields_enabled(True)
        elif state == "disconnected":
            self._apply_status_style("idle")
            if self.statusLabel:
                self.statusLabel.text = _("Disconnected")
            if self.connectButton:
                self.connectButton.enabled = True
            if self.disconnectButton:
                self.disconnectButton.enabled = False
            self._set_fields_enabled(True)
        elif state == "error":
            self._apply_status_style("error")
            reason = detail.get("reason")
            if self.statusLabel:
                if reason:
                    self.statusLabel.text = _("Connection error: {reason}").format(
                        reason=reason
                    )
                else:
                    self.statusLabel.text = _("Connection error (reconnecting)")
            if self.connectButton:
                self.connectButton.enabled = False
            if self.disconnectButton:
                self.disconnectButton.enabled = True
            self._set_fields_enabled(False)

    def _refresh_status(self) -> None:
        if self._hub.isHubConnected():
            self._apply_status_style("connected")
            if self.statusLabel:
                self.statusLabel.text = _connected_status_text(
                    self._hub.get_message_count()
                )
            if self.connectButton:
                self.connectButton.enabled = False
            if self.disconnectButton:
                self.disconnectButton.enabled = True
            self._set_fields_enabled(False)
        elif not self._hub.isHubThreadRunning():
            self._apply_status_style("idle")
            if self.statusLabel:
                self.statusLabel.text = _("Disconnected")
            if self.connectButton:
                self.connectButton.enabled = True
            if self.disconnectButton:
                self.disconnectButton.enabled = False
            self._set_fields_enabled(True)
