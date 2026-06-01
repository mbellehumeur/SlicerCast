"""Cast Interface — Resource Servers subsection (hub connect, onMessage scripts)."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import logging
import os
import queue
import sys
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urljoin

import qt

import slicer
from slicer.i18n import tr as _

from .cast_client import CastClientOptions, HubConfig, SessionConfig, SlicerCastClient

LOGGER = logging.getLogger("CastInterface.ResourceServers")
LOGGER.setLevel(logging.INFO)

# --- Hub config (align with Viewers/platform/app/public/config/cast.js) ---

DEFAULT_HUB_NAME = "SLICER-HUB"

_HUB_NAME_ALIASES = {
    "VOLVIEW-HUB": "SLICER-HUB",
    "VOLVIEW-HUB-CLOUD": "SLICER-HUB-CLOUD",
}

HUBS: Dict[str, Dict[str, Any]] = {
    "SLICER-HUB-CLOUD": {
        "hub_endpoint": "https://cast-hub-g6abetanhjesb6cx.westeurope-01.azurewebsites.net/api/hub",
        "authorization_endpoint": "https://cast-hub-g6abetanhjesb6cx.westeurope-01.azurewebsites.net/oauth/authorize",
        "token_endpoint": "https://cast-hub-g6abetanhjesb6cx.westeurope-01.azurewebsites.net/oauth/token",
        "client_id": "130c3d9c-4157-4dd1-aa1d-slicer",
        "client_secret": "0c931e4163c1bc984b5266735dc652a2f1e3e6e8d8cfe5b0855f433cc8ff018f",
        "lease": 999,
    },
    "SLICER-HUB": {
        "hub_endpoint": "http://127.0.0.1:2018/api/hub",
        "authorization_endpoint": "http://127.0.0.1:2018/oauth/authorize",
        "token_endpoint": "http://127.0.0.1:2018/oauth/token",
        "client_id": "130c3d9c-4157-4dd1-aa1d-slicer",
        "client_secret": "0c931e4163c1bc984b5266735dc652a2f1e3e6e8d8cfe5b0855f433cc8ff018f",
        "lease": 999,
    },
}


def normalize_hub_name(name: str) -> str:
    """Map removed presets and invalid values to a configured Slicer hub."""
    key = str(name or "").strip()
    if not key:
        return DEFAULT_HUB_NAME
    key = _HUB_NAME_ALIASES.get(key, key)
    if key in HUBS:
        return key
    return DEFAULT_HUB_NAME

TOPIC = "*"
DEFAULT_PRODUCT_NAME = "AIBRAIN"
DEFAULT_PRODUCT_VERSION = "1.0"
DEFAULT_DESCRIPTION = "Brain lesions segmentator"


def default_aibrain_script_path() -> str:
    lib_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(
        os.path.dirname(lib_dir), "Resources", "scripts", "aibrain_on_message.py"
    )


def default_totalsegmentator_script_path() -> str:
    lib_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(
        os.path.dirname(lib_dir),
        "Resources",
        "scripts",
        "total_segmentator_inline.py",
    )


DEFAULT_AIBRAIN_SCRIPT_PATH = default_aibrain_script_path()
DEFAULT_TOTALSEG_SCRIPT_PATH = default_totalsegmentator_script_path()
DEFAULT_TOTALSEG_PRODUCT_NAME = "TOTALSEG"
DEFAULT_TOTALSEG_DESCRIPTION = "Total Segmentator CT segmentation (DICOM RT Struct out)"
EVENTS = ["dicom-send", "nifti-send"]
TOTALSEG_EVENTS = ["dicom-send", "nifti-send"]
TOTALSEG_SCRIPT_BASENAME = "total_segmentator_inline.py"
ACTORS = ["EC"]
USER_NAME = "3dslicer-server"
AUTO_RECONNECT = True
# Main-thread queue poll while this module is active (avoid singleShot(0) spin).
MAIN_QUEUE_TIMER_MS = 50


@dataclass
class ResourceServerConfig:
    hub_name: str
    product_name: str
    product_version: str
    description: str
    script_path: str


def is_totalsegmentator_resource_server(cfg: ResourceServerConfig) -> bool:
    """True when this row uses the TotalSegmentator onMessage script."""
    script = (cfg.script_path or "").replace("\\", "/").lower()
    if script.endswith(f"/{TOTALSEG_SCRIPT_BASENAME}") or script.endswith(
        TOTALSEG_SCRIPT_BASENAME
    ):
        return True
    if script.endswith("/total_segmentator.py") or script.endswith(
        "total_segmentator.py"
    ):
        return True
    name = (cfg.product_name or "").strip().upper()
    return name == DEFAULT_TOTALSEG_PRODUCT_NAME or name.startswith("TOTALSEG")


def subscribe_events_for_resource_server(cfg: ResourceServerConfig) -> List[str]:
    if is_totalsegmentator_resource_server(cfg):
        return list(TOTALSEG_EVENTS)
    return list(EVENTS)


DEFAULT_RESOURCE_SERVERS = [
    ResourceServerConfig(
        DEFAULT_HUB_NAME,
        DEFAULT_PRODUCT_NAME,
        DEFAULT_PRODUCT_VERSION,
        DEFAULT_DESCRIPTION,
        DEFAULT_AIBRAIN_SCRIPT_PATH,
    ),
    ResourceServerConfig(
        DEFAULT_HUB_NAME,
        DEFAULT_TOTALSEG_PRODUCT_NAME,
        DEFAULT_PRODUCT_VERSION,
        DEFAULT_TOTALSEG_DESCRIPTION,
        DEFAULT_TOTALSEG_SCRIPT_PATH,
    ),
]

_SETTINGS_GROUP = "CastInterface"
_SETTINGS_KEY_RESOURCE_SERVERS = "resourceServers"
_SETTINGS_KEY_RESOURCE_SERVERS_LEGACY = "serviceProviders"

EMPTY_FHIRCAST_CONTEXT = {"context.type": "", "context": []}


def _config_from_dict(data: Dict[str, Any]) -> ResourceServerConfig:
    return ResourceServerConfig(
        hub_name=normalize_hub_name(str(data.get("hub_name") or DEFAULT_HUB_NAME)),
        product_name=str(data.get("product_name") or DEFAULT_PRODUCT_NAME),
        product_version=str(data.get("product_version") or DEFAULT_PRODUCT_VERSION),
        description=str(data.get("description") or DEFAULT_DESCRIPTION),
        script_path=str(data.get("script_path") or DEFAULT_AIBRAIN_SCRIPT_PATH),
    )


def load_saved_resource_servers() -> List[ResourceServerConfig]:
    settings = qt.QSettings()
    settings.beginGroup(_SETTINGS_GROUP)
    raw = settings.value(_SETTINGS_KEY_RESOURCE_SERVERS, "")
    if not raw:
        raw = settings.value(_SETTINGS_KEY_RESOURCE_SERVERS_LEGACY, "")
    settings.endGroup()
    if not raw:
        return []
    try:
        payload = json.loads(str(raw))
    except (json.JSONDecodeError, TypeError) as exc:
        LOGGER.warning("Could not load saved resource servers: %s", exc)
        return []
    if not isinstance(payload, list):
        return []
    servers: List[ResourceServerConfig] = []
    for item in payload:
        if isinstance(item, dict):
            servers.append(_config_from_dict(item))
    return servers


def save_resource_servers_to_settings(
    servers: List[ResourceServerConfig],
) -> None:
    settings = qt.QSettings()
    settings.beginGroup(_SETTINGS_GROUP)
    settings.setValue(
        _SETTINGS_KEY_RESOURCE_SERVERS,
        json.dumps([asdict(cfg) for cfg in servers]),
    )
    settings.endGroup()


def hub_admin_url_for_name(hub_name: str) -> Optional[str]:
    """Admin portal URL for a configured hub (``hub_endpoint`` + ``admin``)."""
    hub_def = HUBS.get(hub_name)
    if not hub_def:
        return None
    hub_endpoint = str(hub_def.get("hub_endpoint", "")).strip()
    if not hub_endpoint:
        return None
    hub_base = hub_endpoint if hub_endpoint.endswith("/") else f"{hub_endpoint}/"
    admin_path = (
        "admin?theme=3dslicer"
        if hub_name in ("SLICER-HUB", "SLICER-HUB-CLOUD")
        else "admin"
    )
    return urljoin(hub_base, admin_path)


def local_slicer_hub_admin_url(port: int) -> str:
    """Admin URL for the embedded local hub on ``port`` (3D Slicer theme)."""
    hub_base = f"http://127.0.0.1:{port}/api/hub/"
    return urljoin(hub_base, "admin?theme=3dslicer")


from .resource_server_hub import (  # noqa: E402
    ResourceServerHubConnection,
    disconnect_all_active_connections,
)


def run_resource_server_on_message(
    resource_server: ResourceServerConfig, message: Dict[str, Any]
) -> None:
    """Load resource server script and call ``onMessage(message, resource_server)`` if defined."""
    path = (resource_server.script_path or "").strip()
    if not path:
        return
    if not os.path.isfile(path):
        LOGGER.warning("Resource server script not found: %s", path)
        return
    cast_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if cast_root not in sys.path:
        sys.path.insert(0, cast_root)

    digest = hashlib.md5(os.path.normpath(path).encode("utf-8")).hexdigest()[:16]
    module_name = f"CastInterface_rs_{digest}"
    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            LOGGER.warning("Could not load resource server script: %s", path)
            return
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        handler = getattr(module, "onMessage", None)
        if not callable(handler):
            LOGGER.warning("Script has no onMessage callable: %s", path)
            return
        handler(message, resource_server)
    except Exception as exc:
        LOGGER.exception(
            "Resource server onMessage failed product=%s script=%s: %s",
            resource_server.product_name,
            path,
            exc,
        )


def build_cast_client(
    hub_name: str,
    product_name: str,
    product_version: str,
    *,
    events: Optional[List[str]] = None,
) -> SlicerCastClient:
    if hub_name not in HUBS:
        raise KeyError(f"Unknown hub {hub_name!r}; choose from {list(HUBS)}")

    hub_def = HUBS[hub_name]
    hub = HubConfig(
        hub_endpoint=hub_def["hub_endpoint"],
        authorization_endpoint=hub_def["authorization_endpoint"],
        token_endpoint=hub_def["token_endpoint"],
        client_id=hub_def["client_id"],
        client_secret=hub_def["client_secret"],
    )
    session = SessionConfig(
        topic=TOPIC,
        product_name=product_name,
        product_version=product_version,
        actors=list(ACTORS),
        events=list(events if events is not None else EVENTS),
        lease=int(hub_def["lease"]),
        user_name=USER_NAME,
        default_target_actor="ID",
    )
    options = CastClientOptions(
        auto_reconnect=AUTO_RECONNECT,
    )
    return SlicerCastClient(hub, session, options)


_PROVIDER_FRAME_STYLE = """
QFrame#CastResourceServerFrame {
  border: 3px solid palette(dark);
  border-radius: 8px;
  background-color: palette(base);
  margin: 4px 2px;
}
"""

_STATUS_TEXT_STYLE_IDLE = "color: palette(text);"
_STATUS_TEXT_STYLE_CONNECTED = "color: #2e7d32; font-weight: bold;"
_STATUS_TEXT_STYLE_ACTIVE = "color: #1a5f9e; font-weight: bold;"
_STATUS_TEXT_STYLE_ERROR = "color: #c45c26; font-weight: bold;"


def _connected_status_text(message_count: int) -> str:
    if message_count == 1:
        return _("Connected (1 case received)")
    return _("Connected ({count} cases received)").format(count=message_count)


def format_on_message_script_display(path: str) -> str:
    """Short label: ``./<script.py>``."""
    normalized = os.path.normpath((path or "").strip())
    if not normalized:
        return ""
    return f"./{os.path.basename(normalized)}"


def _provider_form_label(text: str) -> qt.QLabel:
    label = qt.QLabel(text)
    label.setAlignment(qt.Qt.AlignLeft | qt.Qt.AlignVCenter)
    return label


def _provider_action_row(
    *widgets: qt.QWidget,
    fill_width: bool = False,
    align_left: bool = False,
) -> qt.QWidget:
    host = qt.QWidget()
    layout = qt.QHBoxLayout(host)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(6)
    if fill_width:
        host.setSizePolicy(qt.QSizePolicy.Expanding, qt.QSizePolicy.Fixed)
        for widget in widgets:
            widget.setSizePolicy(qt.QSizePolicy.Expanding, qt.QSizePolicy.Fixed)
            layout.addWidget(widget, 1)
    elif align_left:
        for widget in widgets:
            widget.setSizePolicy(qt.QSizePolicy.Fixed, qt.QSizePolicy.Fixed)
            layout.addWidget(widget)
        layout.addStretch(1)
    elif len(widgets) == 2:
        host.setSizePolicy(qt.QSizePolicy.Expanding, qt.QSizePolicy.Fixed)
        for widget in widgets:
            widget.setSizePolicy(qt.QSizePolicy.Fixed, qt.QSizePolicy.Fixed)
        layout.addWidget(widgets[0])
        layout.addStretch(1)
        layout.addWidget(widgets[1])
    else:
        layout.addStretch(1)
        for widget in widgets:
            widget.setSizePolicy(qt.QSizePolicy.Fixed, qt.QSizePolicy.Fixed)
            layout.addWidget(widget)
    return host


def _provider_action_column_width(
    *button_rows: tuple[qt.QWidget, ...],
) -> int:
    spacing = 6
    row_widths = []
    for widgets in button_rows:
        width = 0
        for index, widget in enumerate(widgets):
            if index > 0:
                width += spacing
            width += widget.sizeHint.width()
        row_widths.append(width)
    return max(row_widths, default=0) + 8


class ResourceServerRow:
    """One resource server row (QFrame)."""

    def __init__(
        self,
        parent: qt.QWidget,
        widget: "CastResourceServersWidget",
        config: Optional[ResourceServerConfig] = None,
        on_remove: Optional[Callable[["ResourceServerRow"], None]] = None,
    ) -> None:
        self._widget = widget
        self._on_remove = on_remove
        self._script_path = ""
        self.hub = ResourceServerHubConnection(widget.post_ui)
        cfg = config or ResourceServerConfig(
            DEFAULT_HUB_NAME,
            DEFAULT_PRODUCT_NAME,
            DEFAULT_PRODUCT_VERSION,
            DEFAULT_DESCRIPTION,
            DEFAULT_AIBRAIN_SCRIPT_PATH,
        )

        self.frame: Optional[qt.QFrame] = qt.QFrame(parent)
        self.frame.setObjectName("CastResourceServerFrame")
        self.frame.setFrameShape(qt.QFrame.NoFrame)
        self.frame.setStyleSheet(_PROVIDER_FRAME_STYLE)
        self.frame.setMinimumHeight(160)
        frame_layout = qt.QVBoxLayout(self.frame)
        frame_layout.setContentsMargins(12, 12, 12, 12)

        self.connectButton = qt.QPushButton(_("Connect"))
        self.connectButton.clicked.connect(self._on_connect)
        self.disconnectButton = qt.QPushButton(_("Disconnect"))
        self.disconnectButton.clicked.connect(self._on_disconnect)
        self.disconnectButton.enabled = False

        self.descriptionEdit = qt.QLineEdit(cfg.description)
        self.descriptionEdit.setPlaceholderText(_("Description"))

        product_row = qt.QWidget()
        product_row_layout = qt.QHBoxLayout(product_row)
        product_row_layout.setContentsMargins(0, 0, 0, 0)
        self.productNameEdit = qt.QLineEdit(cfg.product_name)
        self.productNameEdit.setPlaceholderText(_("Product name"))
        self.versionLabel = qt.QLabel(_("Version:"))
        self.versionLabel.setAlignment(qt.Qt.AlignLeft | qt.Qt.AlignVCenter)
        self.versionEdit = qt.QLineEdit(cfg.product_version)
        self.versionEdit.setMaximumWidth(80)
        product_row_layout.addWidget(self.productNameEdit, 1)

        self.scriptPathEdit = qt.QLineEdit()
        self.scriptPathEdit.setReadOnly(True)
        self.scriptPathEdit.setPlaceholderText(_("Browse to choose onMessage script (.py)"))
        self._set_script_path(cfg.script_path)
        self.browseScriptButton = qt.QPushButton(_("Browse…"))
        self.browseScriptButton.clicked.connect(self._on_browse_script)
        self.editScriptButton = qt.QPushButton(_("Edit"))
        self.editScriptButton.setToolTip(
            _("Open the onMessage script in your configured editor")
        )
        self.editScriptButton.clicked.connect(self._on_edit_script)

        self.hubComboBox = qt.QComboBox()
        for hub_name in sorted(HUBS.keys()):
            self.hubComboBox.addItem(hub_name)
        hub_index = self.hubComboBox.findText(
            normalize_hub_name(cfg.hub_name or DEFAULT_HUB_NAME)
        )
        if hub_index >= 0:
            self.hubComboBox.setCurrentIndex(hub_index)
        hub_metrics = qt.QFontMetrics(self.hubComboBox.font)
        max_hub_text_w = max(
            hub_metrics.horizontalAdvance(name) for name in HUBS.keys()
        )
        self.hubComboBox.setMaximumWidth(max_hub_text_w + 48)

        self.openHubButton = qt.QPushButton(_("Open Hub Portal"))
        self.openHubButton.setToolTip(
            _("Open the hub admin portal in your default browser")
        )
        self.openHubButton.clicked.connect(self._on_open_hub)

        self.statusLabel = qt.QLabel(_("Disconnected"))
        self.saveButton = qt.QPushButton(_("Save"))
        self.saveButton.clicked.connect(self._on_save_clicked)
        self.removeButton = qt.QPushButton(_("Remove"))
        self.removeButton.clicked.connect(self._on_remove_clicked)
        self._apply_status_style("idle")

        action_col_width = _provider_action_column_width(
            (self.connectButton, self.disconnectButton),
        )

        content = qt.QWidget()
        grid = qt.QGridLayout(content)
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)
        grid.setColumnStretch(1, 1)
        grid.setColumnStretch(2, 0)
        grid.setColumnMinimumWidth(2, action_col_width)

        grid_row = 0
        grid.addWidget(_provider_form_label(_("Description:")), grid_row, 0)
        grid.addWidget(self.descriptionEdit, grid_row, 1)
        grid.addWidget(
            _provider_action_row(self.connectButton, self.disconnectButton),
            grid_row,
            2,
        )
        grid_row += 1
        grid.addWidget(_provider_form_label(_("Hub:")), grid_row, 0)
        grid.addWidget(self.hubComboBox, grid_row, 1, qt.Qt.AlignLeft)
        grid.addWidget(
            _provider_action_row(self.openHubButton, fill_width=True),
            grid_row,
            2,
        )
        grid_row += 1
        grid.addWidget(_provider_form_label(_("Product:")), grid_row, 0)
        grid.addWidget(product_row, grid_row, 1)
        grid.addWidget(
            _provider_action_row(
                self.versionLabel, self.versionEdit, align_left=True
            ),
            grid_row,
            2,
        )
        grid_row += 1
        grid.addWidget(_provider_form_label(_("onMessage script:")), grid_row, 0)
        grid.addWidget(self.scriptPathEdit, grid_row, 1)
        grid.addWidget(
            _provider_action_row(self.browseScriptButton, self.editScriptButton),
            grid_row,
            2,
        )
        grid_row += 1
        grid.addWidget(_provider_form_label(_("Status:")), grid_row, 0)
        grid.addWidget(self.statusLabel, grid_row, 1)
        grid.addWidget(
            _provider_action_row(self.saveButton, self.removeButton),
            grid_row,
            2,
        )

        frame_layout.addWidget(content)

    def _apply_status_style(self, variant: str) -> None:
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

    def _on_open_hub(self) -> None:
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

    def _set_script_path(self, path: str) -> None:
        self._script_path = os.path.normpath((path or "").strip())
        if self.scriptPathEdit:
            self.scriptPathEdit.setText(
                format_on_message_script_display(self._script_path)
            )
            tip = self._script_path or _("No script selected")
            self.scriptPathEdit.setToolTip(tip)

    def _on_browse_script(self) -> None:
        start = self._script_path
        if not start:
            start = slicer.app.startupWorkingPath
        selected = qt.QFileDialog.getOpenFileName(
            self.frame,
            _("Select onMessage script"),
            start,
            _("Python scripts (*.py);;All files (*.*)"),
        )
        path = ""
        if isinstance(selected, (tuple, list)):
            if selected:
                path = str(selected[0]).strip()
        elif selected:
            # Slicer PythonQt may return a single path string; [0] would be "C" on Windows.
            path = str(selected).strip()
        if path:
            self._set_script_path(path)

    def _on_edit_script(self) -> None:
        path = self._script_path
        if not path:
            slicer.util.warningDisplay(_("Enter or choose a script path first."))
            return
        if not os.path.isfile(path):
            slicer.util.warningDisplay(
                _("onMessage script not found:\n{path}").format(path=path)
            )
            return
        file_url = qt.QUrl.fromLocalFile(os.path.normpath(path))
        if not qt.QDesktopServices.openUrl(file_url):
            slicer.util.warningDisplay(
                _("Could not open script:\n{path}").format(path=path)
            )

    def _on_save_clicked(self) -> None:
        self._widget.save_row(self)

    def _on_remove_clicked(self) -> None:
        if not self._on_remove:
            return
        cfg = self.to_config()
        label = cfg.product_name or cfg.description or _("this resource server")
        if not slicer.util.confirmYesNoDisplay(
            _('Remove resource server "{name}"?').format(name=label),
            windowTitle=_("Remove resource server"),
            parent=self.frame,
        ):
            return
        self._on_remove(self)

    def _on_connect(self) -> None:
        self._widget.connect_row(self)

    def _on_disconnect(self) -> None:
        self._widget.disconnect_row(self)

    def to_config(self) -> ResourceServerConfig:
        return ResourceServerConfig(
            hub_name=self.hubComboBox.currentText,
            product_name=self.productNameEdit.text.strip(),
            product_version=self.versionEdit.text.strip(),
            description=self.descriptionEdit.text.strip(),
            script_path=self._script_path,
        )

    def set_connection_locked(self, locked: bool) -> None:
        """While connected: hub/script disabled (grayed); identity fields read-only."""
        editable = not locked
        self.hubComboBox.setEnabled(editable)
        self.scriptPathEdit.setEnabled(editable)
        self.browseScriptButton.setEnabled(editable)
        self.editScriptButton.setEnabled(editable)
        self.descriptionEdit.setReadOnly(locked)
        self.productNameEdit.setReadOnly(locked)
        self.versionEdit.setReadOnly(locked)

    def set_action_buttons_enabled(self, enabled: bool) -> None:
        self.saveButton.setEnabled(enabled)
        self.removeButton.setEnabled(enabled)

    def _show_disconnect_active(self, *, focus_disconnect: bool = False) -> None:
        self.connectButton.enabled = False
        self.disconnectButton.enabled = True
        if focus_disconnect:
            self.disconnectButton.setFocus(qt.Qt.OtherFocusReason)

    def _on_hub_connection_state(
        self, state: str, _detail: Optional[Dict[str, Any]] = None
    ) -> None:
        if state == "connected":
            self._apply_status_style("connected")
            count = (_detail or {}).get("message_count")
            if count is None:
                count = self.hub.get_message_count()
            self.statusLabel.text = _connected_status_text(int(count))
            self._show_disconnect_active()
        elif state == "connecting":
            self._apply_status_style("active")
            self.statusLabel.text = _("Connecting…")
            self._show_disconnect_active()
        elif state == "reconnecting":
            self._apply_status_style("active")
            self.statusLabel.text = _("Reconnecting…")
            self._show_disconnect_active()
        elif state == "failed":
            self._apply_status_style("failed")
            reason = (_detail or {}).get("reason") or _("Unknown error")
            self.statusLabel.text = _("Cannot connect: {reason}").format(
                reason=reason
            )
            self.connectButton.enabled = True
            self.disconnectButton.enabled = False
            self.set_connection_locked(False)
            self.set_action_buttons_enabled(True)
            self._widget._update_resource_server_remove_buttons()
        elif state == "disconnected":
            self._apply_status_style("idle")
            self.statusLabel.text = _("Disconnected")
            self.connectButton.enabled = True
            self.disconnectButton.enabled = False
            self.set_connection_locked(False)
            self.set_action_buttons_enabled(True)
            self._widget._update_resource_server_remove_buttons()
        elif state == "error":
            self._apply_status_style("error")
            reason = (_detail or {}).get("reason")
            if reason:
                self.statusLabel.text = _("Connection error: {reason}").format(
                    reason=reason
                )
            else:
                self.statusLabel.text = _("Connection error (reconnecting)")
            self._show_disconnect_active()


class CastResourceServersWidget:
    """UI and actions for the Resource Servers section."""

    def __init__(self) -> None:
        self._resourceServerRows: List[ResourceServerRow] = []
        self._section: Optional[qt.QWidget] = None
        self.resourceServersListLayout: Optional[qt.QVBoxLayout] = None
        self._main_queue: queue.Queue[Callable[[], None]] = queue.Queue()
        self._main_queue_running = False
        self._setup_complete = False
        self._main_queue_timer = qt.QTimer()
        self._main_queue_timer.setInterval(MAIN_QUEUE_TIMER_MS)
        self._main_queue_timer.timeout.connect(self._main_queue_process)

    def setup(self, section: qt.QWidget) -> None:
        if self._setup_complete:
            return
        self._setup_complete = True
        self._section = section
        layout = qt.QVBoxLayout(section)

        self.resourceServersScrollArea = qt.QScrollArea()
        self.resourceServersScrollArea.setWidgetResizable(True)
        self.resourceServersScrollArea.setFrameShape(qt.QFrame.NoFrame)
        self.resourceServersScrollArea.setMinimumHeight(320)
        self.resourceServersScrollArea.setMaximumHeight(640)
        self.resourceServersListWidget = qt.QWidget()
        self.resourceServersListLayout = qt.QVBoxLayout(self.resourceServersListWidget)
        self.resourceServersListLayout.setContentsMargins(0, 0, 0, 0)
        self.resourceServersListLayout.setSpacing(18)
        self.resourceServersScrollArea.setWidget(self.resourceServersListWidget)
        layout.addWidget(self.resourceServersScrollArea)

        addRemoveRow = qt.QHBoxLayout()
        self.addResourceServerButton = qt.QPushButton(_("Add resource server"))
        self.addResourceServerButton.clicked.connect(self.onAddResourceServer)
        addRemoveRow.addWidget(self.addResourceServerButton)
        addRemoveRow.addStretch(1)
        layout.addLayout(addRemoveRow)

        try:
            saved = load_saved_resource_servers()
            servers = saved if saved else list(DEFAULT_RESOURCE_SERVERS)
            for server in servers:
                self._add_resource_server_row(server)
        except Exception as exc:
            LOGGER.exception("Failed to create default resource server row: %s", exc)
            slicer.util.errorDisplay(
                f"Cast Interface UI error (resource server row): {exc}"
            )
        self.resourceServersListLayout.addStretch(1)

    def cleanup(self) -> None:
        self.exit()
        for row in list(self._resourceServerRows):
            row.hub.disconnectHub()
        disconnect_all_active_connections()
        self._main_queue_drain()

    def enter(self) -> None:
        self._main_queue_running = True
        self._main_queue_timer.start()
        self._main_queue_drain()
        for row in self._resourceServerRows:
            self._update_row_status(row)

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
            LOGGER.exception("Cast main queue error: %s", exc)

    def _main_queue_process(self) -> None:
        if not self._main_queue_running:
            return
        self._main_queue_drain()

    def _add_resource_server_row(
        self, config: Optional[ResourceServerConfig] = None
    ) -> ResourceServerRow:
        if self.resourceServersListLayout is None:
            raise RuntimeError("CastResourceServersWidget.setup not called")
        try:
            row = ResourceServerRow(
                self.resourceServersListWidget,
                self,
                config=config,
                on_remove=self._remove_resource_server_row,
            )
        except Exception as exc:
            LOGGER.exception("Failed to add resource server row: %s", exc)
            slicer.util.errorDisplay(f"Could not add resource server: {exc}")
            raise
        self._resourceServerRows.append(row)
        insert_at = max(0, self.resourceServersListLayout.count() - 1)
        self.resourceServersListLayout.insertWidget(insert_at, row.frame)
        row.frame.show()
        self.resourceServersListWidget.adjustSize()
        self._update_resource_server_remove_buttons()
        return row

    def _remove_resource_server_row(self, row: ResourceServerRow) -> None:
        if len(self._resourceServerRows) <= 1:
            return
        if row not in self._resourceServerRows:
            return
        row.hub.disconnectHub()
        self._resourceServerRows.remove(row)
        row.removeButton.setEnabled(False)
        qt.QTimer.singleShot(0, lambda r=row: self._destroy_resource_server_row(r))

    def _destroy_resource_server_row(self, row: ResourceServerRow) -> None:
        if self.resourceServersListLayout is None:
            return
        frame = row.frame
        if frame is None:
            return
        row.frame = None
        self.resourceServersListLayout.removeWidget(frame)
        frame.setParent(None)
        frame.deleteLater()
        self.resourceServersListWidget.adjustSize()
        self._update_resource_server_remove_buttons()

    def _update_resource_server_remove_buttons(self) -> None:
        allow_remove = len(self._resourceServerRows) > 1
        for row in self._resourceServerRows:
            row.removeButton.setEnabled(allow_remove)

    def onAddResourceServer(self) -> None:
        try:
            self._add_resource_server_row(
                ResourceServerConfig(
                    DEFAULT_HUB_NAME,
                    DEFAULT_PRODUCT_NAME,
                    DEFAULT_PRODUCT_VERSION,
                    DEFAULT_DESCRIPTION,
                    DEFAULT_AIBRAIN_SCRIPT_PATH,
                )
            )
        except Exception:
            pass

    def get_resource_servers(self) -> List[ResourceServerConfig]:
        return [row.to_config() for row in self._resourceServerRows]

    def save_row(self, row: ResourceServerRow) -> None:
        cfg = row.to_config()
        if not cfg.product_name:
            slicer.util.errorDisplay(_("Enter a product name for this resource server."))
            return
        script_path = (cfg.script_path or "").strip()
        if script_path and not os.path.isfile(script_path):
            slicer.util.warningDisplay(
                _("onMessage script not found:\n{path}").format(path=script_path)
            )
            return
        try:
            save_resource_servers_to_settings(self.get_resource_servers())
            LOGGER.info(
                "Saved %d resource server(s) to settings",
                len(self._resourceServerRows),
            )
        except Exception as exc:
            LOGGER.exception("Failed to save resource servers: %s", exc)
            slicer.util.errorDisplay(
                _("Could not save resource servers: {error}").format(error=exc)
            )

    def connect_row(self, row: ResourceServerRow) -> None:
        cfg = row.to_config()
        if not cfg.product_name:
            slicer.util.errorDisplay(_("Enter a product name for this resource server."))
            return
        if row.hub.isHubThreadRunning():
            return
        try:
            row.set_connection_locked(True)
            row.set_action_buttons_enabled(False)
            row._apply_status_style("active")
            row.statusLabel.text = _("Connecting…")
            row._show_disconnect_active(focus_disconnect=True)
            row.hub.connectHub(
                cfg.hub_name,
                cfg.product_name,
                cfg.product_version or DEFAULT_PRODUCT_VERSION,
                cfg.script_path,
                cfg,
                status_callback=row._on_hub_connection_state,
            )
        except Exception as exc:
            LOGGER.warning("Cast connect failed: %s", exc)
            self._update_row_status(row)

    def disconnect_row(self, row: ResourceServerRow) -> None:
        row.hub.disconnectHub()
        self._update_row_status(row)

    def _update_row_status(self, row: ResourceServerRow) -> None:
        if row.hub.isHubConnected():
            row._apply_status_style("connected")
            row.statusLabel.text = _connected_status_text(row.hub.get_message_count())
            row._show_disconnect_active()
            row.set_connection_locked(True)
            row.set_action_buttons_enabled(False)
        else:
            row._apply_status_style("idle")
            row.statusLabel.text = _("Disconnected")
            row.connectButton.enabled = True
            row.disconnectButton.enabled = False
            row.set_connection_locked(False)
            row.set_action_buttons_enabled(True)
            self._update_resource_server_remove_buttons()
