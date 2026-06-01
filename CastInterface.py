"""3D Slicer scripted module: Cast Interface."""

from __future__ import annotations

import qt

from slicer.i18n import tr as _
from slicer.ScriptedLoadableModule import (
    ScriptedLoadableModule,
    ScriptedLoadableModuleWidget,
)

from Lib.CastHub import CastHubWidget
from Lib.CastImageDisplayClient import CastImageDisplayClientWidget
from Lib.CastResourceServers import CastResourceServersWidget


class CastInterface(ScriptedLoadableModule):
    def __init__(self, parent):
        ScriptedLoadableModule.__init__(self, parent)
        self.parent.title = _("Cast Interface")
        self.parent.categories = [_("Cast")]
        self.parent.dependencies = []
        self.parent.contributors = ["ProjectWeek45"]
        self.parent.helpText = _(
            """
            Cast interface for 3D Slicer: Resource Servers, Image Display client and Hub.<br><br>
            Resource Servers:
            Resource servers subscribe to all user topics for dicom events and send back results to the user.
            Each resource server connects with its own product name and onMessage script.
            The script handles producing the results from the DICOM files received.
            <br><br>
            Image Display Client:
            The image display client provide a PACS client type interface to the 3D slicer viewer.  Supported events are ImagingStudy-open, Imaging-Study-close and request for sceneview.
            <br><br>
            Hub:
            The hub is the server that distributes the messages and handles the data transfer requests over the websocket connection to each client.
            """
        )
        self.parent.acknowledgementText = _(
            """
            Cast client protocol aligned with vtk-js CastClient. <br>
            The Cast hub server lives in this extension under cast_api/ (port 2018).
            """
        )


_COLLAPSIBLE_SECTION_STYLE = """
QGroupBox {
  border: 2px solid palette(mid);
  border-radius: 6px;
  margin-top: 14px;
  padding: 12px 10px 10px 10px;
}
QGroupBox::title {
  subcontrol-origin: margin;
  subcontrol-position: top left;
  padding: 2px 8px;
  left: 8px;
}
"""


class CastInterfaceWidget(ScriptedLoadableModuleWidget):
    def __init__(self, parent=None) -> None:
        ScriptedLoadableModuleWidget.__init__(self, parent)
        self.resourceServersWidget = CastResourceServersWidget()
        self.imageDisplayClientWidget = CastImageDisplayClientWidget()
        self.hubWidget = CastHubWidget()

    @staticmethod
    def _collapsible_group_box(
        title: str, *, expanded: bool = True
    ) -> tuple[qt.QGroupBox, qt.QWidget]:
        section = qt.QGroupBox(title)
        section.setStyleSheet(_COLLAPSIBLE_SECTION_STYLE)
        section.setCheckable(True)
        section.setChecked(expanded)
        inner = qt.QWidget()
        outer = qt.QVBoxLayout(section)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(inner)
        inner.setVisible(expanded)
        section.toggled.connect(inner.setVisible)
        return section, inner

    def setup(self) -> None:
        ScriptedLoadableModuleWidget.setup(self)
        self.layout.setSpacing(10)

        hubSection, hubInner = self._collapsible_group_box(_("Hub"), expanded=True)
        self.layout.addWidget(hubSection)
        self.hubWidget.setup(hubInner)

        resourceServersSection, resourceServersInner = self._collapsible_group_box(
            _("Resource Servers"), expanded=True
        )
        self.layout.addWidget(resourceServersSection)
        self.resourceServersWidget.setup(resourceServersInner)

        clientSection, clientInner = self._collapsible_group_box(
            _("Image Display Client"), expanded=False
        )
        self.layout.addWidget(clientSection)
        self.imageDisplayClientWidget.setup(clientInner)

        self.layout.addStretch(1)

    def cleanup(self) -> None:
        self.resourceServersWidget.cleanup()
        self.imageDisplayClientWidget.cleanup()
        self.hubWidget.cleanup()

    def enter(self) -> None:
        self.resourceServersWidget.enter()
        self.imageDisplayClientWidget.enter()
        self.hubWidget.enter()

    def exit(self) -> None:
        self.resourceServersWidget.exit()
        self.imageDisplayClientWidget.exit()
        self.hubWidget.exit()
