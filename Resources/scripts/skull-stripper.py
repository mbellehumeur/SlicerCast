"""Default AIBRAIN onMessage: echo ai-results DICOM after each inbound dicom-send."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict

from Lib.cast_client import dicom_send_byte_length, dicom_send_file_name
from Lib.cast_provider_runtime import (
    get_active_resource_server_products,
    publish_dicom_send_file,
    record_dicom_send_received,
)

LOGGER = logging.getLogger("CastInterface.AIBRAIN")

_MODULE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
AI_RESULTS_DCM = os.path.join(_MODULE_ROOT, "Resources", "ai-results-mrbrain.dcm")


def onMessage(message: Dict[str, Any], provider: Any) -> None:
    event = message.get("event") or {}
    if event.get("hub.event") != "dicom-send":
        return

    LOGGER.info(
        "AIBRAIN onMessage: received dicom-send id=%s topic=%s file=%s",
        message.get("id", ""),
        event.get("hub.topic", ""),
        dicom_send_file_name(message),
    )

    topic = (event.get("hub.topic") or "").strip()
    if not topic:
        LOGGER.warning("AIBRAIN onMessage: dicom-send missing hub.topic")
        return

    byte_length = dicom_send_byte_length(message)
    entry = record_dicom_send_received(topic, byte_length)
    LOGGER.info(
        "AIBRAIN received dicom-send topic=%s size=%d at=%s",
        entry["topic"],
        entry["size"],
        entry["time"],
    )

    product_name = getattr(provider, "product_name", "") or "AIBRAIN"
    active = get_active_resource_server_products()
    if len(active) > 1:
        LOGGER.info(
            "AIBRAIN skipping demo publish; other providers are connected: %s",
            ", ".join(p for p in active if p != product_name),
        )
        return

    if not publish_dicom_send_file(product_name, topic, AI_RESULTS_DCM):
        LOGGER.warning("AIBRAIN failed to publish %s to topic %s", AI_RESULTS_DCM, topic)
    else:
        LOGGER.info(
            "AIBRAIN scheduled publish %s dicom-send to topic=%s",
            AI_RESULTS_DCM,
            topic,
        )
