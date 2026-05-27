"""Helpers for service-provider onMessage scripts (publish, dicom-send payloads)."""

from __future__ import annotations

import base64
import binascii
import logging
import os
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .cast_client import (
    _dicom_send_context_items,
    generate_message_id,
)

if TYPE_CHECKING:
    from .service_provider_hub import ServiceProviderHubConnection

LOGGER = logging.getLogger("CastInterface.ProviderRuntime")
if not LOGGER.handlers:
    _stderr_handler = logging.StreamHandler(sys.stderr)
    _stderr_handler.setFormatter(
        logging.Formatter("[%(name)s] %(levelname)s: %(message)s")
    )
    LOGGER.addHandler(_stderr_handler)
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False

_connections: Dict[str, "ServiceProviderHubConnection"] = {}
_receive_log: List[Dict[str, Any]] = []


def register_connection(
    product_name: str, connection: "ServiceProviderHubConnection"
) -> None:
    key = (product_name or "").strip()
    if key:
        _connections[key] = connection


def unregister_connection(product_name: str) -> None:
    key = (product_name or "").strip()
    if key:
        _connections.pop(key, None)


def get_receive_log() -> List[Dict[str, Any]]:
    return list(_receive_log)


def get_active_provider_products() -> List[str]:
    """Product names of service providers currently connected to the hub."""
    return list(_connections.keys())


def _dicom_bytes_from_resource(resource: Dict[str, Any]) -> Optional[bytes]:
    data = resource.get("data")
    if isinstance(data, (bytes, bytearray)) and len(data) > 0:
        return bytes(data)
    if isinstance(data, str) and data.strip():
        try:
            raw = base64.standard_b64decode(data)
        except (binascii.Error, ValueError):
            return None
        return raw if raw else None
    return None


def _file_name_from_resource(resource: Dict[str, Any], index: int) -> str:
    name = resource.get("fileName")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return f"dicom-send-{index + 1}.dcm"


def dicom_send_is_complete(event: Dict[str, Any]) -> bool:
    for item in _dicom_send_context_items(event):
        if str(item.get("status", "")).strip().lower() == "complete":
            return True
    return False


def dicom_transfer_id_from_event(event: Dict[str, Any]) -> Optional[str]:
    for item in _dicom_send_context_items(event):
        transfer_id = item.get("dicomTransferId")
        if isinstance(transfer_id, str) and transfer_id.strip():
            return transfer_id.strip()
    return None


def extract_all_dicom_send_payloads(
    message: Dict[str, Any],
) -> List[Tuple[str, bytes]]:
    """Return every ``(fileName, raw bytes)`` in a ``dicom-send`` context list."""
    event = message.get("event") or {}
    if not isinstance(event, dict) or event.get("hub.event") != "dicom-send":
        return []
    if dicom_send_is_complete(event):
        return []

    context = event.get("context")
    if isinstance(context, dict) and isinstance(context.get("resource"), dict):
        resource = context["resource"]
        raw = _dicom_bytes_from_resource(resource)
        if raw:
            file_name = context.get("fileName")
            if isinstance(file_name, str) and file_name.strip():
                return [(file_name.strip(), raw)]
            return [(_file_name_from_resource(resource, 0), raw)]

    payloads: List[Tuple[str, bytes]] = []
    for index, item in enumerate(_dicom_send_context_items(event)):
        resource = item.get("resource")
        if not isinstance(resource, dict):
            continue
        raw = _dicom_bytes_from_resource(resource)
        if raw:
            payloads.append((_file_name_from_resource(resource, index), raw))
    return payloads


def _nifti_file_name_from_resource(resource: Dict[str, Any], index: int) -> str:
    name = resource.get("fileName")
    if isinstance(name, str) and name.strip():
        return name.strip()
    if index == 0:
        return "nifti-send.nii.gz"
    return f"nifti-send-{index + 1}.nii.gz"


def extract_all_nifti_send_payloads(
    message: Dict[str, Any],
) -> List[Tuple[str, bytes]]:
    """Return every ``(fileName, raw bytes)`` in a ``nifti-send`` context list."""
    event = message.get("event") or {}
    if not isinstance(event, dict) or event.get("hub.event") != "nifti-send":
        return []

    payloads: List[Tuple[str, bytes]] = []
    for index, item in enumerate(_dicom_send_context_items(event)):
        resource = item.get("resource")
        if not isinstance(resource, dict):
            continue
        raw = _dicom_bytes_from_resource(resource)
        if raw:
            payloads.append((_nifti_file_name_from_resource(resource, index), raw))
    return payloads


def extract_dicom_send_payload(
    message: Dict[str, Any],
) -> Optional[Tuple[str, bytes]]:
    """Return the first ``(fileName, raw bytes)`` from a ``dicom-send`` message."""
    payloads = extract_all_dicom_send_payloads(message)
    if not payloads:
        return None
    return payloads[0]


def record_dicom_send_received(topic: str, byte_length: int) -> Dict[str, Any]:
    entry = {
        "topic": topic,
        "size": byte_length,
        "time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    _receive_log.append(entry)
    return entry


def record_nifti_send_received(topic: str, byte_length: int) -> Dict[str, Any]:
    entry = {
        "topic": topic,
        "event": "nifti-send",
        "size": byte_length,
        "time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    _receive_log.append(entry)
    return entry


def build_dicom_send_publish_message(
    topic: str, file_path: str
) -> Dict[str, Any]:
    path = os.path.normpath(file_path)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"DICOM file not found: {path}")

    with open(path, "rb") as dcm_file:
        raw = dcm_file.read()

    file_name = os.path.basename(path)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "id": generate_message_id(),
        "event": {
            "hub.topic": topic,
            "hub.event": "dicom-send",
            "context": [
                {
                    "key": "dicom",
                    "resource": {
                        "data": raw,
                        "fileName": file_name,
                        "mimeType": "application/dicom",
                    },
                }
            ],
        },
    }


def build_nifti_send_publish_message(
    topic: str, file_path: str
) -> Dict[str, Any]:
    path = os.path.normpath(file_path)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"NIfTI file not found: {path}")

    with open(path, "rb") as nifti_file:
        raw = nifti_file.read()

    file_name = os.path.basename(path)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "id": generate_message_id(),
        "event": {
            "hub.topic": topic,
            "hub.event": "nifti-send",
            "context": [
                {
                    "key": "nifti",
                    "resource": {
                        "data": raw,
                        "fileName": file_name,
                        "mimeType": "application/vnd.unknown.nifti-1",
                    },
                }
            ],
        },
    }


def send_cast_request_response(
    product_name: str,
    correlation_id: str,
    data_type: str,
    data: Dict[str, Any],
    topic: Optional[str] = None,
) -> bool:
    """Send ``<datatype>-response`` on the provider hub WebSocket."""
    key = (product_name or "").strip()
    connection = _connections.get(key)
    if not connection or not connection._client or not connection._loop:
        LOGGER.warning(
            "send_cast_request_response: no connection for product=%s (active: %s)",
            key,
            ", ".join(get_active_provider_products()) or "(none)",
        )
        return False

    def run() -> None:
        connection._client.send_cast_request_response(
            correlation_id, data_type, data, topic
        )

    connection._loop.call_soon_threadsafe(run)
    return True


def publish_dicom_send_file(product_name: str, topic: str, file_path: str) -> bool:
    """Schedule dicom-send publish on the provider's hub connection thread."""
    key = (product_name or "").strip()
    connection = _connections.get(key)
    if not connection:
        LOGGER.warning(
            "publish_dicom_send_file: no connection for product=%s (active: %s)",
            key,
            ", ".join(get_active_provider_products()) or "(none)",
        )
        return False

    try:
        message = build_dicom_send_publish_message(topic, file_path)
    except Exception as exc:
        LOGGER.exception("publish_dicom_send_file build failed: %s", exc)
        return False

    connection.schedule_publish(message)
    return True


def publish_nifti_send_file(product_name: str, topic: str, file_path: str) -> bool:
    """Schedule nifti-send publish on the provider's hub connection thread."""
    key = (product_name or "").strip()
    connection = _connections.get(key)
    if not connection:
        LOGGER.warning(
            "publish_nifti_send_file: no connection for product=%s (active: %s)",
            key,
            ", ".join(get_active_provider_products()) or "(none)",
        )
        return False

    try:
        message = build_nifti_send_publish_message(topic, file_path)
    except Exception as exc:
        LOGGER.exception("publish_nifti_send_file build failed: %s", exc)
        return False

    connection.schedule_publish(message)
    return True
