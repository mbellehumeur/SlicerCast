"""Cast hub client library (async, aiohttp).

Mirrors vtk-js ``CastClient`` for OAuth, subscribe, WebSocket bind, publish,
and typed request/response. Hub URLs are supplied by the caller (e.g.
``3dslicer-cast-ai-interface.py``).

Per-dataType hub.event names: ``<dataType.lower()>-request`` /
``<dataType.lower()>-response``. Keep helpers in sync with:
- ``vtk-js/Sources/IO/Core/CastClient/eventNames.js``
- ``VolView/src/io/cast/event-names.ts``
- the OHIF Cast extension's ``event-names.ts`` (Viewers/extensions/cast)
"""

from __future__ import annotations

import asyncio
import copy
import http.client
import threading
import json
import logging
import os
import platform
import random
import socket
import string
import sys
import time
import traceback
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
import urllib.error
import urllib.request
from urllib.parse import urljoin, urlparse, urlunparse

REQUEST_SUFFIX = "-request"
RESPONSE_SUFFIX = "-response"


def normalize_data_type(data_type: Optional[str]) -> str:
    """Return the lowercased / trimmed dataType, or empty string if missing."""
    if not isinstance(data_type, str):
        return ""
    return data_type.strip().lower()


def request_event_for(data_type: Optional[str]) -> str:
    """Return ``<datatype>-request``, or empty string if dataType is missing."""
    base = normalize_data_type(data_type)
    if not base:
        return ""
    return f"{base}{REQUEST_SUFFIX}"


def response_event_for(data_type: Optional[str]) -> str:
    """Return ``<datatype>-response``, or empty string if dataType is missing."""
    base = normalize_data_type(data_type)
    if not base:
        return ""
    return f"{base}{RESPONSE_SUFFIX}"


def is_request_event(name: Optional[str]) -> bool:
    if not isinstance(name, str):
        return False
    return name.endswith(REQUEST_SUFFIX)


def is_response_event(name: Optional[str]) -> bool:
    if not isinstance(name, str):
        return False
    return name.endswith(RESPONSE_SUFFIX)


def data_type_from_event_name(name: Optional[str]) -> str:
    """Strip ``-request`` / ``-response`` and return the lowercased base."""
    if not isinstance(name, str):
        return ""
    if name.endswith(REQUEST_SUFFIX):
        return name[: -len(REQUEST_SUFFIX)]
    if name.endswith(RESPONSE_SUFFIX):
        return name[: -len(RESPONSE_SUFFIX)]
    return ""


def build_cast_request_event(
    *,
    data_type: Optional[str] = None,
    topic: Optional[str] = None,
    hub_event: Optional[str] = None,
    extra_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the ``event`` object for ``POST /api/hub/request``."""
    dt = (data_type or "").strip()
    he = (hub_event or "").strip().lower() or request_event_for(dt)
    if not he or not is_request_event(he):
        raise ValueError(
            "request: non-empty data_type or hub_event (*-request) required"
        )
    event: Dict[str, Any] = {"hub.event": he}
    resolved_topic = (topic or "").strip()
    if resolved_topic:
        event["hub.topic"] = resolved_topic
    ctx: Dict[str, Any] = dict(extra_context) if extra_context else {}
    if dt and "dataType" not in ctx:
        ctx["dataType"] = dt
    if ctx:
        event["context"] = ctx
    return event


import aiohttp

LOGGER = logging.getLogger(__name__)


def _short_caller_stack(skip: int = 2, depth: int = 6) -> str:
    """Compact, one-line-per-frame stack of recent Python callers (newest last)."""
    frames = traceback.extract_stack()[:-skip]
    frames = frames[-depth:]
    lines = []
    for frame in frames:
        path = frame.filename.replace("\\", "/").rsplit("/", 2)
        short = "/".join(path[-2:]) if len(path) > 1 else frame.filename
        lines.append(f"  {short}:{frame.lineno} {frame.name}")
    return "\n".join(lines) if lines else "  (no caller frames)"


DEFAULT_MESSAGE_ID_PREFIX = "PYCAST-"
RECONNECT_INTERVAL_SEC = 10.0
RECONNECT_ERROR_THRESHOLD = 3
# aiohttp defaults to 4 MiB; hub dicom-send uses a follow-on binary frame.
DICOM_WS_MAX_MSG_SIZE = 0
_SUBSCRIBER_SUFFIX_ALPHABET = string.ascii_uppercase + string.digits


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# Windows defaults SO_RCVBUF / SO_SNDBUF to ~64 KB which throttles large WS
# binary receive throughput. Lift to 4 MiB by default; set <=0 to skip tuning.
CAST_CLIENT_WS_SOCKET_RCVBUF_BYTES = _env_int(
    "CAST_CLIENT_WS_SOCKET_RCVBUF_BYTES", 4 * 1024 * 1024
)
CAST_CLIENT_WS_SOCKET_SNDBUF_BYTES = _env_int(
    "CAST_CLIENT_WS_SOCKET_SNDBUF_BYTES", 4 * 1024 * 1024
)
# Parallel GET /api/hub/payloads/{id} (STOW batches); <=0 means unlimited.
CAST_CLIENT_HTTP_PAYLOAD_MAX_CONCURRENT = _env_int(
    "CAST_CLIENT_HTTP_PAYLOAD_MAX_CONCURRENT", 25
)
# Log batch progress every N completed GETs (<=0 logs start and done only).
CAST_CLIENT_HTTP_PAYLOAD_PROGRESS_INTERVAL = _env_int(
    "CAST_CLIENT_HTTP_PAYLOAD_PROGRESS_INTERVAL", 25
)


def _normalize_loopback_url(url: str) -> str:
    """Use IPv4 loopback on Windows (``localhost`` can stall ~3s per connection)."""
    if not url:
        return url
    try:
        parsed = urlparse(url)
    except Exception:
        return url
    if (parsed.hostname or "").lower() != "localhost":
        return url
    port = parsed.port
    netloc = f"127.0.0.1:{port}" if port is not None else "127.0.0.1"
    return urlunparse(parsed._replace(netloc=netloc))


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    return raw if raw else default


def _tune_websocket_socket(ws: "aiohttp.ClientWebSocketResponse") -> None:
    """Lift TCP send/receive buffer sizes on the aiohttp WS socket.

    Called right after ``ws_connect``. Reaches into the writer's transport to
    retrieve the underlying ``socket.socket`` and applies the configured
    ``SO_RCVBUF`` / ``SO_SNDBUF`` values. Failures are non-fatal.
    """
    rcv = CAST_CLIENT_WS_SOCKET_RCVBUF_BYTES
    snd = CAST_CLIENT_WS_SOCKET_SNDBUF_BYTES
    if rcv <= 0 and snd <= 0:
        return
    try:
        writer = getattr(ws, "_writer", None)
        transport = writer.transport if writer is not None else None
        sock = transport.get_extra_info("socket") if transport is not None else None
        if sock is None:
            LOGGER.debug("Cast websocket socket tuning skipped (no socket)")
            return
        if rcv > 0:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcv)
        if snd > 0:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, snd)
        applied_rcv = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        applied_snd = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
        LOGGER.info(
            "Cast websocket socket buffers requested rcv=%d snd=%d "
            "applied rcv=%d snd=%d",
            rcv,
            snd,
            applied_rcv,
            applied_snd,
        )
    except Exception as exc:
        LOGGER.warning("Cast websocket socket tuning failed: %r", exc)

ConnectionStateCallback = Callable[[str, Optional[Dict[str, Any]]], None]
MessageCallback = Callable[[Dict[str, Any]], None]


@dataclass
class HubConfig:
    hub_endpoint: str
    authorization_endpoint: str
    token_endpoint: str
    client_id: str
    client_secret: str


@dataclass
class SessionConfig:
    topic: str = ""
    subscriber_name: str = ""
    product_name: str = ""
    product_version: str = "1.0"
    actors: List[str] = field(default_factory=list)
    events: List[str] = field(default_factory=list)
    lease: int = 999
    user_name: str = ""
    # Optional overrides merged into subscribe ``subscriber.client_info`` JSON.
    client_info: Dict[str, str] = field(default_factory=dict)
    default_target_actor: str = ""


@dataclass
class CastClientOptions:
    auto_reconnect: bool = False
    auto_start: bool = False
    preserve_session_topic_from_token: bool = False
    message_id_prefix: str = DEFAULT_MESSAGE_ID_PREFIX


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_message_id(prefix: str = DEFAULT_MESSAGE_ID_PREFIX) -> str:
    return prefix + uuid.uuid4().hex[:16]


def resolve_target_actor_for_wire(value: Optional[str]) -> Optional[str]:
    """Return wire ``target.actor`` value, or None when empty (``*`` is sent as ``*``)."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def resolve_target_product_name_for_wire(value: Optional[str]) -> Optional[str]:
    """Return wire ``target.product.name``, or None when empty (``*`` is sent as ``*``)."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def generate_subscriber_name(product_name: str = "PYCAST") -> str:
    base = "".join(
        c if c.isalnum() else "-"
        for c in (product_name or "PYCAST").strip()
    ).strip("-") or "PYCAST"
    suffix = "".join(
        random.choice(_SUBSCRIBER_SUFFIX_ALPHABET) for _ in range(6)
    )
    return f"{base}-{suffix}"


def normalize_websocket_url(hub_endpoint: str, websocket_url: str) -> str:
    """Rebase WS URL host/scheme to match the hub HTTP endpoint."""
    try:
        hub_parsed = urlparse(hub_endpoint)
        ws_parsed = urlparse(websocket_url)
        ws_scheme = "wss" if hub_parsed.scheme == "https" else "ws"
        rebased = urlunparse(
            (
                ws_scheme,
                hub_parsed.netloc,
                ws_parsed.path,
                ws_parsed.params,
                ws_parsed.query,
                ws_parsed.fragment,
            )
        )
        return rebased
    except Exception:
        return websocket_url


def _dicom_send_context_items(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    context = event.get("context")
    if isinstance(context, list):
        return [item for item in context if isinstance(item, dict)]
    if isinstance(context, dict):
        return [context]
    return []


def _context_files_from_event(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    context = event.get("context")
    if not isinstance(context, dict):
        return []
    files = context.get("files")
    if not isinstance(files, list):
        return []
    return [entry for entry in files if isinstance(entry, dict)]


def _first_pending_payload_slot(
    event: Dict[str, Any],
) -> Optional[Tuple[str, Optional[int], str]]:
    """Return ``(kind, index, payload_id)`` for the first unfetched payload."""
    for idx, entry in enumerate(_context_files_from_event(event)):
        payload_id = entry.get("payloadId")
        if (
            isinstance(payload_id, str)
            and payload_id.strip()
            and entry.get("data") is None
        ):
            return ("files", idx, payload_id.strip())
    for item in _dicom_send_context_items(event):
        resource = item.get("resource")
        if not isinstance(resource, dict):
            continue
        payload_id = resource.get("payloadId")
        if (
            isinstance(payload_id, str)
            and payload_id.strip()
            and resource.get("data") is None
        ):
            return ("resource", None, payload_id.strip())
    return None


def _list_pending_payload_slots(
    event: Dict[str, Any],
) -> List[Tuple[str, Optional[int], str]]:
    """Every unfetched ``payloadId`` on ``context.files[]`` then ``resource`` slots."""
    slots: List[Tuple[str, Optional[int], str]] = []
    for idx, entry in enumerate(_context_files_from_event(event)):
        payload_id = entry.get("payloadId")
        if (
            isinstance(payload_id, str)
            and payload_id.strip()
            and entry.get("data") is None
        ):
            slots.append(("files", idx, payload_id.strip()))
    for item in _dicom_send_context_items(event):
        resource = item.get("resource")
        if not isinstance(resource, dict):
            continue
        payload_id = resource.get("payloadId")
        if (
            isinstance(payload_id, str)
            and payload_id.strip()
            and resource.get("data") is None
        ):
            slots.append(("resource", None, payload_id.strip()))
    return slots


# Cast binary-family events: any ``hub.event`` whose name matches one of these
# prefixes (exact or followed by ``-`` / ``_``) is considered binary-bearing
# for transport purposes. Keep this list byte-for-byte equivalent across the
# four Cast implementations (vtk-js sendNormalize, Slicer cast_client, VolView
# server cast_client, hub cast_api) per AGENTS.md section 2.
_CAST_BINARY_EVENT_PREFIXES = ("dicom", "nifti", "jpg", "png", "nrrd")


def is_cast_binary_event(event_name: Any) -> bool:
    if not isinstance(event_name, str):
        return False
    name = event_name.strip().lower()
    if not name:
        return False
    for prefix in _CAST_BINARY_EVENT_PREFIXES:
        if name == prefix or name.startswith(prefix + "-") or name.startswith(prefix + "_"):
            return True
    return False


def cast_payload_id(event: Dict[str, Any]) -> str:
    """Return the first pending ``payloadId`` on ``context.files[]`` or ``resource``."""
    slot = _first_pending_payload_slot(event)
    return slot[2] if slot else ""


def has_pending_payload(event: Dict[str, Any]) -> bool:
    """True when a binary-family event carries unfetched ``payloadId`` value(s)."""
    return _first_pending_payload_slot(event) is not None


def message_needs_stow_batch_publish(msg: Dict[str, Any]) -> bool:
    event = msg.get("event")
    if not isinstance(event, dict) or not is_cast_binary_event(event.get("hub.event")):
        return False
    files = _context_files_from_event(event)
    if not files:
        return False
    return any(
        isinstance(entry.get("data"), (bytes, bytearray, memoryview))
        for entry in files
    )


def extract_stow_batch_file_bytes(msg: Dict[str, Any]) -> List[bytes]:
    event = msg.get("event")
    if not isinstance(event, dict):
        raise ValueError("CastClient: STOW batch publish message missing event")
    blobs: List[bytes] = []
    for entry in _context_files_from_event(event):
        data = entry.get("data")
        if data is not None:
            blobs.append(_read_binary_strict(data))
    return blobs


def normalize_stow_batch_file_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(entry, dict):
        raise ValueError("CastClient: STOW batch files[] entries must be objects")
    byte_length: Optional[int] = None
    if isinstance(entry.get("byteLength"), int) and entry["byteLength"] >= 0:
        byte_length = entry["byteLength"]
    if "data" in entry and entry.get("data") is not None:
        if isinstance(entry.get("data"), str):
            raise ValueError(
                "CastClient: STOW batch string payloads are not supported; "
                "pass binary input instead"
            )
        byte_length = len(_read_binary_strict(entry["data"]))
    if byte_length is None:
        raise ValueError(
            "CastClient: STOW batch requires files[].data or files[].byteLength"
        )
    normalized = copy.deepcopy(entry)
    file_name = normalized.get("fileName")
    normalized["fileName"] = (
        file_name.strip()
        if isinstance(file_name, str) and file_name.strip()
        else "dicom-send.dcm"
    )
    mime_type = normalized.get("mimeType")
    normalized["mimeType"] = (
        mime_type.strip()
        if isinstance(mime_type, str) and mime_type.strip()
        else "application/dicom"
    )
    normalized.pop("data", None)
    normalized.pop("binaryTransfer", None)
    normalized.pop("url", None)
    normalized.pop("payloadId", None)
    normalized.pop("expiresAt", None)
    normalized["byteLength"] = byte_length
    return normalized


def normalize_stow_batch_message_metadata_only(msg: Dict[str, Any]) -> Dict[str, Any]:
    event = msg.get("event")
    if not isinstance(event, dict):
        raise ValueError("CastClient: STOW batch requires event object")
    if not is_cast_binary_event(event.get("hub.event")):
        return msg
    context = event.get("context")
    if not isinstance(context, dict):
        raise ValueError("CastClient: STOW batch requires event.context object")
    files = context.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("CastClient: STOW batch requires non-empty event.context.files[]")
    normalized_files = [normalize_stow_batch_file_entry(entry) for entry in files]
    out = copy.deepcopy(msg)
    out["event"] = {
        **event,
        "context": {**context, "files": normalized_files},
    }
    return out


def _build_stow_related_body(
    boundary: str, json_text: str, dicom_blobs: List[bytes]
) -> bytes:
    parts: List[bytes] = []
    parts.append(
        f"--{boundary}\r\nContent-Type: application/dicom+json\r\n\r\n".encode(
            "utf-8"
        )
    )
    parts.append(json_text.encode("utf-8"))
    parts.append(b"\r\n")
    for blob in dicom_blobs:
        parts.append(
            f"--{boundary}\r\nContent-Type: application/dicom\r\n\r\n".encode(
                "utf-8"
            )
        )
        parts.append(blob)
        parts.append(b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode("utf-8"))
    return b"".join(parts)


def message_needs_multipart_publish(msg: Dict[str, Any]) -> bool:
    event = msg.get("event")
    if not isinstance(event, dict) or not is_cast_binary_event(event.get("hub.event")):
        return False
    if _context_files_from_event(event):
        return False
    for item in _dicom_send_context_items(event):
        resource = item.get("resource")
        if isinstance(resource, dict) and resource.get("data") is not None:
            return True
    return False


def extract_first_binary_file_bytes(msg: Dict[str, Any]) -> bytes:
    event = msg.get("event")
    if not isinstance(event, dict):
        raise ValueError("CastClient: publish message missing event")
    for item in _dicom_send_context_items(event):
        resource = item.get("resource")
        if not isinstance(resource, dict) or resource.get("data") is None:
            continue
        return _read_binary_strict(resource["data"])
    raise ValueError("CastClient: multipart publish missing resource.data")


_HTTP_PAYLOAD_READ_CHUNK_BYTES = 4 * 1024 * 1024


def _tune_http_client_socket(sock: Optional[socket.socket]) -> None:
    """Lift SO_RCVBUF on blocking HTTP payload downloads (Windows default ~64KB)."""
    if sock is None:
        return
    rcv = CAST_CLIENT_WS_SOCKET_RCVBUF_BYTES
    if rcv <= 0:
        return
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcv)
    except OSError:
        pass


@dataclass
class _ThreadPayloadHttp:
    """Per-thread keep-alive ``http.client`` connection for payload GETs."""

    key: Optional[Tuple[str, int, bool]] = None
    conn: Optional[http.client.HTTPConnection] = None


class _PayloadHttpConnectionPool:
    """Thread-local ``http.client`` connections (one fresh GET per download)."""

    def __init__(self) -> None:
        self._tls = threading.local()

    def _close_tls_connection(self) -> None:
        holder = getattr(self._tls, "payload_http", None)
        if holder is None or holder.conn is None:
            return
        try:
            holder.conn.close()
        except OSError:
            pass
        holder.conn = None

    def _open_connection(
        self, host: str, port: int, https: bool
    ) -> http.client.HTTPConnection:
        if https:
            return http.client.HTTPSConnection(host, port, timeout=600)
        return http.client.HTTPConnection(host, port, timeout=600)

    def download(self, url: str, bearer_token: str) -> bytes:
        url = _normalize_loopback_url(url)
        parsed = urlparse(url)
        host = parsed.hostname
        if not host:
            raise ValueError(f"Cast http payload url missing host: {url!r}")
        default_port = 443 if parsed.scheme == "https" else 80
        port = parsed.port if parsed.port is not None else default_port
        https = parsed.scheme == "https"
        key = (host, port, https)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        holder = getattr(self._tls, "payload_http", None)
        if holder is None or holder.key != key or holder.conn is None:
            if holder is not None and holder.conn is not None:
                try:
                    holder.conn.close()
                except OSError:
                    pass
            holder = _ThreadPayloadHttp(
                key=key, conn=self._open_connection(host, port, https)
            )
            self._tls.payload_http = holder

        headers: Dict[str, str] = {}
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"

        conn = holder.conn
        assert conn is not None
        try:
            conn.request("GET", path, headers=headers)
            resp = conn.getresponse()
            if resp.status != 200:
                body = resp.read()
                raise urllib.error.HTTPError(
                    url, resp.status, resp.reason, resp.headers, body
                )
            _tune_http_client_socket(conn.sock)
            parts: List[bytes] = []
            while True:
                chunk = resp.read(_HTTP_PAYLOAD_READ_CHUNK_BYTES)
                if not chunk:
                    break
                parts.append(chunk)
            return b"".join(parts)
        except (ConnectionError, OSError, urllib.error.HTTPError):
            self._close_tls_connection()
            raise
        finally:
            # Do not reuse sockets; idle closes cause WinError 10053 on large batches.
            self._close_tls_connection()

    def download_with_retry(self, url: str, bearer_token: str) -> bytes:
        try:
            return self.download(url, bearer_token)
        except (ConnectionError, OSError) as exc:
            LOGGER.warning(
                "Cast payload GET retry after %s: %s",
                type(exc).__name__,
                exc,
            )
            return self.download(url, bearer_token)


def _download_http_payload_sync(url: str, bearer_token: str) -> bytes:
    """Legacy one-shot GET (no keep-alive). Prefer ``_PayloadHttpConnectionPool``."""
    pool = _PayloadHttpConnectionPool()
    return pool.download(url, bearer_token)


def dicom_send_byte_length(message: Dict[str, Any]) -> int:
    """Return DICOM payload size from an assembled dicom-send notification."""
    event = message.get("event") or {}
    for item in _dicom_send_context_items(event):
        resource = item.get("resource")
        if not isinstance(resource, dict):
            continue
        byte_length = resource.get("byteLength")
        if isinstance(byte_length, int) and byte_length >= 0:
            return byte_length
        data = resource.get("data")
        if isinstance(data, (bytes, bytearray)):
            return len(data)
        if isinstance(data, str) and data:
            return len(data)
    return 0


def dicom_send_file_name(message: Dict[str, Any]) -> str:
    event = message.get("event") or {}
    for item in _dicom_send_context_items(event):
        resource = item.get("resource")
        if isinstance(resource, dict):
            name = resource.get("fileName")
            if isinstance(name, str) and name.strip():
                return name.strip()
    return "dicom-send.dcm"


def get_client_info_payload(
    product_name: str = "",
    product_version: str = "",
    extra: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, str]]:
    """Build ``subscriber.client_info`` for hub subscribe (vtk-js ``getClientInfoPayload``)."""
    info: Dict[str, str] = {}
    pn = (product_name or "").strip()
    pv = (product_version or "").strip()
    if pn:
        info["productName"] = pn
    if pv:
        info["version"] = pv
    info["platform"] = platform.platform()
    info["userAgent"] = f"Python/{sys.version.split()[0]}"
    try:
        import locale

        lang = locale.getdefaultlocale()[0]
        if lang and str(lang).strip():
            info["language"] = str(lang).strip()
    except Exception:
        pass
    try:
        tz = datetime.now(timezone.utc).astimezone().tzname()
        if tz and str(tz).strip():
            info["timezone"] = str(tz).strip()
    except Exception:
        pass
    if extra:
        for key, value in extra.items():
            if value is not None and str(value).strip():
                info[str(key)] = str(value).strip()
    return info or None


def _read_binary_strict(data: Any) -> bytes:
    if isinstance(data, memoryview):
        return data.tobytes()
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, bytes):
        return data
    raise TypeError(
        "CastClient: dicom-send resource.data must be bytes, bytearray, or "
        "memoryview (string payloads are not supported; pass binary input)"
    )


def normalize_dicom_send_context_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Metadata-only dicom-send context item for multipart publish."""
    if not isinstance(item, dict):
        raise ValueError("CastClient: dicom-send context items must be objects")
    resource = item.get("resource")
    if not isinstance(resource, dict):
        raise ValueError("CastClient: dicom-send context item missing resource object")

    byte_length: Optional[int] = None
    if isinstance(resource.get("byteLength"), int) and resource["byteLength"] >= 0:
        byte_length = resource["byteLength"]

    if "data" in resource and resource.get("data") is not None:
        if isinstance(resource.get("data"), str):
            raise ValueError(
                "CastClient: dicom-send string payloads are not supported; "
                "pass binary input instead"
            )
        byte_length = len(_read_binary_strict(resource["data"]))

    if byte_length is None:
        raise ValueError(
            "CastClient: dicom-send multipart requires resource.data or "
            "resource.byteLength"
        )

    normalized_resource = copy.deepcopy(resource)
    file_name = normalized_resource.get("fileName")
    normalized_resource["fileName"] = (
        file_name.strip() if isinstance(file_name, str) and file_name.strip() else "dicom-send.dcm"
    )
    mime_type = normalized_resource.get("mimeType")
    normalized_resource["mimeType"] = (
        mime_type.strip()
        if isinstance(mime_type, str) and mime_type.strip()
        else "application/dicom"
    )
    normalized_resource.pop("data", None)
    normalized_resource.pop("binaryTransfer", None)
    normalized_resource.pop("url", None)
    normalized_resource.pop("payloadId", None)
    normalized_resource.pop("expiresAt", None)
    normalized_resource["byteLength"] = byte_length
    return {**item, "resource": normalized_resource}


def normalize_dicom_send_message_strict(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize dicom-send publish metadata (multipart ``message`` part)."""
    event = msg.get("event")
    if not isinstance(event, dict):
        raise ValueError("CastClient: dicom-send requires event object")
    if event.get("hub.event") != "dicom-send":
        return msg

    items = _dicom_send_context_items(event)
    if not items:
        raise ValueError("CastClient: dicom-send requires non-empty event.context")

    normalized_context = [
        normalize_dicom_send_context_item(item) for item in items
    ]
    out = copy.deepcopy(msg)
    out["event"] = {**event, "context": normalized_context}
    return out


def normalize_nifti_send_context_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Metadata-only nifti-send context item for multipart publish."""
    return normalize_nifti_send_context_item_metadata_only(item)


def _default_nifti_resource_fields(normalized_resource: Dict[str, Any]) -> Dict[str, Any]:
    file_name = normalized_resource.get("fileName")
    normalized_resource["fileName"] = (
        file_name.strip()
        if isinstance(file_name, str) and file_name.strip()
        else "nifti-send.nii.gz"
    )
    mime_type = normalized_resource.get("mimeType")
    normalized_resource["mimeType"] = (
        mime_type.strip()
        if isinstance(mime_type, str) and mime_type.strip()
        else "application/vnd.unknown.nifti-1"
    )
    return normalized_resource


def normalize_nifti_send_context_item_metadata_only(
    item: Dict[str, Any],
) -> Dict[str, Any]:
    """Metadata-only nifti-send item (vtk-js ``normalizeNiftiSendContextItemMetadataOnly``)."""
    if not isinstance(item, dict):
        raise ValueError("CastClient: nifti-send context items must be objects")
    resource = item.get("resource")
    if not isinstance(resource, dict):
        raise ValueError("CastClient: nifti-send context item missing resource object")

    byte_length: Optional[int] = None
    if isinstance(resource.get("byteLength"), int) and resource["byteLength"] >= 0:
        byte_length = resource["byteLength"]

    if "data" in resource and resource.get("data") is not None:
        if isinstance(resource.get("data"), str):
            raise ValueError(
                "CastClient: nifti-send string payloads are not supported; "
                "pass binary input instead"
            )
        byte_length = len(_read_binary_strict(resource["data"]))

    if byte_length is None:
        raise ValueError(
            "CastClient: nifti-send multipart requires resource.data or "
            "resource.byteLength"
        )

    normalized_resource = _default_nifti_resource_fields(copy.deepcopy(resource))
    normalized_resource.pop("data", None)
    normalized_resource.pop("binaryTransfer", None)
    normalized_resource.pop("url", None)
    normalized_resource.pop("payloadId", None)
    normalized_resource.pop("expiresAt", None)
    normalized_resource["byteLength"] = byte_length
    return {**item, "resource": normalized_resource}


def normalize_nifti_send_message_metadata_only(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Metadata-only nifti-send publish (vtk-js ``normalizeNiftiSendMessageMetadataOnly``)."""
    event = msg.get("event")
    if not isinstance(event, dict):
        raise ValueError("CastClient: nifti-send requires event object")
    if event.get("hub.event") != "nifti-send":
        return msg

    items = _dicom_send_context_items(event)
    if not items:
        raise ValueError("CastClient: nifti-send requires non-empty event.context")

    normalized_context = [
        normalize_nifti_send_context_item_metadata_only(item) for item in items
    ]
    out = copy.deepcopy(msg)
    out["event"] = {**event, "context": normalized_context}
    return out


def normalize_nifti_send_message_strict(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Alias for metadata-only nifti-send normalize (multipart publish)."""
    return normalize_nifti_send_message_metadata_only(msg)


class CastClient(ABC):
    @abstractmethod
    async def authenticate(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    async def get_token(self, code: str) -> bool:
        ...

    @abstractmethod
    async def subscribe(self) -> int:
        ...

    @abstractmethod
    async def unsubscribe(self) -> None:
        ...

    @abstractmethod
    async def publish(self, cast_message: Dict[str, Any]) -> Optional[int]:
        ...

    @abstractmethod
    async def request(
        self,
        *,
        subscriber: str,
        topic: Optional[str] = None,
        data_type: Optional[str] = None,
        actor: Optional[str] = None,
        target_actor: Optional[str] = None,
        product_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        ...

    @abstractmethod
    def send_cast_request_response(
        self,
        correlation_id: str,
        data_type: str,
        data: Any,
        topic: Optional[str] = None,
    ) -> None:
        ...

    @abstractmethod
    def on_message(self, callback: MessageCallback) -> None:
        ...

    @abstractmethod
    def on_connection_state_change(
        self, callback: ConnectionStateCallback
    ) -> None:
        ...

    @abstractmethod
    async def close(self, *, hub_unsubscribe: bool = True) -> None:
        ...


class SlicerCastClient(CastClient):
    """Async Cast hub client using aiohttp."""

    def __init__(
        self,
        hub: HubConfig,
        session: SessionConfig,
        options: Optional[CastClientOptions] = None,
        *,
        session_http: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self._hub = HubConfig(
            hub_endpoint=_normalize_loopback_url(hub.hub_endpoint),
            authorization_endpoint=_normalize_loopback_url(hub.authorization_endpoint),
            token_endpoint=_normalize_loopback_url(hub.token_endpoint),
            client_id=hub.client_id,
            client_secret=hub.client_secret,
        )
        self._session_cfg = session
        self._options = options or CastClientOptions()
        self._http = session_http
        self._owns_http = session_http is None

        if not self._session_cfg.subscriber_name.strip():
            self._session_cfg.subscriber_name = generate_subscriber_name(
                self._session_cfg.product_name or "PYCAST"
            )

        self._token = ""
        self._last_id_token = ""
        self._last_published_message_id = ""
        self._subscribed = False
        self._resubscribe_requested = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_task: Optional[asyncio.Task[None]] = None
        self._reconnect_task: Optional[asyncio.Task[None]] = None
        self._reconnect_fail_streak = 0
        self._closed = False

        self._on_message: Optional[MessageCallback] = None
        self._on_connection_state: Optional[ConnectionStateCallback] = None
        self._message_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._hub_channel_endpoint: str = ""
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._payload_http_pool = _PayloadHttpConnectionPool()

        if self._options.auto_reconnect:
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    def _emit_connection_state(
        self, state: str, detail: Optional[Dict[str, Any]] = None
    ) -> None:
        if self._on_connection_state:
            self._on_connection_state(state, detail)

    async def _get_http(self) -> aiohttp.ClientSession:
        if self._http is None:
            max_concurrent = max(1, CAST_CLIENT_HTTP_PAYLOAD_MAX_CONCURRENT)
            self._http = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None, connect=10, sock_read=600),
                trust_env=False,
                connector=aiohttp.TCPConnector(
                    limit=0,
                    limit_per_host=max(16, max_concurrent * 2),
                ),
            )
        return self._http

    def _authorization_endpoint(self) -> str:
        explicit = (self._hub.authorization_endpoint or "").strip()
        if explicit:
            return explicit
        parsed = urlparse(self._hub.token_endpoint)
        return f"{parsed.scheme}://{parsed.netloc}/oauth/authorize"

    def _product_name_for_oauth(self) -> str:
        return (self._session_cfg.product_name or "PYCAST").strip() or "PYCAST"

    def _events_form_value(self) -> str:
        events = self._session_cfg.events
        if not events:
            return ""
        if len(events) == 1 and events[0] == "*":
            return "*"
        return ",".join(events)

    def _subscribe_form_data(self, hub_mode: str) -> Dict[str, str]:
        data: Dict[str, str] = {
            "hub.mode": hub_mode,
            "hub.channel.type": "websocket",
            "hub.events": self._events_form_value(),
            "hub.topic": self._session_cfg.topic,
            "hub.lease": str(self._session_cfg.lease),
            "subscriber.name": self._session_cfg.subscriber_name,
            "subscriber.product.name": self._session_cfg.product_name,
            "subscriber.product.version": self._session_cfg.product_version,
        }
        if hub_mode == "unsubscribe" and self._hub_channel_endpoint:
            data["hub.channel.endpoint"] = self._hub_channel_endpoint
        actors = [a.strip() for a in self._session_cfg.actors if a.strip()]
        if actors:
            data["subscriber.actors"] = json.dumps(actors)
        client_info = get_client_info_payload(
            self._session_cfg.product_name,
            self._session_cfg.product_version,
            self._session_cfg.client_info or None,
        )
        if client_info:
            data["subscriber.client_info"] = json.dumps(client_info)
        return data

    def set_topic(self, topic: str) -> None:
        self._session_cfg.topic = topic

    def set_token(self, token: str) -> None:
        self._token = token or ""

    def set_subscriber_name(self, subscriber_name: str) -> None:
        self._session_cfg.subscriber_name = subscriber_name

    def set_user_name(self, user_name: str) -> None:
        self._session_cfg.user_name = user_name or ""

    @property
    def message_queue(self) -> asyncio.Queue[Dict[str, Any]]:
        return self._message_queue

    def on_message(self, callback: MessageCallback) -> None:
        self._on_message = callback

    def on_connection_state_change(
        self, callback: ConnectionStateCallback
    ) -> None:
        self._on_connection_state = callback

    async def authenticate(self) -> Dict[str, Any]:
        authorize_endpoint = self._authorization_endpoint()
        if not authorize_endpoint:
            raise ValueError(
                "SlicerCastClient.authenticate: no authorization_endpoint"
            )

        form: Dict[str, str] = {
            "client_product_name": self._product_name_for_oauth(),
        }
        if self._last_id_token:
            form["id_token"] = self._last_id_token
        elif self._session_cfg.user_name:
            form["user_name"] = self._session_cfg.user_name
        if self._session_cfg.topic:
            form["topic"] = self._session_cfg.topic

        http = await self._get_http()
        async with http.post(
            authorize_endpoint,
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as response:
            if response.status != 200:
                text = await response.text()
                raise RuntimeError(
                    f"authenticate failed HTTP {response.status}: {text}"
                )
            data = await response.json()
        if isinstance(data.get("user_name"), str) and data["user_name"]:
            self._session_cfg.user_name = data["user_name"]
        return {
            "user_name": data.get("user_name") or "",
            "code": data.get("code") or "",
            "expires_in": data.get("expires_in"),
        }

    async def get_token(self, code: str) -> bool:
        if not code:
            LOGGER.error("get_token: code is required")
            return False

        form = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self._hub.client_id,
            "client_secret": self._hub.client_secret,
            "client_product_name": self._product_name_for_oauth(),
        }
        http = await self._get_http()
        async with http.post(
            self._hub.token_endpoint,
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as response:
            if response.status != 200:
                LOGGER.error("get_token failed HTTP %s", response.status)
                return False
            config = await response.json()

        if isinstance(config.get("access_token"), str) and config["access_token"]:
            self._token = config["access_token"]
        if isinstance(config.get("id_token"), str) and config["id_token"]:
            self._last_id_token = config["id_token"]
        topic = config.get("topic")
        if isinstance(topic, str) and topic:
            if not self._options.preserve_session_topic_from_token:
                self.set_topic(topic)
            if self._options.auto_start:
                await self.subscribe()
        return bool(self._token)

    async def _start_websocket(self, websocket_url: str) -> None:
        await self._stop_websocket()
        normalized = normalize_websocket_url(
            self._hub.hub_endpoint, websocket_url
        )
        http = await self._get_http()
        self._hub_channel_endpoint = normalized
        self._ws = await http.ws_connect(
            normalized, max_msg_size=DICOM_WS_MAX_MSG_SIZE
        )
        _tune_websocket_socket(self._ws)
        self._async_loop = asyncio.get_running_loop()
        await self._safe_send_str(
            json.dumps({"hub.channel.endpoint": normalized}),
            reason="bind",
        )
        self._ws_task = asyncio.create_task(self._websocket_reader())
        LOGGER.info("Cast websocket reader started (non-blocking hub I/O)")
        self._reconnect_fail_streak = 0
        self._emit_connection_state("connected")

    async def _stop_websocket(self) -> None:
        LOGGER.info(
            "Cast _stop_websocket called closed=%s subscribed=%s "
            "ws_open=%s caller=\n%s",
            self._closed,
            self._subscribed,
            bool(self._ws and not self._ws.closed),
            _short_caller_stack(),
        )
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            self._ws_task = None
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None
        self._async_loop = None

    @staticmethod
    def _ws_outbound_label(payload: str) -> str:
        try:
            msg = json.loads(payload)
        except json.JSONDecodeError:
            return "non-json"
        if msg.get("type") == "pong":
            return "pong"
        if msg.get("hub.channel.endpoint"):
            return "bind"
        event = msg.get("event")
        if isinstance(event, dict):
            hub_event = event.get("hub.event")
            if isinstance(hub_event, str) and hub_event.strip():
                return hub_event.strip()
        msg_type = msg.get("type")
        if isinstance(msg_type, str) and msg_type.strip():
            return msg_type.strip()
        return "json"

    def _log_ws_send_task_result(self, task: "asyncio.Task[None]") -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            LOGGER.warning("Cast websocket outbound task failed: %s", exc)

    async def _safe_send_str(self, payload: str, *, reason: str = "") -> None:
        label = reason or self._ws_outbound_label(payload)
        ws = self._ws
        if ws is None or ws.closed:
            LOGGER.debug("Cast websocket outbound skipped (%s): socket closed", label)
            return
        try:
            await ws.send_str(payload)
        except aiohttp.ClientConnectionResetError as exc:
            LOGGER.warning(
                "Cast websocket outbound failed (%s): connection reset (%s)",
                label,
                exc,
            )
        except (ConnectionError, OSError) as exc:
            LOGGER.warning(
                "Cast websocket outbound failed (%s): %s",
                label,
                exc,
            )

    def _schedule_ws_send_str(self, payload: str, *, reason: str = "") -> None:
        loop = self._async_loop
        if loop is None:
            return
        label = reason or self._ws_outbound_label(payload)

        def start_send() -> None:
            task = asyncio.create_task(
                self._safe_send_str(payload, reason=label),
                name=f"CastWsSend-{label}",
            )
            task.add_done_callback(self._log_ws_send_task_result)

        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            start_send()
        else:
            loop.call_soon_threadsafe(start_send)

    def _enqueue_message(self, cast_message: Dict[str, Any]) -> None:
        try:
            self._message_queue.put_nowait(cast_message)
        except asyncio.QueueFull:
            pass

    def _schedule_enqueue_message(self, cast_message: Dict[str, Any]) -> None:
        loop = self._async_loop
        if loop is None:
            self._enqueue_message(cast_message)
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            self._enqueue_message(cast_message)
        else:
            loop.call_soon_threadsafe(self._enqueue_message, cast_message)

    def _resolve_payload_url(self, payload_id: str) -> str:
        if not payload_id:
            return ""
        path = f"/api/hub/payloads/{payload_id}"
        try:
            return _normalize_loopback_url(
                urljoin(self._hub.hub_endpoint, path)
            )
        except Exception as exc:
            LOGGER.warning(
                "Cast payload url resolution failed payloadId=%r err=%r",
                payload_id,
                exc,
            )
            return ""

    async def _download_payload_bytes(self, url: str) -> bytes:
        """Blocking GET on a worker thread (fresh connection per file)."""
        return await asyncio.to_thread(
            self._payload_http_pool.download_with_retry,
            url,
            self._token or "",
        )

    def _attach_payload_bytes_inplace(
        self,
        cast_message: Dict[str, Any],
        data: bytes,
        slot: Tuple[str, Optional[int], str],
    ) -> None:
        """Mutate ``cast_message`` in place (no ``deepcopy``)."""
        event = cast_message.get("event")
        if not isinstance(event, dict):
            raise ValueError("CastClient.fetch_payload: missing event object")
        kind, index, _payload_id = slot
        if kind == "files":
            context = event.get("context")
            if not isinstance(context, dict):
                raise ValueError("CastClient.fetch_payload: missing context.files[]")
            files = context.get("files")
            if not isinstance(files, list) or index is None or index >= len(files):
                raise ValueError("CastClient.fetch_payload: invalid files[] index")
            entry = files[index]
            if not isinstance(entry, dict):
                raise ValueError("CastClient.fetch_payload: invalid files[] entry")
            entry.pop("binaryTransfer", None)
            entry.pop("url", None)
            entry.pop("payloadId", None)
            entry.pop("expiresAt", None)
            entry["data"] = data
            entry["byteLength"] = len(data)
            return
        for item in _dicom_send_context_items(event):
            resource = item.get("resource")
            if not isinstance(resource, dict):
                continue
            resource.pop("binaryTransfer", None)
            resource.pop("url", None)
            resource.pop("payloadId", None)
            resource.pop("expiresAt", None)
            resource["data"] = data
            resource["byteLength"] = len(data)
            item["resource"] = resource
            return
        raise ValueError("CastClient.fetch_payload: no resource slot on message")

    def _attach_payload_bytes(
        self,
        cast_message: Dict[str, Any],
        data: bytes,
        slot: Optional[Tuple[str, Optional[int], str]] = None,
    ) -> Dict[str, Any]:
        enriched = copy.deepcopy(cast_message)
        event = enriched.get("event") or {}
        if slot is None:
            slot = _first_pending_payload_slot(event)
        if slot is None:
            raise ValueError("CastClient.fetch_payload: no payload slot on message")
        kind, index, _payload_id = slot
        if kind == "files":
            context = event.get("context")
            if not isinstance(context, dict):
                raise ValueError("CastClient.fetch_payload: missing context.files[]")
            files = context.get("files")
            if not isinstance(files, list) or index is None or index >= len(files):
                raise ValueError("CastClient.fetch_payload: invalid files[] index")
            entry = dict(files[index])
            entry.pop("binaryTransfer", None)
            entry.pop("url", None)
            entry.pop("payloadId", None)
            entry.pop("expiresAt", None)
            entry["data"] = data
            entry["byteLength"] = len(data)
            files[index] = entry
            enriched["event"] = event
            return enriched
        for item in _dicom_send_context_items(event):
            resource = item.get("resource")
            if not isinstance(resource, dict):
                continue
            resource = dict(resource)
            resource.pop("binaryTransfer", None)
            resource.pop("url", None)
            resource.pop("payloadId", None)
            resource.pop("expiresAt", None)
            resource["data"] = data
            resource["byteLength"] = len(data)
            item["resource"] = resource
            enriched["event"] = event
            return enriched
        raise ValueError("CastClient.fetch_payload: no resource slot on message")

    async def fetch_payload(self, cast_message: Dict[str, Any]) -> Dict[str, Any]:
        """App-initiated GET for the next pending ``payloadId``; returns message with ``data``."""
        event = cast_message.get("event") or {}
        slot = _first_pending_payload_slot(event)
        if slot is None:
            return cast_message
        payload_id = slot[2]
        url = self._resolve_payload_url(payload_id)
        if not url:
            raise ValueError("CastClient.fetch_payload: invalid payloadId")
        started_at = time.monotonic()
        data = await self._download_payload_bytes(url)
        elapsed = max(time.monotonic() - started_at, 0.0)
        LOGGER.info(
            "Cast payload fetched id=%s event=%s payloadId=%s bytes=%d elapsed=%.2fs",
            cast_message.get("id", ""),
            event.get("hub.event", ""),
            payload_id[:8],
            len(data),
            elapsed,
        )
        self._attach_payload_bytes_inplace(cast_message, data, slot)
        return cast_message

    async def fetch_all_payloads(
        self, cast_message: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Download every pending payload (parallel pooled ``http.client`` GETs)."""
        event = cast_message.get("event") or {}
        slots = _list_pending_payload_slots(event)
        if not slots:
            return cast_message

        max_concurrent = CAST_CLIENT_HTTP_PAYLOAD_MAX_CONCURRENT
        if max_concurrent <= 0:
            max_concurrent = len(slots)
        else:
            max_concurrent = max(1, max_concurrent)
        sem = asyncio.Semaphore(max_concurrent)
        started_batch = time.monotonic()
        sample_url = self._resolve_payload_url(slots[0][2])
        total_slots = len(slots)
        progress_interval = CAST_CLIENT_HTTP_PAYLOAD_PROGRESS_INTERVAL
        completed_count = 0
        completed_bytes = 0
        progress_lock = asyncio.Lock()
        msg_id = cast_message.get("id", "")
        hub_event = event.get("hub.event", "")
        LOGGER.info(
            "Cast payload batch start id=%s event=%s files=%d concurrent=%d url_sample=%s",
            msg_id,
            hub_event,
            total_slots,
            max_concurrent,
            sample_url,
        )

        async def _maybe_log_progress() -> None:
            nonlocal completed_count, completed_bytes
            if progress_interval <= 0:
                return
            if completed_count != total_slots and (
                completed_count % progress_interval != 0
            ):
                return
            elapsed = max(time.monotonic() - started_batch, 0.0)
            LOGGER.info(
                "Download id=%s event=%s completed=%d/%d "
                "bytes=%d elapsed=%.2fs",
                msg_id,
                hub_event,
                completed_count,
                total_slots,
                completed_bytes,
                elapsed,
            )

        async def fetch_one(
            slot: Tuple[str, Optional[int], str],
        ) -> Tuple[Tuple[str, Optional[int], str], bytes, float]:
            nonlocal completed_count, completed_bytes
            async with sem:
                payload_id = slot[2]
                url = self._resolve_payload_url(payload_id)
                if not url:
                    raise ValueError(
                        f"CastClient.fetch_payload: invalid payloadId {payload_id!r}"
                    )
                slot_started = time.monotonic()
                try:
                    data = await self._download_payload_bytes(url)
                except Exception as exc:
                    LOGGER.error(
                        "Cast payload GET failed id=%s payloadId=%s index=%s: %s",
                        msg_id,
                        payload_id[:16],
                        slot[1],
                        exc,
                    )
                    raise
                elapsed = max(time.monotonic() - slot_started, 0.0)
                async with progress_lock:
                    completed_count += 1
                    completed_bytes += len(data)
                    await _maybe_log_progress()
                return slot, data, elapsed

        results = await asyncio.gather(
            *(fetch_one(slot) for slot in slots),
            return_exceptions=True,
        )
        failures: List[Tuple[str, BaseException]] = []
        for slot, result in zip(slots, results):
            if isinstance(result, BaseException):
                failures.append((slot[2], result))
        if failures:
            pid, first_exc = failures[0]
            LOGGER.error(
                "Cast payload batch failed id=%s event=%s failed=%d/%d "
                "first_payloadId=%s: %s",
                msg_id,
                hub_event,
                len(failures),
                total_slots,
                pid[:16],
                first_exc,
            )
            raise first_exc

        msg = cast_message
        total_bytes = 0
        max_slot_elapsed = 0.0
        for slot, data, elapsed in results:
            total_bytes += len(data)
            max_slot_elapsed = max(max_slot_elapsed, elapsed)
            self._attach_payload_bytes_inplace(msg, data, slot)
        batch_elapsed = max(time.monotonic() - started_batch, 0.0)
        LOGGER.info(
            "Cast payload batch done id=%s event=%s files=%d bytes=%d "
            "elapsed=%.2fs max_slot=%.2fs concurrent=%d",
            cast_message.get("id", ""),
            event.get("hub.event", ""),
            len(slots),
            total_bytes,
            batch_elapsed,
            max_slot_elapsed,
            max_concurrent,
        )
        return msg

    async def _websocket_reader(self) -> None:
        assert self._ws is not None
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_websocket_text(msg.data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    LOGGER.warning(
                        "unexpected binary WebSocket message (%d bytes); "
                        "hub uses text JSON + payloadId",
                        len(msg.data or b""),
                    )
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    LOGGER.warning(
                        "Cast websocket protocol error close_code=%s exc=%r",
                        self._ws.close_code if self._ws else None,
                        self._ws.exception() if self._ws else None,
                    )
                    break
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                ):
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("websocket reader error: %s", exc)
        finally:
            if not self._closed:
                self._resubscribe_requested = True
                self._emit_connection_state("disconnected")

    async def _handle_websocket_text(self, event_data: str) -> None:
        try:
            cast_message = json.loads(event_data)
        except json.JSONDecodeError:
            LOGGER.warning("invalid JSON on websocket")
            return

        if cast_message.get("type") == "ping":
            await self._safe_send_str(
                json.dumps({"type": "pong", "timestamp": _utc_timestamp()}),
                reason="pong",
            )
            return

        self._process_parsed_text_message(cast_message)

    def _process_parsed_text_message(self, cast_message: Dict[str, Any]) -> None:
        if cast_message.get("hub.mode"):
            return

        event = cast_message.get("event")
        if not event:
            return
        if event.get("hub.event") == "heartbeat":
            return

        if cast_message.get("id") == self._last_published_message_id:
            return

        self._deliver_message(cast_message)

    def _deliver_message(self, cast_message: Dict[str, Any]) -> None:
        if self._on_message:
            self._on_message(cast_message)
        self._schedule_enqueue_message(cast_message)

    async def subscribe(self, *, emit_connecting: bool = True) -> int:
        topic = (self._session_cfg.topic or "").strip()
        if not topic:
            LOGGER.warning("subscribe: no topic defined")
            return 0
        if not self._token:
            LOGGER.warning("subscribe: no token available")
            return 0

        if emit_connecting:
            self._emit_connection_state("connecting")
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Bearer {self._token}",
        }
        http = await self._get_http()
        try:
            async with http.post(
                self._hub.hub_endpoint,
                data=self._subscribe_form_data("subscribe"),
                headers=headers,
            ) as response:
                status = response.status
                if status == 202:
                    body = await response.json()
                    endpoint = body.get("hub.channel.endpoint")
                    if not endpoint:
                        LOGGER.error("subscribe: missing hub.channel.endpoint")
                        return status
                    self._subscribed = True
                    self._resubscribe_requested = False
                    await self._start_websocket(str(endpoint))
                    return status

                if status == 401:
                    LOGGER.warning("subscribe 401: refreshing token")
                    try:
                        auth = await self.authenticate()
                        if auth.get("code"):
                            await self.get_token(auth["code"])
                    except Exception as exc:
                        LOGGER.error("token refresh after 401 failed: %s", exc)
                else:
                    LOGGER.error("subscribe rejected HTTP %s", status)
                return status
        except Exception as exc:
            LOGGER.error("subscribe exception: %s", exc)
            return 0

    async def unsubscribe(self) -> None:
        LOGGER.info(
            "Cast unsubscribe called closed=%s subscribed=%s caller=\n%s",
            self._closed,
            self._subscribed,
            _short_caller_stack(),
        )
        self._subscribed = False
        self._resubscribe_requested = False
        if self._token:
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Bearer {self._token}",
            }
            http = await self._get_http()
            try:
                async with http.post(
                    self._hub.hub_endpoint,
                    data=self._subscribe_form_data("unsubscribe"),
                    headers=headers,
                ) as response:
                    if response.status == 202:
                        LOGGER.debug("unsubscribed from hub")
            except Exception as exc:
                LOGGER.warning("unsubscribe error: %s", exc)
        await self._stop_websocket()
        self._hub_channel_endpoint = ""
        self._emit_connection_state("disconnected")

    async def publish_stow_batch(
        self, cast_message: Dict[str, Any], file_bytes_list: List[bytes]
    ) -> Optional[int]:
        msg = dict(cast_message)
        msg["timestamp"] = msg.get("timestamp") or _utc_timestamp()
        msg["id"] = msg.get("id") or generate_message_id(
            self._options.message_id_prefix
        )
        self._last_published_message_id = msg["id"]

        if msg.get("subscriber.name") is None and self._session_cfg.subscriber_name:
            msg["subscriber.name"] = self._session_cfg.subscriber_name
        if msg.get("subscriber.product.name") is None and self._session_cfg.product_name:
            msg["subscriber.product.name"] = self._session_cfg.product_name

        event = msg.get("event")
        if isinstance(event, dict) and not event.get("hub.topic"):
            event["hub.topic"] = self._session_cfg.topic

        if msg.get("target.actor") is None and self._session_cfg.default_target_actor:
            wire_target = resolve_target_actor_for_wire(
                self._session_cfg.default_target_actor
            )
            if wire_target:
                msg["target.actor"] = wire_target

        msg = normalize_stow_batch_message_metadata_only(msg)
        boundary = f"cast-stow-{generate_message_id(self._options.message_id_prefix)}"
        body = _build_stow_related_body(boundary, json.dumps(msg), list(file_bytes_list))
        content_type = (
            f'multipart/related; boundary="{boundary}"; type="application/dicom"'
        )

        http = await self._get_http()
        try:
            async with http.post(
                self._hub.hub_endpoint,
                data=body,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": content_type,
                },
            ) as response:
                return response.status
        except Exception as exc:
            LOGGER.debug("publish_stow_batch error: %s", exc)
            return None

    async def publish_multipart(
        self, cast_message: Dict[str, Any], file_bytes: bytes
    ) -> Optional[int]:
        msg = dict(cast_message)
        msg["timestamp"] = msg.get("timestamp") or _utc_timestamp()
        msg["id"] = msg.get("id") or generate_message_id(
            self._options.message_id_prefix
        )
        self._last_published_message_id = msg["id"]

        if msg.get("subscriber.name") is None and self._session_cfg.subscriber_name:
            msg["subscriber.name"] = self._session_cfg.subscriber_name
        if msg.get("subscriber.product.name") is None and self._session_cfg.product_name:
            msg["subscriber.product.name"] = self._session_cfg.product_name

        event = msg.get("event")
        if isinstance(event, dict) and not event.get("hub.topic"):
            event["hub.topic"] = self._session_cfg.topic

        if msg.get("target.actor") is None and self._session_cfg.default_target_actor:
            wire_target = resolve_target_actor_for_wire(
                self._session_cfg.default_target_actor
            )
            if wire_target:
                msg["target.actor"] = wire_target

        hub_event = (msg.get("event") or {}).get("hub.event")
        if hub_event == "dicom-send":
            msg = normalize_dicom_send_message_strict(msg)
        elif hub_event == "nifti-send":
            msg = normalize_nifti_send_message_metadata_only(msg)
        else:
            raise ValueError(
                f"CastClient.publish_multipart: unsupported event {hub_event!r}"
            )

        event = msg.get("event") or {}
        file_name = dicom_send_file_name(msg)
        mime_type = "application/dicom"
        for item in _dicom_send_context_items(event or {}):
            resource = item.get("resource")
            if isinstance(resource, dict):
                mt = resource.get("mimeType")
                if isinstance(mt, str) and mt.strip():
                    mime_type = mt.strip()
                break

        form = aiohttp.FormData()
        form.add_field(
            "message",
            json.dumps(msg),
            content_type="application/json",
        )
        form.add_field(
            "file",
            file_bytes,
            filename=file_name,
            content_type=mime_type,
        )

        http = await self._get_http()
        try:
            async with http.post(
                self._hub.hub_endpoint,
                data=form,
                headers={"Authorization": f"Bearer {self._token}"},
            ) as response:
                return response.status
        except Exception as exc:
            LOGGER.debug("publish_multipart error: %s", exc)
            return None

    async def publish(self, cast_message: Dict[str, Any]) -> Optional[int]:
        msg = dict(cast_message)
        msg["timestamp"] = msg.get("timestamp") or _utc_timestamp()
        msg["id"] = msg.get("id") or generate_message_id(
            self._options.message_id_prefix
        )
        self._last_published_message_id = msg["id"]

        if msg.get("subscriber.name") is None and self._session_cfg.subscriber_name:
            msg["subscriber.name"] = self._session_cfg.subscriber_name
        if msg.get("subscriber.product.name") is None and self._session_cfg.product_name:
            msg["subscriber.product.name"] = self._session_cfg.product_name

        event = msg.get("event")
        if isinstance(event, dict) and not event.get("hub.topic"):
            event["hub.topic"] = self._session_cfg.topic

        if msg.get("target.actor") is None and self._session_cfg.default_target_actor:
            wire_target = resolve_target_actor_for_wire(
                self._session_cfg.default_target_actor
            )
            if wire_target:
                msg["target.actor"] = wire_target

        if message_needs_stow_batch_publish(msg):
            file_bytes_list = extract_stow_batch_file_bytes(msg)
            return await self.publish_stow_batch(msg, file_bytes_list)

        if message_needs_multipart_publish(msg):
            file_bytes = extract_first_binary_file_bytes(msg)
            return await self.publish_multipart(msg, file_bytes)

        http = await self._get_http()
        try:
            async with http.post(
                self._hub.hub_endpoint,
                json=msg,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self._token}",
                },
            ) as response:
                return response.status
        except Exception as exc:
            LOGGER.debug("publish error: %s", exc)
            return None

    async def request(
        self,
        *,
        subscriber: str,
        topic: Optional[str] = None,
        data_type: Optional[str] = None,
        actor: Optional[str] = None,
        target_actor: Optional[str] = None,
        product_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        subscriber = (subscriber or "").strip()
        if not subscriber:
            raise ValueError('request: "subscriber.name" is required')
        if not self._token.strip():
            raise ValueError(
                "request: token required (authenticate and get_token first)"
            )

        hub_endpoint = self._hub.hub_endpoint.rstrip("/")
        url = f"{hub_endpoint}/request"
        body: Dict[str, Any] = {
            "subscriber.name": subscriber,
            "id": generate_message_id(self._options.message_id_prefix),
            "timestamp": _utc_timestamp(),
        }
        resolved_topic = (topic or self._session_cfg.topic or "").strip()
        body["event"] = build_cast_request_event(
            data_type=data_type,
            topic=resolved_topic or None,
        )
        if actor and str(actor).strip():
            body["subscriber.actor"] = str(actor).strip()
        wire_target = resolve_target_actor_for_wire(target_actor)
        if wire_target is None and self._session_cfg.default_target_actor:
            wire_target = resolve_target_actor_for_wire(
                self._session_cfg.default_target_actor
            )
        if wire_target:
            body["target.actor"] = wire_target
        wire_product = resolve_target_product_name_for_wire(product_name)
        if wire_product:
            body["target.product.name"] = wire_product

        http = await self._get_http()
        async with http.post(
            url,
            json=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._token}",
            },
        ) as response:
            content_type = response.headers.get("Content-Type", "")
            if "application/json" in content_type:
                try:
                    data = await response.json()
                except Exception:
                    data = ""
            else:
                data = await response.text()
            return {
                "ok": 200 <= response.status < 300,
                "status": response.status,
                "data": data,
            }

    def send_cast_request_response(
        self,
        correlation_id: str,
        data_type: str,
        data: Any,
        topic: Optional[str] = None,
    ) -> None:
        if not self._ws or self._ws.closed:
            LOGGER.warning("send_cast_request_response: websocket not open")
            return
        dt = (data_type or "").strip()
        if not dt:
            LOGGER.error(
                "send_cast_request_response requires a non-empty dataType"
            )
            return

        event_name = response_event_for(dt)
        response: Dict[str, Any] = {
            "timestamp": _utc_timestamp(),
            "id": generate_message_id(self._options.message_id_prefix),
            "subscriber.name": self._session_cfg.subscriber_name or None,
            "subscriber.product.name": self._session_cfg.product_name or None,
            "event": {
                "hub.topic": topic or self._session_cfg.topic,
                "hub.event": event_name,
                "context": {
                    "id": correlation_id,
                    "dataType": dt,
                    "data": data,
                },
            },
        }
        if self._session_cfg.actors:
            response["actor"] = self._session_cfg.actors[0]
        self._schedule_ws_send_str(
            json.dumps(response), reason=event_name or "cast-response"
        )

    async def _reconnect_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(RECONNECT_INTERVAL_SEC)
            if (
                self._resubscribe_requested
                and self._subscribed
                and self._options.auto_reconnect
            ):
                LOGGER.debug("attempting resubscribe")
                self._emit_connection_state("reconnecting")
                self._resubscribe_requested = False
                status = await self.subscribe(emit_connecting=False)
                if status == 202:
                    self._reconnect_fail_streak = 0
                else:
                    self._resubscribe_requested = True
                    self._reconnect_fail_streak += 1
                    if self._reconnect_fail_streak >= RECONNECT_ERROR_THRESHOLD:
                        self._emit_connection_state(
                            "error",
                            {
                                "reason": "reconnect_failed",
                                "status": status,
                                "attempts": self._reconnect_fail_streak,
                            },
                        )

    async def close(self, *, hub_unsubscribe: bool = True) -> None:
        """Release WebSocket and HTTP. Hub unsubscribe is optional (Slicer stays subscribed)."""
        LOGGER.info(
            "Cast close called closed=%s hub_unsubscribe=%s caller=\n%s",
            self._closed,
            hub_unsubscribe,
            _short_caller_stack(),
        )
        self._closed = True
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None
        if hub_unsubscribe:
            await self.unsubscribe()
        else:
            await self._stop_websocket()
            self._emit_connection_state("disconnected")
        if self._owns_http and self._http:
            await self._http.close()
            self._http = None


def hub_event_name(message: Dict[str, Any]) -> str:
    event = message.get("event") or {}
    name = event.get("hub.event")
    return name.lower() if isinstance(name, str) else ""


def request_context(message: Dict[str, Any]) -> Dict[str, Any]:
    event = message.get("event") or {}
    context = event.get("context")
    if isinstance(context, dict):
        return context
    return {}
