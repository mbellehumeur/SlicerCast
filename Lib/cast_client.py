"""Cast hub client library (async, aiohttp).

Mirrors vtk-js ``CastClient`` for OAuth, subscribe, WebSocket bind, publish,
and typed request/response. Hub URLs are supplied by the caller (e.g.
``CastInterface`` Slicer module).

Per-dataType hub.event names: ``<dataType.lower()>-request`` /
``<dataType.lower()>-response``. Keep helpers in sync with:
- ``vtk-js/Sources/IO/Core/CastClient/eventNames.js``
- ``VolView/src/io/cast/event-names.ts``
- the OHIF Cast extension's ``event-names.ts`` (Viewers/extensions/cast)
"""

from __future__ import annotations

import asyncio
import base64
import copy
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
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

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
if not LOGGER.handlers:
    _stderr_handler = logging.StreamHandler(sys.stderr)
    _stderr_handler.setFormatter(
        logging.Formatter("[%(name)s] %(levelname)s: %(message)s")
    )
    LOGGER.addHandler(_stderr_handler)
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False


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


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    return raw if raw else default


# WS binary fan-out preference advertised to the hub at subscribe time via
# the ``hub.binaryTransfer`` form field. "ws-chunks" asks the hub to send
# nifti-send / dicom-send as one text envelope + N BINARY frames (chunked) so
# the aiohttp receiver can yield control between frames. "ws" (default for
# legacy) keeps the single-frame path. Other values are sent as-is so the
# hub can decide whether to honor them.
CAST_CLIENT_BINARY_TRANSFER = _env_str(
    "CAST_CLIENT_BINARY_TRANSFER", "ws-chunks"
).lower()


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
    client_info: Dict[str, str] = field(default_factory=dict)
    default_target_actor: str = ""


@dataclass
class CastClientOptions:
    auto_reconnect: bool = False
    auto_start: bool = False
    preserve_session_topic_from_token: bool = False
    message_id_prefix: str = DEFAULT_MESSAGE_ID_PREFIX
    quiet_hub_errors: bool = False


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_message_id(prefix: str = DEFAULT_MESSAGE_ID_PREFIX) -> str:
    return prefix + uuid.uuid4().hex[:16]


def resolve_target_actor_for_wire(value: Optional[str]) -> Optional[str]:
    """Return wire ``target.actor``, or None when empty (``*`` is sent as ``*``)."""
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


_CAST_BINARY_TRANSFER_EVENTS = frozenset({"dicom-send", "nifti-send"})


def cast_binary_transfer_waits_for_binary_frame(event: Dict[str, Any]) -> bool:
    """True when hub sent JSON first and file bytes follow on the WebSocket."""
    if event.get("hub.event") not in _CAST_BINARY_TRANSFER_EVENTS:
        return False
    for item in _dicom_send_context_items(event):
        resource = item.get("resource")
        if not isinstance(resource, dict):
            continue
        marker = resource.get("binaryTransfer")
        if marker is True or marker == "ws-chunks":
            data = resource.get("data")
            if data is None or data == "":
                return True
    return False


def cast_binary_transfer_mode(event: Dict[str, Any]) -> str:
    """Return ``"ws"`` for single-frame, ``"ws-chunks"`` for chunked, ``""`` otherwise."""
    if event.get("hub.event") not in _CAST_BINARY_TRANSFER_EVENTS:
        return ""
    for item in _dicom_send_context_items(event):
        resource = item.get("resource")
        if not isinstance(resource, dict):
            continue
        marker = resource.get("binaryTransfer")
        if marker is True:
            return "ws"
        if marker == "ws-chunks":
            return "ws-chunks"
    return ""


def dicom_send_waits_for_binary_frame(event: Dict[str, Any]) -> bool:
    return cast_binary_transfer_waits_for_binary_frame(event)


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
        import slicer

        version = slicer.app.applicationVersion
        if version and str(version).strip():
            info["userAgent"] = f"3D Slicer {str(version).strip()}"
    except Exception:
        pass
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
    if not isinstance(item, dict):
        raise ValueError("CastClient: dicom-send context items must be objects")
    resource = item.get("resource")
    if not isinstance(resource, dict):
        raise ValueError("CastClient: dicom-send context item missing resource object")
    if "data" not in resource:
        raise ValueError("CastClient: dicom-send resource.data is required")
    if isinstance(resource.get("data"), str):
        raise ValueError(
            "CastClient: dicom-send string payloads are not supported; "
            "pass binary input instead"
        )

    raw = _read_binary_strict(resource["data"])
    normalized_resource = copy.deepcopy(resource)
    normalized_resource["data"] = base64.standard_b64encode(raw).decode("ascii")
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
    normalized_resource["binaryTransfer"] = True
    normalized_resource["byteLength"] = len(raw)
    return {**item, "resource": normalized_resource}


def normalize_dicom_send_message_strict(msg: Dict[str, Any]) -> Dict[str, Any]:
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
    if not isinstance(item, dict):
        raise ValueError("CastClient: nifti-send context items must be objects")
    resource = item.get("resource")
    if not isinstance(resource, dict):
        raise ValueError("CastClient: nifti-send context item missing resource object")
    if "data" not in resource:
        raise ValueError("CastClient: nifti-send resource.data is required")
    if isinstance(resource.get("data"), str):
        raise ValueError(
            "CastClient: nifti-send string payloads are not supported; "
            "pass binary input instead"
        )

    raw = _read_binary_strict(resource["data"])
    normalized_resource = copy.deepcopy(resource)
    normalized_resource["data"] = base64.standard_b64encode(raw).decode("ascii")
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
    normalized_resource["binaryTransfer"] = True
    normalized_resource["byteLength"] = len(raw)
    return {**item, "resource": normalized_resource}


def normalize_nifti_send_message_strict(msg: Dict[str, Any]) -> Dict[str, Any]:
    event = msg.get("event")
    if not isinstance(event, dict):
        raise ValueError("CastClient: nifti-send requires event object")
    if event.get("hub.event") != "nifti-send":
        return msg

    items = _dicom_send_context_items(event)
    if not items:
        raise ValueError("CastClient: nifti-send requires non-empty event.context")

    normalized_context = [
        normalize_nifti_send_context_item(item) for item in items
    ]
    out = copy.deepcopy(msg)
    out["event"] = {**event, "context": normalized_context}
    return out


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
        self._hub = hub
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
        self._pending_dicom_send: Optional[Dict[str, Any]] = None
        self._pending_dicom_send_started_at: Optional[float] = None
        self._pending_dicom_send_byte_length: Optional[int] = None
        self._pending_chunks: Optional[List[bytes]] = None
        self._pending_chunks_received: int = 0
        self._pending_chunks_total: int = 0
        self._skip_next_dicom_binary = False
        self._hub_channel_endpoint: str = ""
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None

        if self._options.auto_reconnect:
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    def _emit_connection_state(
        self, state: str, detail: Optional[Dict[str, Any]] = None
    ) -> None:
        if self._on_connection_state:
            self._on_connection_state(state, detail)

    async def _get_http(self) -> aiohttp.ClientSession:
        if self._http is None:
            # total=None: do not cap total time. The session is reused by both
            # short HTTP calls (OAuth/subscribe/publish) and long-lived
            # WebSocket connections (/bind/<endpoint>). A finite ``total`` is
            # carried into ws_connect's request and, on aiohttp versions where
            # the request-level timer stays armed for the lifetime of the WS
            # response, would fire mid-receive on large WS binary frames
            # (e.g. nifti-send). ``connect=30`` still caps connection setup.
            self._http = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None, connect=30)
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
        if hub_mode == "subscribe" and CAST_CLIENT_BINARY_TRANSFER:
            data["hub.binaryTransfer"] = CAST_CLIENT_BINARY_TRANSFER
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
            if not self._options.quiet_hub_errors:
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
                if not self._options.quiet_hub_errors:
                    LOGGER.error("get_token failed HTTP %s", response.status)
                return False
            config = await response.json()

        if isinstance(config.get("access_token"), str) and config["access_token"]:
            self._token = config["access_token"]
        if isinstance(config.get("id_token"), str) and config["id_token"]:
            self._last_id_token = config["id_token"]
        if isinstance(config.get("subscriber_name"), str) and config[
            "subscriber_name"
        ]:
            self._session_cfg.subscriber_name = config["subscriber_name"]
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
        self._pending_dicom_send = None
        self._pending_dicom_send_started_at = None
        self._pending_dicom_send_byte_length = None
        self._pending_chunks = None
        self._pending_chunks_received = 0
        self._pending_chunks_total = 0
        self._skip_next_dicom_binary = False
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

    async def _websocket_reader(self) -> None:
        assert self._ws is not None
        reader_started_at = time.monotonic()
        last_text_at: Optional[float] = None
        last_binary_at: Optional[float] = None
        last_msg_type: Optional[str] = None
        text_frames = 0
        binary_frames = 0
        binary_bytes = 0
        close_code: Optional[int] = None
        close_extra: Any = None
        reader_exc: Optional[BaseException] = None
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    last_msg_type = "TEXT"
                    last_text_at = time.monotonic()
                    text_frames += 1
                    await self._handle_websocket_text(msg.data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    last_msg_type = "BINARY"
                    last_binary_at = time.monotonic()
                    binary_frames += 1
                    binary_bytes += len(msg.data)
                    LOGGER.info(
                        "Cast websocket binary frame received bytes=%d total_bytes=%d frame_count=%d",
                        len(msg.data),
                        binary_bytes,
                        binary_frames,
                    )
                    await asyncio.to_thread(
                        self._process_binary_message, msg.data
                    )
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    last_msg_type = "ERROR"
                    close_code = self._ws.close_code if self._ws else None
                    reader_exc = self._ws.exception() if self._ws else None
                    LOGGER.warning(
                        "Cast websocket protocol error close_code=%s exc=%r",
                        close_code,
                        reader_exc,
                    )
                    break
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                ):
                    last_msg_type = msg.type.name
                    close_code = self._ws.close_code if self._ws else None
                    close_extra = getattr(msg, "extra", None)
                    LOGGER.info(
                        "Cast websocket close frame type=%s close_code=%s reason=%s",
                        msg.type.name,
                        close_code,
                        close_extra,
                    )
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reader_exc = exc
            LOGGER.warning("websocket reader error: %s", exc)
        finally:
            now = time.monotonic()
            uptime = now - reader_started_at
            since_last_binary = (
                f"{now - last_binary_at:.1f}s" if last_binary_at else "n/a"
            )
            since_last_text = (
                f"{now - last_text_at:.1f}s" if last_text_at else "n/a"
            )
            pending = self._pending_dicom_send
            pending_summary = None
            if pending is not None:
                event = pending.get("event") or {}
                pending_summary = {
                    "id": pending.get("id"),
                    "event": event.get("hub.event"),
                }
                LOGGER.warning(
                    "websocket closed before binary completed id=%s event=%s",
                    pending.get("id", ""),
                    event.get("hub.event", ""),
                )
            if close_code is None and self._ws is not None:
                close_code = self._ws.close_code
            LOGGER.info(
                "Cast websocket reader exit uptime=%.1fs last_msg=%s "
                "text_frames=%d binary_frames=%d binary_bytes=%d "
                "since_last_binary=%s since_last_text=%s "
                "close_code=%s reason=%s exc=%r pending=%s",
                uptime,
                last_msg_type,
                text_frames,
                binary_frames,
                binary_bytes,
                since_last_binary,
                since_last_text,
                close_code,
                close_extra,
                reader_exc,
                pending_summary,
            )
            self._pending_dicom_send = None
            self._pending_dicom_send_started_at = None
            self._pending_dicom_send_byte_length = None
            self._pending_chunks = None
            self._pending_chunks_received = 0
            self._pending_chunks_total = 0
            self._skip_next_dicom_binary = False
            if not self._closed:
                self._resubscribe_requested = True
                self._emit_connection_state("disconnected")

    def _process_binary_message(self, data: bytes) -> None:
        if self._skip_next_dicom_binary:
            self._skip_next_dicom_binary = False
            return

        pending = self._pending_dicom_send
        if not pending or not isinstance(pending.get("event"), dict):
            LOGGER.warning("unexpected binary WebSocket message")
            return

        chunked_frame_count = 0
        if self._pending_chunks is not None:
            self._pending_chunks.append(data)
            self._pending_chunks_received += len(data)
            if (
                self._pending_chunks_total > 0
                and self._pending_chunks_received < self._pending_chunks_total
            ):
                if (
                    len(self._pending_chunks) == 1
                    or len(self._pending_chunks) % 4 == 0
                ):
                    LOGGER.info(
                        "Cast binary transfer chunk id=%s event=%s "
                        "received=%d/%d frames=%d",
                        pending.get("id", ""),
                        pending["event"].get("hub.event", ""),
                        self._pending_chunks_received,
                        self._pending_chunks_total,
                        len(self._pending_chunks),
                    )
                return
            chunked_frame_count = len(self._pending_chunks)
            data = b"".join(self._pending_chunks)
            self._pending_chunks = None
            self._pending_chunks_received = 0
            self._pending_chunks_total = 0

        self._pending_dicom_send = None
        started_at = self._pending_dicom_send_started_at
        announced_bytes = self._pending_dicom_send_byte_length
        self._pending_dicom_send_started_at = None
        self._pending_dicom_send_byte_length = None
        event = pending["event"]
        attached = False
        for item in _dicom_send_context_items(event):
            resource = item.get("resource")
            if not isinstance(resource, dict):
                continue
            marker = resource.get("binaryTransfer")
            if marker is True or marker == "ws-chunks":
                resource = dict(resource)
                resource.pop("binaryTransfer", None)
                resource.pop("chunkSize", None)
                resource.pop("chunkCount", None)
                resource["data"] = data
                resource["byteLength"] = len(data)
                item["resource"] = resource
                attached = True
                break

        if not attached:
            LOGGER.warning("binary frame did not match pending binary Cast message")
            return
        LOGGER.info(
            "dicom-send binary frame attached id=%s bytes=%d",
            pending.get("id", ""),
            len(data),
        )
        if started_at is not None:
            elapsed = max(time.monotonic() - started_at, 0.0)
            throughput = (
                f"{(len(data) / (1024 * 1024)) / elapsed:.2f}"
                if elapsed > 0
                else "n/a"
            )
            LOGGER.info(
                "Cast binary transfer end id=%s event=%s mode=%s bytes=%d "
                "announcedBytes=%s frames=%d elapsed=%.2fs throughput=%s MB/s",
                pending.get("id", ""),
                event.get("hub.event", ""),
                "ws-chunks" if chunked_frame_count > 0 else "ws",
                len(data),
                announced_bytes,
                chunked_frame_count if chunked_frame_count > 0 else 1,
                elapsed,
                throughput,
            )
        if pending.get("id") == self._last_published_message_id:
            return
        self._deliver_message(pending)

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
            if cast_binary_transfer_waits_for_binary_frame(event):
                self._skip_next_dicom_binary = True
            return

        if cast_binary_transfer_waits_for_binary_frame(event):
            self._pending_dicom_send = cast_message
            self._pending_dicom_send_started_at = time.monotonic()
            self._pending_dicom_send_byte_length = dicom_send_byte_length(
                cast_message
            )
            mode = cast_binary_transfer_mode(event)
            if mode == "ws-chunks":
                self._pending_chunks = []
                self._pending_chunks_received = 0
                self._pending_chunks_total = (
                    self._pending_dicom_send_byte_length or 0
                )
            else:
                self._pending_chunks = None
                self._pending_chunks_received = 0
                self._pending_chunks_total = 0
            LOGGER.info(
                "Cast binary transfer start id=%s event=%s byteLength=%s mode=%s",
                cast_message.get("id", ""),
                (event or {}).get("hub.event", ""),
                self._pending_dicom_send_byte_length,
                mode or "ws",
            )
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
                        if not self._options.quiet_hub_errors:
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
                        if not self._options.quiet_hub_errors:
                            LOGGER.error("token refresh after 401 failed: %s", exc)
                elif not self._options.quiet_hub_errors:
                    LOGGER.error("subscribe rejected HTTP %s", status)
                return status
        except Exception as exc:
            if not self._options.quiet_hub_errors:
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

        msg = normalize_dicom_send_message_strict(msg)
        msg = normalize_nifti_send_message_strict(msg)

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
