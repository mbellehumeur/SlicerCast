#!/usr/bin/env python
"""
Standalone Cast Hub implementation.
This is a complete, standalone implementation that doesn't depend on WebServer.py.
Uses FastAPI for HTTP server and WebSocket support.

Run with: python cast_api.py

Requirements:
    pip install fastapi uvicorn
    
    Optional (for form data support):
    pip install python-multipart
    
    Note: Form data will work without python-multipart by parsing raw body,
    but python-multipart provides better form data parsing support.

    slicer.util.pip_install('fastapi', 'uvicorn', 'python-multipart', 'gunicorn')

Environment (hub WebSocket keepalive, default off):
    CAST_HUB_WS_KEEPALIVE
        Set to 1/true/yes/on to send JSON {"type":"ping"} on subscriber /bind/ sockets.
    CAST_HUB_WS_KEEPALIVE_INTERVAL_SECONDS
        Ping interval when keepalive is enabled (default 30).

Environment (uvicorn WebSocket protocol PING/PONG, default on):
    CAST_HUB_UVICORN_WS_PING_INTERVAL_SECONDS
        Numeric seconds between uvicorn WS protocol pings (default 20).
        Set to 0 to disable.
    CAST_HUB_UVICORN_WS_PING_TIMEOUT_SECONDS
        PONG-wait timeout when uvicorn pings are enabled (defaults to the interval).

Environment (multipart binary payload store):
    CAST_HUB_HTTP_PAYLOAD_TTL_SECONDS
        TTL in seconds for stored payloads served via GET
        /api/hub/payloads/{payloadId} (default 300). A background reaper
        evicts expired entries every min(30s, ttl/4).
    CAST_HUB_HTTP_PAYLOAD_MAX_TOTAL_BYTES
        Soft cap on total in-flight stored payload bytes (default 2 GiB).
        Registrations beyond this cap fan out metadata-only JSON (no
        payloadId; the message is never dropped).

Environment (binary transfer filename policy, default on):
    CAST_HUB_FILENAME_POLICY
        Set to off/0/false/no to disable allowlist checks.
    CAST_HUB_ALLOWED_EXTENSIONS
        Comma-separated suffixes (e.g. .dcm,.nii.gz). Defaults are documented
        in filename-policy.md.
"""

import sys
import os
import json
import uuid
import time
import asyncio
import base64
import copy
import urllib.request
import hmac
import hashlib
import re
import secrets
import logging
from email import policy
from email.parser import BytesParser
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs

# cast_api.py is run as a script (see module docstring), so sibling imports
# are the right form here.
_base_dir = os.path.dirname(os.path.abspath(__file__))
_lib_dir = os.path.join(_base_dir, "Lib")
if _lib_dir not in sys.path:
    sys.path.insert(0, _lib_dir)
from cast_client import (
    data_type_from_event_name,
    is_cast_binary_event,
    is_request_event,
    is_response_event,
)
from cast_filename_policy import (
    FilenamePolicyError,
    enforce_transfer_filenames_for_notification,
)
from hub_metrics import collect_hub_metrics

# Default cast-request fan-out timeout (seconds). Was 5s; bumped because
# multiple subscribers may now respond to the same request.
CAST_REQUEST_TIMEOUT_SECONDS = float(os.getenv("CAST_REQUEST_TIMEOUT_SECONDS", "10"))


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


CAST_HUB_WS_KEEPALIVE = _env_flag("CAST_HUB_WS_KEEPALIVE", default=False)
CAST_HUB_WS_KEEPALIVE_INTERVAL_SECONDS = float(
    os.getenv("CAST_HUB_WS_KEEPALIVE_INTERVAL_SECONDS", "30")
)


def _env_optional_float(name: str) -> Optional[float]:
    raw = os.getenv(name)
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _env_uvicorn_ws_ping_seconds(name: str, default: float) -> Optional[float]:
    """Return ping interval/timeout in seconds; ``0`` disables; unset uses ``default``."""
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else None


CAST_HUB_UVICORN_WS_PING_INTERVAL = _env_uvicorn_ws_ping_seconds(
    "CAST_HUB_UVICORN_WS_PING_INTERVAL_SECONDS", 20.0
)
CAST_HUB_UVICORN_WS_PING_TIMEOUT = (
    _env_uvicorn_ws_ping_seconds(
        "CAST_HUB_UVICORN_WS_PING_TIMEOUT_SECONDS",
        CAST_HUB_UVICORN_WS_PING_INTERVAL or 20.0,
    )
    or CAST_HUB_UVICORN_WS_PING_INTERVAL
)


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


# Multipart binary ingest: publishers POST ``multipart/related`` (Cast JSON +
# file parts). The hub stores bytes under short-lived ``payloadId`` values and
# fans out text-only WebSocket JSON with ``context.files[].payloadId``. Subscribers
# GET ``/api/hub/payloads/{payloadId}`` when the application chooses to download.
CAST_HUB_HTTP_PAYLOAD_TTL_SECONDS = _env_positive_int(
    "CAST_HUB_HTTP_PAYLOAD_TTL_SECONDS", 300
)
# Soft cap on total in-flight payload bytes. If a registration would exceed
# this, the hub strips the http marker and fans out metadata-only JSON.
CAST_HUB_HTTP_PAYLOAD_MAX_TOTAL_BYTES = _env_positive_int(
    "CAST_HUB_HTTP_PAYLOAD_MAX_TOTAL_BYTES", 2 * 1024 * 1024 * 1024
)
# Chunk size when streaming GET /api/hub/payloads/{token} bodies. Avoids
# Response(content=bytes) which can be throttled by BaseHTTPMiddleware-style
# body teeing and many small uvicorn writes on Windows.
CAST_HUB_HTTP_PAYLOAD_SEND_CHUNK_BYTES = _env_positive_int(
    "CAST_HUB_HTTP_PAYLOAD_SEND_CHUNK_BYTES", 4 * 1024 * 1024
)


class _HttpPayloadEntry:
    """One stored payload (raw bytes + metadata) keyed by random payloadId."""

    __slots__ = (
        "raw",
        "byte_length",
        "expires_at",
        "file_name",
        "mime_type",
        "created_at",
    )

    def __init__(
        self,
        raw: bytes,
        byte_length: int,
        expires_at: datetime,
        file_name: str,
        mime_type: str,
        created_at: datetime,
    ) -> None:
        self.raw = raw
        self.byte_length = byte_length
        self.expires_at = expires_at
        self.file_name = file_name
        self.mime_type = mime_type
        self.created_at = created_at


class _HttpPayloadStore:
    """In-memory payloadId -> bytes store for multipart binary publishes.

    All access happens on the hub's single asyncio loop so we do not need a
    lock. Memory is bounded by ``CAST_HUB_HTTP_PAYLOAD_MAX_TOTAL_BYTES``; on
    overflow ``register`` returns ``None`` and the caller fans out
    metadata-only JSON.
    """

    def __init__(self) -> None:
        self._entries: Dict[str, _HttpPayloadEntry] = {}
        self._total_bytes: int = 0

    def register(
        self,
        raw: bytes,
        file_name: str,
        mime_type: str,
        ttl_seconds: int,
        max_total_bytes: int,
    ) -> Optional[tuple]:
        size = len(raw)
        if max_total_bytes > 0 and self._total_bytes + size > max_total_bytes:
            return None
        token = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=max(int(ttl_seconds), 1))
        self._entries[token] = _HttpPayloadEntry(
            raw=raw,
            byte_length=size,
            expires_at=expires_at,
            file_name=(file_name or "").strip(),
            mime_type=(mime_type or "").strip()
            or "application/octet-stream",
            created_at=now,
        )
        self._total_bytes += size
        return token, expires_at

    def get(self, token: str) -> Optional[_HttpPayloadEntry]:
        entry = self._entries.get(token)
        if entry is None:
            return None
        if entry.expires_at < datetime.now(timezone.utc):
            return None
        return entry

    def evict_expired(self) -> int:
        now = datetime.now(timezone.utc)
        expired = [t for t, e in self._entries.items() if e.expires_at < now]
        for token in expired:
            entry = self._entries.pop(token, None)
            if entry is not None:
                self._total_bytes -= entry.byte_length
        return len(expired)

    def total_bytes(self) -> int:
        return self._total_bytes

    def count(self) -> int:
        return len(self._entries)


_http_payload_store = _HttpPayloadStore()

cast_hub_logger = logging.getLogger("cast_hub")

_UPPERCASE_GET_REQUEST_DATA_TYPES = frozenset(
    {
        "pngthumbnail",
        "jpgthumbnail",
        "pngfullsize",
        "jpgfullsize",
    }
)


def _cast_request_event_dict(request_data: Dict) -> Dict[str, Any]:
    event = request_data.get("event")
    return event if isinstance(event, dict) else {}


def _request_hub_event_from_body(request_data: Dict) -> str:
    return str(_cast_request_event_dict(request_data).get("hub.event", "")).strip().lower()


def _request_topic_from_body(request_data: Dict) -> str:
    topic = _cast_request_event_dict(request_data).get("hub.topic")
    return str(topic).strip() if topic is not None and str(topic).strip() else ""


def _request_context_data_type(
    request_data: Dict, request_event_name: str
) -> Optional[str]:
    event = _cast_request_event_dict(request_data)
    context = event.get("context")
    if isinstance(context, dict):
        data_type = context.get("dataType")
        if data_type is not None and str(data_type).strip():
            return str(data_type).strip()
    base = data_type_from_event_name(request_event_name)
    if not base:
        return None
    if base in _UPPERCASE_GET_REQUEST_DATA_TYPES:
        return base.upper()
    return base


def _summarize_websocket_message(message: Any, *, max_len: int = 200) -> str:
    """Short summary for WebSocket traffic (avoid logging full payloads)."""
    if not isinstance(message, dict):
        text = str(message)
        return text if len(text) <= max_len else text[:max_len] + "..."

    msg_type = message.get("type")
    if msg_type in ("ping", "pong"):
        return f"type={msg_type}"

    event = message.get("event")
    if isinstance(event, dict):
        hub_event = event.get("hub.event", "")
        context = event.get("context")
        request_id = ""
        if isinstance(context, dict) and context.get("id"):
            request_id = f" id={context.get('id')}"
        sub = message.get("subscriber.name") or message.get("subscriber") or ""
        sub_part = f" subscriber={sub}" if sub else ""
        return f"hub.event={hub_event}{request_id}{sub_part}"

    if msg_type:
        return f"type={msg_type}"

    keys = ",".join(sorted(message.keys())[:8])
    return f"keys=[{keys}]"

# Try to import FastAPI - install with: pip install fastapi uvicorn
try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import (
        JSONResponse,
        FileResponse,
        RedirectResponse,
        StreamingResponse,
    )
    from fastapi.staticfiles import StaticFiles
    import uvicorn
except ImportError:
    print("ERROR: FastAPI not installed. Install with: pip install fastapi uvicorn")
    sys.exit(1)


app = FastAPI(title="Cast Hub")


def hub_path_excluded_from_request_log(path: str) -> bool:
    """Hub UI polling paths omitted from cast_hub log and uvicorn access log."""
    if path == "/api/hub/admin" or path.startswith("/api/hub/admin/"):
        return True
    if path.startswith("/api/hub/payloads/"):
        return True
    return False


def path_suppress_uvicorn_access_log(path: str, method: str) -> bool:
    """Skip uvicorn access lines when cast_hub middleware already logs the request."""
    if hub_path_excluded_from_request_log(path):
        return True
    if path.startswith("/api/hub") and method in ("GET", "POST", "DELETE"):
        return True
    if path in ("/oauth/token", "/oauth/authorize") and method == "POST":
        return True
    return False


_UVICORN_ACCESS_PATH_RE = re.compile(r' - "([A-Z]+) ([^\s?]+)')


class UvicornAccessLogFilter(logging.Filter):
    """Suppress duplicate uvicorn access lines (cast_hub middleware logs these)."""

    def filter(self, record: logging.LogRecord) -> bool:
        match = _UVICORN_ACCESS_PATH_RE.search(record.getMessage())
        if not match:
            return True
        method, path = match.group(1), match.group(2)
        return not path_suppress_uvicorn_access_log(path, method)


def _install_uvicorn_access_log_filter() -> None:
    """Apply after uvicorn configures logging (see startup hook below)."""
    filt = UvicornAccessLogFilter()
    logger = logging.getLogger("uvicorn.access")
    if not any(isinstance(f, UvicornAccessLogFilter) for f in logger.filters):
        logger.addFilter(filt)
    for handler in logger.handlers:
        if not any(isinstance(f, UvicornAccessLogFilter) for f in handler.filters):
            handler.addFilter(filt)


def _configure_cast_hub_logging() -> None:
    """Ensure cast_hub logger emits INFO+ when no handler is configured yet."""
    if not cast_hub_logger.handlers and not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    cast_hub_logger.setLevel(logging.INFO)


@app.on_event("startup")
async def _quiet_hub_uvicorn_access_logs() -> None:
    _configure_cast_hub_logging()
    _install_uvicorn_access_log_filter()
    if CAST_HUB_WS_KEEPALIVE:
        cast_hub_logger.info(
            "Cast hub WebSocket keepalive: on (interval=%ss, JSON ping/pong)",
            CAST_HUB_WS_KEEPALIVE_INTERVAL_SECONDS,
        )
    else:
        cast_hub_logger.info(
            "Cast hub WebSocket keepalive: off "
            "(set CAST_HUB_WS_KEEPALIVE=true to enable)"
        )
    if CAST_HUB_UVICORN_WS_PING_INTERVAL is None:
        cast_hub_logger.info(
            "Cast hub uvicorn WS protocol PING: off "
            "(set CAST_HUB_UVICORN_WS_PING_INTERVAL_SECONDS=0 to keep off)"
        )
    else:
        cast_hub_logger.info(
            "Cast hub uvicorn WS protocol PING: on (interval=%ss, timeout=%ss)",
            CAST_HUB_UVICORN_WS_PING_INTERVAL,
            CAST_HUB_UVICORN_WS_PING_TIMEOUT,
        )
    cast_hub_logger.info(
        "Cast hub http payload store: ttl=%ss max_total_bytes=%s",
        CAST_HUB_HTTP_PAYLOAD_TTL_SECONDS,
        CAST_HUB_HTTP_PAYLOAD_MAX_TOTAL_BYTES,
    )
    asyncio.create_task(_http_payload_reaper())


# Enable CORS with explicit configuration for Azure
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)


# Mount static files directory (HTML/CSS/images only — not Lib Python sources)
base_dir = _base_dir
resources_dir = os.path.join(base_dir, "Resources")
if os.path.exists(resources_dir):
    app.mount("/static", StaticFiles(directory=resources_dir), name="static")


def _subscription_topic_matches(sub_topic: str, event_topic: str) -> bool:
    """True when a subscription should receive publishes for ``event_topic``."""
    sub_topic = (sub_topic or "").strip()
    event_topic = (event_topic or "").strip()
    if sub_topic == "*":
        return bool(event_topic)
    return event_topic == sub_topic


def _subscription_handles_event(sub: Dict, topic_name: str, event_type: str) -> bool:
    """Topic plus event filter — same eligibility as delivery (before channel / echo checks)."""
    if not _subscription_topic_matches(sub.get("topic", ""), topic_name):
        return False
    subscribed_events = (sub.get("events") or "").lower()
    et = event_type.lower()
    return et in subscribed_events or "*" in subscribed_events


def _actor_to_text(actor: Any) -> str:
    if isinstance(actor, str):
        return actor.strip()
    if isinstance(actor, dict):
        candidate = actor.get("id") or actor.get("key") or actor.get("name")
        return str(candidate).strip() if candidate is not None else ""
    if actor is None:
        return ""
    return str(actor).strip()


def _normalize_target_actor(value: Any) -> str:
    """Return destination filter keyword, or empty for missing / ``*`` (no filter)."""
    text = _actor_to_text(value)
    if text == "*":
        return ""
    return text


def _subscription_actor_list(sub: Dict) -> List[str]:
    actors = sub.get("actors") or []
    if isinstance(actors, list):
        return [str(a).strip() for a in actors if str(a).strip()]
    text = str(actors).strip()
    return [text] if text else []


def _response_identity_from_ws_message(
    message: Any,
    *,
    endpoint: Optional[str] = None,
    subscriptions: Optional[List[Dict]] = None,
) -> Dict[str, str]:
    """Subscriber and actor from a bind-socket *-response envelope."""
    if not isinstance(message, dict):
        return {"subscriber": "", "actor": ""}
    subscriber_name = str(
        message.get("subscriber.name") or message.get("subscriber") or ""
    ).strip()
    actor_name = _actor_to_text(message.get("actor"))
    event = message.get("event")
    if isinstance(event, dict):
        context = event.get("context")
        if not actor_name and isinstance(context, dict):
            actor_name = _actor_to_text(context.get("actor"))
        if not subscriber_name:
            sender = event.get("sender")
            if isinstance(sender, dict):
                subscriber_name = str(sender.get("subscriber", "")).strip()
    if not subscriber_name and endpoint:
        subs = (
            subscriptions if subscriptions is not None else cast_hub.get_subscriptions()
        )
        for sub in subs:
            if sub.get("websocket_endpoint") == endpoint:
                subscriber_name = str(sub.get("subscriber", "")).strip()
                break
    return {"subscriber": subscriber_name, "actor": actor_name}


def _subscription_accepts_target_actor(sub: Dict, target_actor: str) -> bool:
    if not target_actor:
        return True
    return target_actor in _subscription_actor_list(sub)


def _subscription_product_name(sub: Dict) -> str:
    client_info = sub.get("client_info") or {}
    if not isinstance(client_info, dict):
        return ""
    product = client_info.get("productName")
    if product is None:
        return ""
    return str(product).strip()


def _target_product_name_from_payload(payload: Dict) -> str:
    """Destination product filter (``target.product.name`` on publish/request)."""
    raw = payload.get("target.product.name")
    if raw is None:
        return ""
    return str(raw).strip()


def _target_product_filter_active(target_product_name: str) -> bool:
    return bool(target_product_name) and target_product_name != "*"


def _subscription_accepts_target_product(sub: Dict, target_product_name: str) -> bool:
    if not target_product_name or target_product_name == "*":
        return True
    return _subscription_product_name(sub) == target_product_name


def _target_actor_from_payload(payload: Dict) -> str:
    """Destination filter from publish payload (``target.actor`` on envelope root)."""
    return _normalize_target_actor(payload.get("target.actor"))


def _target_actor_filter_from_request_body(request_data: Dict) -> str:
    """Hub request routing: ``target.actor`` only (``*`` = all roles; omit = all roles)."""
    if "target.actor" in request_data:
        return _normalize_target_actor(request_data.get("target.actor"))
    return ""


def _format_hub_events_display(hub_events: Any) -> str:
    """Human-readable hub.events for subscription INFO logs and admin alignment."""
    if hub_events is None:
        return "(none)"
    text = str(hub_events).strip()
    if not text:
        return "(empty)"
    if text == "*":
        return "*"
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if len(parts) > 1:
        return "[" + ", ".join(parts) + "]"
    return text


def _subscription_log_message(
    action: str,
    *,
    subscriber_name: str,
    hub_topic: str,
    channel_type: str,
    hub_events: Any,
    subscriber_actors: List[str],
    subscriber_product: str = "",
    subscriber_version: str = "",
    websocket_endpoint: str = "",
) -> str:
    actors_display = (
        "[" + ", ".join(subscriber_actors) + "]" if subscriber_actors else "[]"
    )
    parts = [
        f"Subscription {action}: subscriber={subscriber_name}",
        f"topic={hub_topic or '(empty)'}",
        f"channel={channel_type}",
        f"events={_format_hub_events_display(hub_events)}",
        f"actors={actors_display}",
    ]
    if subscriber_product:
        parts.append(f"product={subscriber_product}")
    if subscriber_version:
        parts.append(f"version={subscriber_version}")
    if websocket_endpoint:
        parts.append(f"endpoint={websocket_endpoint}")
    return " ".join(parts)


class CastHub:
    """Cast Hub implementation for managing subscriptions and broadcasting events"""
    
    def __init__(self):
        self.subscriptions: List[Dict] = []
        self.websocket_connections: Dict[str, WebSocket] = {}  # endpoint -> WebSocket
        self.admin_websockets: List[Dict] = []  # Track admin connections with metadata: [{"websocket": WebSocket, "location": str, "connected_at": str}]
        self.conferences: List[Dict] = []
        self.audit_log: List[Dict] = []  # List of logged events
        self.audit_log_counter: int = 0  # Incrementing message number
        self.server_port = 2018
        self.user_count = 0
        self.last_admin_refresh_time: float = 0.0  # Track last admin refresh time for rate limiting
        self.pending_admin_refresh_task = None  # Track pending refresh task (to cancel if needed)
        self.admin_revision: int = 0  # Bumped when admin clients are notified of hub changes
        self.single_user_mode: bool = False  # When enabled, token endpoint always returns topic 'SINGLE-USER'
        # request_id -> { actor, product_name, expected, subscriber_products,
        #                  responses, seen_envelope_ids, event_name,
        #                  completion: asyncio.Event, lock: asyncio.Lock,
        #                  data_type, started_at, request_topic }
        self.pending_requests: Dict[str, Dict[str, Any]] = {}
        # OAuth authorization codes: code -> { user_name, topic, count, expires_at, used }
        self.auth_codes: Dict[str, Dict[str, Any]] = {}

    def log(self, message: str, level: int = logging.INFO) -> None:
        """Log hub diagnostics via the cast_hub logger."""
        cast_hub_logger.log(level, message)

    def set_server_port(self, port: int):
        """Set the server port for generating WebSocket URLs"""
        self.server_port = port
    
    def check_subscription_request(self, subscription_request: Dict) -> Dict:
        """Verify WebSub subscription callback"""
        callback = subscription_request.get("hub.callback") or subscription_request.get("hub_callback")
        secret = subscription_request.get("hub.secret") or subscription_request.get("hub_secret")
        topic = subscription_request.get("hub.topic") or subscription_request.get("hub_topic")
        
        if not callback or not secret or not topic:
            return {"status": 400, "data": "Missing required parameters"}
        
        try:
            challenge_url = f"{callback}?hub.challenge={secret}&hub.topic={topic}"
            req = urllib.request.Request(challenge_url)
            with urllib.request.urlopen(req, timeout=5) as response:
                status = response.getcode()
                data = response.read().decode()
                if data == secret and status == 200:
                    return {"status": 200, "data": data}
                else:
                    return {"status": 500, "data": "Verification failed"}
        except Exception as e:
            self.log(f"WebSub verification error: {e}")
            return {"status": 500, "data": str(e)}
    
    def add_subscription(self, subscription_data: Dict) -> Dict:
        """Handle subscription/unsubscription requests - matches CastHubRequestHandler.handleHubSubscription"""
        hub_mode = subscription_data.get("hub.mode", subscription_data.get("hub_mode", "subscribe"))
        hub_topic = subscription_data.get("hub.topic", subscription_data.get("hub_topic", ""))
        hub_events = subscription_data.get("hub.events", subscription_data.get("hub_events", ""))
        hub_callback = subscription_data.get("hub.callback", subscription_data.get("hub_callback", ""))
        hub_secret = subscription_data.get("hub.secret", subscription_data.get("hub_secret", ""))
        hub_lease = subscription_data.get("hub.lease_seconds", subscription_data.get("hub.lease", subscription_data.get("hub_lease", "7200")))
        subscriber_name = subscription_data.get("subscriber.name", subscription_data.get("subscriber_name", "unknown"))
        subscriber_product = subscription_data.get("subscriber.product.name")
        subscriber_version = subscription_data.get("subscriber.product.version")
        raw_sub_actors = subscription_data.get("subscriber.actors")
        raw_client_info = subscription_data.get("subscriber.client_info")
        subscriber_actors = []
        if raw_sub_actors is not None:
            try:
                parsed_sub_actors = (
                    json.loads(raw_sub_actors)
                    if isinstance(raw_sub_actors, str)
                    else raw_sub_actors
                )
                if isinstance(parsed_sub_actors, list):
                    subscriber_actors = [
                        str(x).strip() for x in parsed_sub_actors if str(x).strip()
                    ]
                elif isinstance(parsed_sub_actors, str):
                    s = parsed_sub_actors.strip()
                    if s:
                        subscriber_actors = [s]
            except (json.JSONDecodeError, TypeError):
                s = str(raw_sub_actors).strip()
                if s:
                    subscriber_actors = [s]
        client_info = None
        if raw_client_info is not None:
            try:
                parsed_client_info = (
                    json.loads(raw_client_info)
                    if isinstance(raw_client_info, str)
                    else raw_client_info
                )
                if isinstance(parsed_client_info, dict):
                    cleaned_client_info = {}
                    for key in (
                        "productName",
                        "version",
                        "userAgent",
                        "platform",
                        "language",
                        "timezone",
                    ):
                        value = parsed_client_info.get(key)
                        if value is not None:
                            value_str = str(value).strip()
                            if value_str:
                                cleaned_client_info[key] = value_str
                    if cleaned_client_info:
                        client_info = cleaned_client_info
            except (json.JSONDecodeError, TypeError, ValueError):
                # Optional metadata: ignore malformed values silently.
                client_info = None
        subscriber_product = str(subscriber_product).strip() if subscriber_product is not None else ""
        subscriber_version = str(subscriber_version).strip() if subscriber_version is not None else ""
        if subscriber_product or subscriber_version:
            if not client_info:
                client_info = {}
            # Prefer explicit subscriber.product.name/version over optional client_info.
            if subscriber_product:
                client_info["productName"] = subscriber_product
            if subscriber_version:
                client_info["version"] = subscriber_version
        channel_type = subscription_data.get("hub.channel.type", subscription_data.get("hub_channel_type", "websub"))
        channel_endpoint = subscription_data.get("hub.channel.endpoint", subscription_data.get("hub_channel_endpoint", ""))
        host = subscription_data.get("host", subscription_data.get("Host", ""))

        if hub_mode == "subscribe":
            # Verify subscription request for WebSub
            if channel_type != "websocket":
                verify_result = self.check_subscription_request({
                    "hub.callback": hub_callback,
                    "hub.secret": hub_secret,
                    "hub.topic": hub_topic
                })
                
                if verify_result["status"] != 200:
                    self.log(f"WebSub verification failed: {verify_result['status']}")
                    raise ValueError("WebSub verification failed")
            
            # Generate WebSocket endpoint identifier
            websocket_endpoint = str(uuid.uuid4())
            
            # Determine protocol and host from request
            # Use wss for HTTPS (Azure), ws for HTTP (local)
            # Extract host from request headers or use provided host
            request_host = host if host else f"localhost:{self.server_port}"
            
            # Determine if HTTPS based on common patterns
            is_secure = (
                "azurewebsites.net" in request_host or 
                "https" in request_host or
                request_host.startswith("secure.") or
                not "localhost" in request_host.lower()
            )
            protocol = "wss" if is_secure else "ws"
            
            websocket_url = f"{protocol}://{request_host}/bind/{websocket_endpoint}"
            
            subscription = {
                "channel": channel_type,
                "endpoint": websocket_url if channel_type == "websocket" else hub_callback,
                "websocket_endpoint": websocket_endpoint,
                "callback": hub_callback,
                "events": hub_events,
                "secret": hub_secret,
                "topic": hub_topic,
                "lease": int(hub_lease),
                "session": hub_topic,
                "subscriber": subscriber_name,
                "actors": subscriber_actors,
                "client_info": client_info,
                "host": host,
                "created": datetime.now().isoformat()
            }
            
            self.subscriptions.append(subscription)
            self.log(
                _subscription_log_message(
                    "added",
                    subscriber_name=subscriber_name,
                    hub_topic=hub_topic,
                    channel_type=channel_type,
                    hub_events=hub_events,
                    subscriber_actors=subscriber_actors,
                    subscriber_product=subscriber_product,
                    subscriber_version=subscriber_version,
                    websocket_endpoint=websocket_endpoint,
                )
            )

            return {
                "subscription": subscription,
                "websocket_url": websocket_url if channel_type == "websocket" else None
            }
        
        elif hub_mode == "unsubscribe":
            # Handle unsubscribe
            endpoint_id = (
                channel_endpoint.split("/bind/")[-1]
                if channel_endpoint and "/bind/" in channel_endpoint
                else None
            )
            removed_count = self.remove_subscription(
                endpoint=endpoint_id,
                callback=hub_callback,
                topic=hub_topic,
            )
            if removed_count == 0:
                self.log(
                    _subscription_log_message(
                        "unsubscribe (no match)",
                        subscriber_name=subscriber_name,
                        hub_topic=hub_topic,
                        channel_type=channel_type,
                        hub_events=hub_events,
                        subscriber_actors=subscriber_actors,
                        subscriber_product=subscriber_product,
                        subscriber_version=subscriber_version,
                        websocket_endpoint=endpoint_id or "",
                    )
                )
            return {"removed": removed_count}
        
        else:
            raise ValueError(f"Invalid hub.mode: {hub_mode}")
    
    def remove_subscription(self, endpoint: str = None, callback: str = None, topic: str = None) -> int:
        """Remove subscriptions matching the given criteria"""
        removed_count = 0
        for sub in self.subscriptions[:]:
            matched = False
            if endpoint and sub.get("websocket_endpoint") == endpoint:
                matched = True
            elif (
                callback
                and sub.get("callback") == callback
                and topic
                and sub.get("topic") == topic
            ):
                matched = True
            if not matched:
                continue
            client_info = sub.get("client_info") if isinstance(sub.get("client_info"), dict) else {}
            self.log(
                _subscription_log_message(
                    "removed",
                    subscriber_name=str(sub.get("subscriber") or "unknown"),
                    hub_topic=str(sub.get("topic") or ""),
                    channel_type=str(sub.get("channel") or ""),
                    hub_events=sub.get("events"),
                    subscriber_actors=sub.get("actors") or [],
                    subscriber_product=str(client_info.get("productName") or ""),
                    subscriber_version=str(client_info.get("version") or ""),
                    websocket_endpoint=str(sub.get("websocket_endpoint") or ""),
                )
            )
            self.subscriptions.remove(sub)
            removed_count += 1

        if removed_count > 0:
            self.log(
                f"Subscription cleanup: removed={removed_count} "
                f"remaining={len(self.subscriptions)}"
            )
        return removed_count
    
    def get_subscriptions(self) -> List[Dict]:
        """Get all active subscriptions"""
        return self.subscriptions
    
    def send_event(self, topic: str, event_type: str, event_data: Dict):
        """Helper method to create a notification (actual sending happens in async endpoint)"""
        notification = {
            "timestamp": datetime.now().isoformat(),
            "id": str(uuid.uuid4()),
            "event": {
                "hub.topic": topic,
                "hub.event": event_type,
                "context": event_data
            }
        }
        return notification
    
    def register_websocket(self, endpoint: str, websocket: WebSocket):
        """Register a WebSocket connection"""
        self.websocket_connections[endpoint] = websocket
        self.log(f"WebSocket registered for endpoint: {endpoint} (total: {len(self.websocket_connections)})")
    
    def unregister_websocket(self, endpoint: str):
        """Unregister a WebSocket connection"""
        if endpoint in self.websocket_connections:
            del self.websocket_connections[endpoint]
            self.remove_subscription(endpoint=endpoint)
            self.log(f"WebSocket unregistered for endpoint: {endpoint} (remaining: {len(self.websocket_connections)})")
    
    def register_admin_websocket(self, websocket: WebSocket, location: str = "unknown"):
        """Register an admin WebSocket connection with location info"""
        # Check if this websocket is already registered
        for admin_client in self.admin_websockets:
            if admin_client["websocket"] == websocket:
                return  # Already registered
        
        admin_client = {
            "websocket": websocket,
            "location": location,
            "connected_at": datetime.now().isoformat()
        }
        self.admin_websockets.append(admin_client)
        self.log(
            f"Admin WebSocket registered from {location} (total: {len(self.admin_websockets)})",
            level=logging.DEBUG,
        )
    
    def unregister_admin_websocket(self, websocket: WebSocket):
        """Unregister an admin WebSocket connection"""
        for admin_client in self.admin_websockets[:]:
            if admin_client["websocket"] == websocket:
                location = admin_client.get("location", "unknown")
                self.admin_websockets.remove(admin_client)
                self.log(
                    f"Admin WebSocket unregistered from {location} (remaining: {len(self.admin_websockets)})",
                    level=logging.DEBUG,
                )
                return
    
    async def _do_send_admin_refresh(self):
        """Internal method to actually send the refresh command"""
        if not self.admin_websockets:
            return
        
        self.last_admin_refresh_time = time.time()
        self.admin_revision += 1

        message = {
            "type": "admin.refresh",
            "revision": self.admin_revision,
            "timestamp": datetime.now().isoformat(),
        }
        
        disconnected = []
        for admin_client in self.admin_websockets:
            try:
                await admin_client["websocket"].send_json(message)
            except Exception as e:
                self.log(f"Error sending refresh to admin: {e}")
                disconnected.append(admin_client["websocket"])
        
        # Clean up disconnected websockets
        for ws in disconnected:
            self.unregister_admin_websocket(ws)
    
    async def send_admin_refresh_command(self):
        """Send refresh command to all connected admin clients (rate limited to max 1 per 2 seconds)
        If rate limited, schedules a delayed send. Only the last suppressed refresh will be sent.
        """
        if not self.admin_websockets:
            return
        
        current_time = time.time()
        time_since_last = current_time - self.last_admin_refresh_time
        
        # If enough time has passed (>= 2 seconds), send immediately
        if time_since_last >= 2.0:
            # Cancel any pending task since we're sending now
            if self.pending_admin_refresh_task and not self.pending_admin_refresh_task.done():
                self.pending_admin_refresh_task.cancel()
                self.pending_admin_refresh_task = None
            
            await self._do_send_admin_refresh()
        else:
            # Rate limited - cancel any existing pending task and schedule a new one
            # This ensures only the last suppressed refresh is sent
            if self.pending_admin_refresh_task and not self.pending_admin_refresh_task.done():
                self.pending_admin_refresh_task.cancel()
            
            # Schedule to send after remaining time (2.0 - time_since_last)
            delay = 2.0 - time_since_last
            
            async def delayed_send():
                await asyncio.sleep(delay)
                await self._do_send_admin_refresh()
                self.pending_admin_refresh_task = None
            
            self.pending_admin_refresh_task = asyncio.create_task(delayed_send())

    async def cancel_pending_admin_refresh(self) -> None:
        """Cancel a scheduled admin refresh (e.g. before hub reset)."""
        task = self.pending_admin_refresh_task
        if task is None or task.done():
            self.pending_admin_refresh_task = None
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        self.pending_admin_refresh_task = None
    
    def add_audit_log(
        self,
        subscriber: str,
        topic: str,
        event_name: str,
        event_data: Dict,
        direction: str = "received",
        publisher: Optional[Dict[str, Optional[str]]] = None,
    ):
        """Add an entry to the audit log.

        ``subscriber`` identifies the subscription row (delivery target for
        ``direction='sent'``, or the requester for cast-request rows).
        ``publisher`` describes who published the event (name, product).
        """
        self.audit_log_counter += 1
        log_entry: Dict[str, Any] = {
            "message_number": self.audit_log_counter,
            "timestamp": datetime.now().isoformat(),
            "subscriber": (subscriber or "").strip() or "unknown",
            "topic": topic,
            "event_name": event_name,
            "event_data": event_data,
            "direction": direction,
        }
        if publisher:
            log_entry["publisher"] = publisher
        self.audit_log.append(log_entry)
        if len(self.audit_log) > 1000:
            self.audit_log = self.audit_log[-1000:]

    def get_audit_log(
        self,
        publisher_filter: Optional[str] = None,
        topic_filter: Optional[str] = None,
        event_filter: Optional[str] = None,
    ) -> List[Dict]:
        """Return audit log entries, optionally filtered by publisher, topic, or event."""
        filtered_log = self.audit_log
        if publisher_filter:
            key = publisher_filter.lower()
            filtered_log = [
                entry
                for entry in filtered_log
                if key in _audit_publisher_name(entry).lower()
            ]
        if topic_filter:
            filtered_log = [
                entry
                for entry in filtered_log
                if topic_filter.lower() in entry.get("topic", "").lower()
            ]
        if event_filter:
            filtered_log = [
                entry
                for entry in filtered_log
                if event_filter.lower() in entry.get("event_name", "").lower()
            ]
        return list(reversed(filtered_log))

    def get_audit_log_unique_values(self) -> Dict[str, List[str]]:
        """Unique publishers, topics, and events from the audit log."""
        publishers: Set[str] = set()
        topics: Set[str] = set()
        events: Set[str] = set()
        for entry in self.audit_log:
            name = _audit_publisher_name(entry)
            if name:
                publishers.add(name)
            topic = entry.get("topic")
            event_name = entry.get("event_name")
            if topic and str(topic).strip():
                topics.add(str(topic).strip())
            if event_name and str(event_name).strip():
                events.add(str(event_name).strip())
        return {
            "publishers": sorted(publishers),
            "topics": sorted(topics),
            "events": sorted(events),
        }

    async def reset_all(self):
        """Reset everything - clear subscriptions, conferences, and audit log (like restarting the service)"""
        await self.cancel_pending_admin_refresh()

        for record in self.pending_requests.values():
            completion = record.get("completion")
            if completion is not None:
                completion.set()

        disconnected_endpoints = []
        for endpoint, websocket in list(self.websocket_connections.items()):
            try:
                await websocket.close()
                self.log(f"WebSocket closed for endpoint: {endpoint}")
            except Exception as e:
                self.log(f"Error closing WebSocket for endpoint {endpoint}: {e}")
            disconnected_endpoints.append(endpoint)

        for admin_client in list(self.admin_websockets):
            try:
                await admin_client["websocket"].close()
                self.log("Admin WebSocket closed", level=logging.DEBUG)
            except Exception as e:
                self.log(f"Error closing admin WebSocket: {e}", level=logging.WARNING)

        self.subscriptions.clear()
        self.websocket_connections.clear()
        self.admin_websockets.clear()
        self.conferences.clear()
        self.audit_log.clear()
        self.audit_log_counter = 0
        self.user_count = 0
        self.pending_requests.clear()
        self.auth_codes.clear()
        self.admin_revision = 0
        self.last_admin_refresh_time = 0.0

        self.log(
            f"Hub reset - all subscriptions, conferences, audit log, and auth codes cleared; "
            f"{len(disconnected_endpoints)} subscriber WebSocket(s) disconnected"
        )


# Global Cast Hub instance
cast_hub = CastHub()
_configure_cast_hub_logging()

@app.middleware("http")
async def cast_hub_request_logging_middleware(request: Request, call_next):
    """Log selected hub HTTP requests without buffering response bodies.

    Uses the plain ``@app.middleware("http")`` pattern instead of
    ``BaseHTTPMiddleware``, which tees entire response bodies through
    memory streams and throttles large ``/api/hub/payloads/`` downloads.
    """
    path = request.url.path
    method = request.method

    should_log = False
    if path.startswith("/api/hub") and method in ("GET", "POST", "DELETE"):
        if not hub_path_excluded_from_request_log(path):
            should_log = True
    elif path in ("/oauth/token", "/oauth/authorize") and method == "POST":
        should_log = True

    if not should_log:
        return await call_next(request)

    client_host = request.client.host if request.client else "unknown"
    client_port = request.client.port if request.client else "unknown"
    client_addr = f"{client_host}:{client_port}"
    response = await call_next(request)
    status_code = response.status_code
    log_message = f'INFO:     {client_addr} - "{method} {path} HTTP/1.1" {status_code}'
    cast_hub.log(log_message)
    return response


async def _parse_request_body(request: Request) -> dict:
    """Parse request body as JSON or form data, with fallback to raw body parsing."""
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        return await request.json()
    try:
        form_data = await request.form()
        merged = {}
        for key in form_data.keys():
            values = form_data.getlist(key)
            merged[key] = values[0] if len(values) == 1 else values
        return merged
    except AssertionError as e:
        if "python-multipart" in str(e):
            body = await request.body()
            if body:
                parsed = parse_qs(body.decode())
                return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
            return {}
        raise


def _serve_html_page(filename: str):
    """Serve an HTML page from cast_api/Resources/, fallback to static mount."""
    html_path = os.path.join(resources_dir, filename)
    if os.path.exists(html_path):
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        return Response(content=html_content, media_type="text/html")
    return RedirectResponse(url=f"/static/{filename}", status_code=302)


@app.get("/api/hub/conference-topics")
@app.get("/api/hub/conference-topics/")
async def get_conference_topics():
    """Topics from active subscriptions (conference-client attendee picker)."""
    topics = sorted(
        t
        for t in {
            sub.get("topic")
            for sub in cast_hub.get_subscriptions()
            if sub.get("topic")
        }
        if str(t).strip() != "*"
    )
    return topics


@app.get("/api/hub/conference")
async def get_conference():
    """Get all conferences"""
    return cast_hub.conferences


@app.post("/api/hub/conference")
async def post_conference(request: Request):
    """Create a conference"""
    try:
        data = await _parse_request_body(request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse request: {e}")
    
    conference = {
        "user": data.get("user"),
        "title": data.get("title"),
        "topics": data.get("topics", [])
    }
    cast_hub.conferences.append(conference)
    cast_hub.log(f"Conference created: {conference.get('title')}")

    # Send conference-start to all participants' WebSockets (title + subscriber names)
    conference_user = conference.get("user")
    attendee_topics = conference.get("topics", [])
    all_participant_topics = [conference_user] + attendee_topics
    subscriber_names = []
    sent_endpoints = set()
    for participant_topic in all_participant_topics:
        for sub in cast_hub.subscriptions:
            if sub.get("topic") == participant_topic and sub.get("channel") == "websocket":
                name = sub.get("subscriber", "unknown")
                if name not in subscriber_names:
                    subscriber_names.append(name)
                break
    # Cast-style message: timestamp, id, event with hub.topic, hub.event, context
    notification = {
        "timestamp": datetime.now().isoformat(),
        "id": str(uuid.uuid4()),
        "event": {
            "hub.topic": conference_user or "",
            "hub.event": "conference-start",
            "context": {
                "title": conference.get("title") or "",
                "participants": subscriber_names,
            },
        },
    }
    message_json = json.dumps(notification)
    for participant_topic in all_participant_topics:
        for sub in cast_hub.subscriptions:
            if sub.get("topic") == participant_topic and sub.get("channel") == "websocket":
                endpoint = sub.get("websocket_endpoint")
                if endpoint and endpoint in cast_hub.websocket_connections and endpoint not in sent_endpoints:
                    try:
                        ws = cast_hub.websocket_connections[endpoint]
                        await ws.send_text(message_json)
                        sent_endpoints.add(endpoint)
                        cast_hub.log(f"Sent conference-start to participant: {sub.get('subscriber')}")
                    except Exception as e:
                        cast_hub.log(f"Conference-start WebSocket error: {e}")

    # Send admin refresh command (rate limited)
    await cast_hub.send_admin_refresh_command()
    
    return {"status": "created"}


@app.delete("/api/hub/conference")
async def delete_conference(request: Request):
    """Delete a conference"""
    try:
        data = await _parse_request_body(request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse request: {e}")
    
    user = data.get("user")
    removed = []
    for conf in cast_hub.conferences[:]:
        if conf.get("user") == user:
            cast_hub.conferences.remove(conf)
            removed.append(conf)
        elif user in conf.get("topics", []):
            cast_hub.log(f"User {user} exited conference {conf.get('title')}")
    
    # Send admin refresh command if conferences were removed (rate limited)
    if len(removed) > 0:
        await cast_hub.send_admin_refresh_command()
    
    return {"removed": len(removed)}


@app.get("/api/hub/conference-client")
@app.get("/api/hub/conference-client/")
async def get_conference_client():
    """Get conference client page"""
    return _serve_html_page("conference-client.html")


@app.get("/api/hub/admin")
@app.get("/api/hub/admin/")
async def get_hub_status():
    """Get hub status page showing all users and endpoints"""
    return _serve_html_page("admin.html")


@app.get("/api/hub/admin/metrics")
async def get_admin_metrics():
    """Lightweight process metrics for admin sparkline charts."""
    return collect_hub_metrics(cast_hub, _http_payload_store)


@app.get("/api/hub/admin/snapshot")
async def get_admin_snapshot(
    publisher: Optional[str] = None,
    topic: Optional[str] = None,
    event: Optional[str] = None,
):
    """Consolidated admin dashboard payload (subscriptions, conferences, audit log)."""
    subscriptions = cast_hub.get_subscriptions()
    log_entries = cast_hub.get_audit_log(
        publisher_filter=publisher, topic_filter=topic, event_filter=event
    )
    unique_values = cast_hub.get_audit_log_unique_values()
    conferences = list(cast_hub.conferences)

    return {
        "revision": cast_hub.admin_revision,
        "single_user_mode": cast_hub.single_user_mode,
        "stats": {
            "total_subscriptions": len(subscriptions),
            "total_authentications": cast_hub.user_count,
            "total_topics": len(
                set(sub.get("topic") for sub in subscriptions if sub.get("topic"))
            ),
            "total_messages": len(cast_hub.audit_log),
            "total_conferences": len(conferences),
            "total_admin_clients": len(cast_hub.admin_websockets),
        },
        "subscriptions": subscriptions,
        "conferences": conferences,
        "audit": {
            "entries": log_entries,
            "count": len(log_entries),
            "unique_publishers": unique_values["publishers"],
            "unique_topics": unique_values["topics"],
            "unique_events": unique_values["events"],
        },
    }


@app.post("/api/admin/reset")
async def reset_hub(request: Request):
    """Reset the hub - clear all subscriptions, conferences, and audit log (like restarting the service)"""
    single_user_mode = False
    try:
        data = await _parse_request_body(request)
        single_user_mode = data.get("single_user_mode", False)
        if isinstance(single_user_mode, str):
            single_user_mode = single_user_mode.lower() == "true"
    except Exception:
        pass

    cast_hub.single_user_mode = single_user_mode

    await cast_hub.reset_all()

    mode_msg = " (Single-user mode enabled)" if single_user_mode else ""
    return {
        "status": "reset",
        "message": (
            "All subscriptions, conferences, audit log cleared, "
            f"and all WebSocket connections disconnected{mode_msg}"
        ),
    }


@app.get("/images/3DSlicer-DesktopIcon.png")
async def get_slicer_icon():
    """Serve the desktop icon path expected by existing HTML pages."""
    icon_dir = os.path.join(
        os.path.dirname(base_dir), "Resources", "Icons"
    )
    logo_path = os.path.join(icon_dir, "CastInterfaceLogo.png")
    if not os.path.exists(logo_path):
        logo_path = os.path.join(resources_dir, "images", "3DSlicer-DesktopIcon.png")
    if os.path.exists(logo_path):
        return FileResponse(logo_path, media_type="image/png")
    raise HTTPException(status_code=404, detail="Icon not found")


@app.get("/favicon.ico")
async def get_favicon(theme: Optional[str] = None):
    """Serve favicon; default 3D Slicer icon (?theme=volview for VolView)."""
    use_volview = (theme or "").strip().lower() == "volview"
    if not use_volview:
        slicer_path = os.path.join(
            resources_dir, "images", "3DSlicer-DesktopIcon.png"
        )
        if os.path.exists(slicer_path):
            return FileResponse(slicer_path, media_type="image/png")
    logo_path = os.path.join(resources_dir, "images", "volview-logo.svg")
    if os.path.exists(logo_path):
        return FileResponse(logo_path, media_type="image/svg+xml")
    raise HTTPException(status_code=404, detail="Favicon not found")


@app.post("/api/hub/request")
@app.post("/api/hub/request/")
async def post_cast_request(request: Request):
    """Dispatch a typed request to all matching subscribers and return collated responses.

    Request body must include ``event.hub.event`` (a ``*-request`` name). Topic filtering uses
    ``event.hub.topic``. Optional ``event.context.dataType`` is forwarded on the WebSocket fan-out.

    Matches subscriptions by ``(topic, target.actor[, target.product.name])`` (``target.actor``
    ``*`` or omitted = all roles on topic). Sends the client's ``hub.event`` to each connected
    match and waits up to ``CAST_REQUEST_TIMEOUT_SECONDS`` for the matching
    ``<datatype>-response`` events to arrive on the bind WebSocket.
    """
    try:
        request_data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not isinstance(request_data, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")

    subscriber = str(request_data.get("subscriber.name", "")).strip()
    request_event_name = _request_hub_event_from_body(request_data)
    topic_param = _request_topic_from_body(request_data)
    data_type = _request_context_data_type(request_data, request_event_name) or ""
    subscriber_actor_value = request_data.get("subscriber.actor")
    requested_actor = _actor_to_text(subscriber_actor_value)
    filter_actor = _target_actor_filter_from_request_body(request_data)
    requested_product_name = _target_product_name_from_payload(request_data)
    product_filter_active = _target_product_filter_active(requested_product_name)

    if not subscriber:
        raise HTTPException(
            status_code=400, detail="Missing 'subscriber.name' parameter"
        )

    if not request_event_name or not is_request_event(request_event_name):
        raise HTTPException(
            status_code=400,
            detail="event.hub.event must be a *-request event name",
        )

    request_id = str(request_data.get("id", "")).strip()
    if not request_id:
        raise HTTPException(
            status_code=400, detail="Missing 'id' on cast request body"
        )

    # Find requester subscriptions first (caller identity; topic only, not hub.events).
    requester_matches = []
    for sub in cast_hub.get_subscriptions():
        sub_name = sub.get("subscriber", "").strip()
        if sub_name != subscriber:
            continue
        if topic_param and not _subscription_topic_matches(
            sub.get("topic", ""), topic_param
        ):
            continue
        requester_matches.append(sub)

    subscriber_exists = bool(requester_matches)
    subscriber_connected = False
    for sub in requester_matches:
        websocket_endpoint = sub.get("websocket_endpoint")
        if websocket_endpoint and websocket_endpoint in cast_hub.websocket_connections:
            subscriber_connected = True
            break

    # Find ALL connected dispatch targets (topic, hub.events, actor[, product]) — same
    # eligibility as publish; subscriber topic ``*`` matches any request topic.
    target_subscriptions: List[Dict] = []
    target_exists = False
    target_any_match = False  # any subscription matched the (topic, actor[, product]) filter
    for sub in cast_hub.get_subscriptions():
        if topic_param and not _subscription_topic_matches(
            sub.get("topic", ""), topic_param
        ):
            continue
        subscribed_events = (sub.get("events") or "").lower()
        request_et = request_event_name.lower()
        if request_et not in subscribed_events and "*" not in subscribed_events:
            continue
        if filter_actor and filter_actor not in _subscription_actor_list(sub):
            continue
        if product_filter_active and not _subscription_accepts_target_product(
            sub, requested_product_name
        ):
            continue
        target_any_match = True
        websocket_endpoint = sub.get("websocket_endpoint")
        if websocket_endpoint and websocket_endpoint in cast_hub.websocket_connections:
            target_subscriptions.append(sub)
            target_exists = True

    target_connected = bool(target_subscriptions)

    audit_status = "not-dispatched"
    audit_error: Optional[str] = None
    audit_topic = topic_param
    responses_collected: List[Dict[str, Any]] = []
    expected_subscribers: List[str] = []
    missing_subscribers: List[str] = []
    timed_out = False
    dispatch_topics: Set[str] = set()
    dispatched_count = 0

    if not subscriber_exists:
        audit_status = "requester-not-found"
        cast_hub.log(
            f"Cast request: requester subscriber '{subscriber}' not found"
            + (f" for topic '{topic_param}'" if topic_param else "")
        )
    elif not subscriber_connected:
        audit_status = "requester-not-connected"
        cast_hub.log(
            f"Cast request: requester subscriber '{subscriber}' is not currently websocket-connected"
        )
    elif not target_any_match:
        audit_status = "target-not-found"
        cast_hub.log(
            "Cast request: no subscription matched "
            f"event='{request_event_name}', target.actor='{filter_actor or '*'}'"
            + (f", topic='{topic_param}'" if topic_param else "")
            + (
                f", target.product.name='{requested_product_name}'"
                if product_filter_active
                else ""
            )
        )
    elif not target_connected:
        audit_status = "target-not-connected"
        cast_hub.log(
            "Cast request: matched targets exist but none are currently websocket-connected"
        )

    if target_connected:
        # Build the rich pending-request record now so the WS handler can find it.
        subscriber_products: Dict[str, str] = {}
        expected_set: Set[str] = set()
        for sub in target_subscriptions:
            name = sub.get("subscriber", "").strip()
            if not name:
                continue
            expected_set.add(name)
            subscriber_products[name] = _subscription_product_name(sub)

        if topic_param:
            dispatch_topics.add(topic_param)

        completion_event = asyncio.Event()
        # Edge case: every match shared an empty subscriber name -> fall through
        # as if no targets matched.
        if not expected_set:
            audit_status = "target-not-found"
            cast_hub.log("Cast request: matched targets had no subscriber names; aborting")
            target_connected = False
        else:
            cast_hub.pending_requests[request_id] = {
                "actor": requested_actor or None,
                "target_actor": filter_actor or None,
                "product_name": requested_product_name or None,
                "data_type": data_type or None,
                "event_name": request_event_name,
                "expected": set(expected_set),
                "subscriber_products": subscriber_products,
                "responses": responses_collected,
                "seen_envelope_ids": set(),
                "completion": completion_event,
                "lock": asyncio.Lock(),
                "topic": topic_param,
                "started_at": datetime.now().isoformat(),
            }

            audit_topic = next(iter(dispatch_topics), topic_param)

            cast_hub.log(
                "Cast request dispatch: "
                f"requester='{subscriber}', "
                f"targets={sorted(expected_set)}, "
                f"topic='{audit_topic}', "
                f"dataType='{data_type or ''}', "
                f"subscriber.actor='{requested_actor or ''}', "
                f"target.actor='{filter_actor or '*'}', "
                f"target.product.name='{requested_product_name or ''}', "
                f"event='{request_event_name}', "
                f"id='{request_id}'"
            )
            cast_hub.add_audit_log(
                subscriber=subscriber,
                topic=audit_topic,
                event_name=request_event_name,
                event_data={
                    "id": request_id,
                    "status": "dispatched",
                    "dataType": data_type or None,
                    "requestedActor": requested_actor or None,
                    "requestedTargetActor": filter_actor or None,
                    "requestedProductName": requested_product_name or None,
                    "expectedCount": len(expected_set),
                    "targetSubscribers": sorted(expected_set),
                },
                direction="sent",
                publisher=_publisher_for_cast_request(subscriber, requested_product_name),
            )

            try:
                send_failures: List[str] = []
                for sub in target_subscriptions:
                    name = sub.get("subscriber", "").strip()
                    websocket_endpoint = sub.get("websocket_endpoint")
                    websocket = cast_hub.websocket_connections.get(websocket_endpoint)
                    if not websocket:
                        send_failures.append(name)
                        continue
                    dispatch_topic = topic_param or sub.get("topic", "").strip()
                    req_event = _cast_request_event_dict(request_data)
                    req_ctx = req_event.get("context")
                    fan_out_context: Dict[str, Any] = (
                        dict(req_ctx) if isinstance(req_ctx, dict) else {}
                    )
                    fan_out_context["id"] = request_id
                    if data_type:
                        fan_out_context["dataType"] = data_type
                    notification = {
                        "timestamp": datetime.now().isoformat(),
                        "id": str(uuid.uuid4()),
                        "event": {
                            "hub.topic": dispatch_topic,
                            "hub.event": request_event_name,
                            "context": fan_out_context,
                        },
                    }
                    if requested_actor:
                        notification["subscriber.actor"] = requested_actor
                    if filter_actor:
                        notification["target.actor"] = filter_actor
                    try:
                        await websocket.send_text(json.dumps(notification))
                        dispatched_count += 1
                    except Exception as send_err:
                        cast_hub.log(
                            f"Cast request send failed for subscriber '{name}': "
                            f"{type(send_err).__name__}: {send_err}"
                        )
                        send_failures.append(name)

                # If any sends failed, drop those subscribers from the expected set
                # so we don't wait for responses they'll never produce.
                async with cast_hub.pending_requests[request_id]["lock"]:
                    for failed_name in send_failures:
                        cast_hub.pending_requests[request_id]["expected"].discard(failed_name)
                    if not cast_hub.pending_requests[request_id]["expected"]:
                        completion_event.set()

                if dispatched_count == 0:
                    audit_status = "send-error"
                    audit_error = "Failed to send to all matched targets"
                else:
                    try:
                        await asyncio.wait_for(
                            completion_event.wait(),
                            timeout=CAST_REQUEST_TIMEOUT_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        timed_out = True

                    record = cast_hub.pending_requests.get(request_id, {}) or {}
                    async with record.get("lock") or asyncio.Lock():
                        responses_collected = list(record.get("responses") or [])
                        missing_subscribers = sorted(record.get("expected") or set())
                    expected_subscribers = sorted(expected_set)

                    if timed_out:
                        audit_status = "timeout"
                        audit_error = (
                            f"Timeout after {CAST_REQUEST_TIMEOUT_SECONDS}s waiting for responses"
                        )
                        cast_hub.log(
                            "Cast request timeout: "
                            f"requester='{subscriber}', id='{request_id}', "
                            f"received={len(responses_collected)}/{len(expected_subscribers)}, "
                            f"missing={missing_subscribers}"
                        )
                    elif responses_collected:
                        audit_status = "response-received"
                    else:
                        audit_status = "no-response"

                    response_preview = ""
                    try:
                        response_preview = json.dumps(responses_collected)[:600]
                    except Exception:
                        response_preview = str(responses_collected)[:600]
                    cast_hub.log(
                        "Cast request collated: "
                        f"requester='{subscriber}', "
                        f"id='{request_id}', "
                        f"count={len(responses_collected)}, "
                        f"timedOut={timed_out}, "
                        f"missing={missing_subscribers}, "
                        f"responses={response_preview[:200]}"
                    )
            except Exception as e:
                audit_status = "send-error"
                audit_error = f"{type(e).__name__}: {e}"
                cast_hub.log(
                    "Cast request dispatch error: "
                    f"requester='{subscriber}', id='{request_id}', "
                    f"{type(e).__name__}: {e}"
                )

    if not expected_subscribers:
        expected_subscribers = sorted({sub.get("subscriber", "").strip() for sub in target_subscriptions if sub.get("subscriber")})

    if request_id:
        record = cast_hub.pending_requests.get(request_id)
        if record:
            async with record.get("lock") or asyncio.Lock():
                responses_collected = list(record.get("responses") or [])
                missing_subscribers = sorted(record.get("expected") or set())
            if responses_collected and audit_status == "no-response":
                audit_status = "response-received"

    cast_hub.add_audit_log(
        subscriber=subscriber,
        topic=audit_topic or topic_param,
        event_name=request_event_name,
        event_data={
            "id": request_id,
            "status": audit_status,
            "dataType": data_type or None,
            "requestedActor": requested_actor or None,
            "requestedTargetActor": filter_actor or None,
            "requestedProductName": requested_product_name or None,
            "expectedCount": len(expected_subscribers),
            "actualCount": len(responses_collected),
            "expectedSubscribers": expected_subscribers,
            "missingSubscribers": missing_subscribers,
            "timedOut": timed_out,
            "error": audit_error,
            "responses": responses_collected,
        },
        direction="received",
        publisher=_publisher_for_cast_request(subscriber, requested_product_name),
    )
    if request_id:
        # Drop pending state only after the audit row is written so a response
        # arriving between collation and logging cannot be lost from the log.
        cast_hub.pending_requests.pop(request_id, None)
    await cast_hub.send_admin_refresh_command()

    out: Dict[str, Any] = {
        "ok": audit_status in ("response-received", "no-response"),
        "id": request_id,
        "subscriber.name": subscriber,
        "dataType": data_type if data_type else None,
        "subscriber.actor": requested_actor or None,
        "target.actor": filter_actor or None,
        "exists": subscriber_exists,
        "connected": subscriber_connected,
        "topic": (next(iter(dispatch_topics)) if dispatch_topics else None),
        "endpoint": None,
        "targetExists": target_exists,
        "targetConnected": target_connected,
        "responses": responses_collected,
        "expected": expected_subscribers,
        "missing": missing_subscribers,
        "timedOut": timed_out,
    }
    if topic_param:
        out["requestedTopic"] = topic_param
    if requested_product_name:
        out["target.product.name"] = requested_product_name
    if audit_error:
        out["error"] = audit_error
    return out


def _http_exception_for_filename_policy(exc: FilenamePolicyError) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={
            "message": str(exc),
            "code": exc.code,
            "fileName": exc.file_name,
        },
    )


def _enforce_binary_transfer_filenames(
    notification: dict,
    *,
    require_name: bool = False,
) -> None:
    """Reject publishes whose transfer filenames fail hub allowlist policy."""
    event = notification.get("event") or {}
    if not is_cast_binary_event(event.get("hub.event")):
        return
    try:
        enforce_transfer_filenames_for_notification(
            notification, require_name=require_name
        )
    except FilenamePolicyError as exc:
        cast_hub.log(
            f"filename policy rejected publish: code={exc.code} "
            f"file={exc.file_name or '(none)'} detail={exc}"
        )
        raise _http_exception_for_filename_policy(exc) from exc


def _notification_has_embedded_resource_bytes(notification: dict) -> bool:
    """True when a JSON publish carries inline ``context.files[].data`` bytes."""
    for entry in _context_files_from_notification(notification):
        data = entry.get("data")
        if data is None:
            continue
        if isinstance(data, str):
            if data.strip():
                return True
            continue
        if isinstance(data, (bytes, bytearray, memoryview)):
            if len(data) > 0:
                return True
            continue
        return True
    return False


def _reject_json_binary_bytes_publish(notification: dict) -> None:
    """Binary-family publishes with file bytes must use multipart/related STOW."""
    event = notification.get("event") or {}
    if not is_cast_binary_event(event.get("hub.event")):
        return
    if _notification_has_embedded_resource_bytes(notification):
        raise HTTPException(
            status_code=400,
            detail=(
                "binary-family publish with file bytes requires "
                "multipart/related STOW (JSON + context.files[] parts)"
            ),
        )


def _context_files_from_notification(notification: dict) -> List[dict]:
    """Return ``context.files[]`` entries from a STOW-style batch publish."""
    event = notification.get("event") or {}
    ctx = event.get("context")
    if not isinstance(ctx, dict):
        return []
    files = ctx.get("files")
    if not isinstance(files, list):
        return []
    return [entry for entry in files if isinstance(entry, dict)]


def _rewrite_files_with_payload_ids(
    notification: dict,
    blobs: List[bytes],
    registrations: List[Optional[Tuple[str, datetime]]],
) -> str:
    """Build WS text JSON with hub-added ``payloadId`` on each ``context.files[]`` entry."""
    n2 = copy.deepcopy(notification)
    ev2 = n2.get("event") or {}
    ctx = ev2.get("context")
    if not isinstance(ctx, dict):
        return json.dumps(n2)
    files = ctx.get("files")
    if not isinstance(files, list):
        return json.dumps(n2)
    for idx, entry in enumerate(files):
        if not isinstance(entry, dict):
            continue
        raw = blobs[idx] if idx < len(blobs) else b""
        stripped = copy.deepcopy(entry)
        stripped.pop("data", None)
        stripped.pop("binaryTransfer", None)
        stripped.pop("url", None)
        stripped["byteLength"] = len(raw)
        registered = registrations[idx] if idx < len(registrations) else None
        if registered is not None:
            payload_id, expires_at = registered
            stripped["payloadId"] = payload_id
            stripped["expiresAt"] = expires_at.isoformat()
        else:
            stripped.pop("payloadId", None)
            stripped.pop("expiresAt", None)
        files[idx] = stripped
    return json.dumps(n2)


def _prepare_stow_batch_fanout(
    notification: dict,
    blobs: List[bytes],
) -> str:
    """Store each DICOM part and fan out one message with ``context.files[]`` payloadIds."""
    files = _context_files_from_notification(notification)
    if len(files) != len(blobs):
        raise HTTPException(
            status_code=400,
            detail=(
                f"STOW batch: context.files length {len(files)} "
                f"does not match DICOM part count {len(blobs)}"
            ),
        )

    _enforce_binary_transfer_filenames(notification, require_name=True)

    registrations: List[Optional[Tuple[str, datetime]]] = []
    stored_count = 0
    for idx, raw in enumerate(blobs):
        entry = files[idx]
        file_name = str(entry.get("fileName") or "").strip()
        mime_type = str(entry.get("mimeType") or "application/dicom").strip()
        registered = _http_payload_store.register(
            raw,
            file_name=file_name,
            mime_type=mime_type or "application/dicom",
            ttl_seconds=CAST_HUB_HTTP_PAYLOAD_TTL_SECONDS,
            max_total_bytes=CAST_HUB_HTTP_PAYLOAD_MAX_TOTAL_BYTES,
        )
        registrations.append(registered)
        if registered is not None:
            stored_count += 1
            payload_id, _expires_at = registered
            cast_hub.log(
                f"Stored STOW payload payloadId={payload_id[:8]}... "
                f"bytes={len(raw)} index={idx} "
                f"file={file_name or '(unnamed)'}"
            )

    if stored_count == len(blobs):
        return _rewrite_files_with_payload_ids(notification, blobs, registrations)

    cast_hub.log(
        "STOW payload store partially unavailable "
        f"(stored={stored_count}/{len(blobs)} "
        f"inflight={_http_payload_store.total_bytes()}B "
        f"cap={CAST_HUB_HTTP_PAYLOAD_MAX_TOTAL_BYTES}B), "
        "fanning out metadata-only for this publish"
    )
    return _fanout_metadata_only_binary(notification)


def _prepare_websocket_fanout_text(
    notification: dict,
    notification_json: str,
    predecoded_binary_list: Optional[List[bytes]] = None,
) -> str:
    """
    WebSocket fan-out is text-only. STOW batch publishes store each file and
    rewrite ``context.files[].payloadId``; metadata-only binary-family events
    fan out without a payloadId.
    """
    event = notification.get("event") or {}
    if not is_cast_binary_event(event.get("hub.event")):
        return notification_json

    if predecoded_binary_list is not None:
        return _prepare_stow_batch_fanout(notification, predecoded_binary_list)

    return _fanout_metadata_only_binary(notification)


def _fanout_metadata_only_binary(notification: dict) -> str:
    """Return JSON envelope with transport fields stripped from resources/files."""
    n2 = copy.deepcopy(notification)
    ev2 = n2.get("event") or {}
    ctx = ev2.get("context")
    if isinstance(ctx, dict):
        files = ctx.get("files")
        if isinstance(files, list):
            cleaned_files: List[dict] = []
            for entry in files:
                if not isinstance(entry, dict):
                    cleaned_files.append(entry)
                    continue
                cleaned = dict(entry)
                cleaned.pop("data", None)
                cleaned.pop("binaryTransfer", None)
                cleaned.pop("url", None)
                cleaned.pop("payloadId", None)
                cleaned.pop("expiresAt", None)
                cleaned_files.append(cleaned)
            ctx["files"] = cleaned_files
        res = ctx.get("resource")
        if isinstance(res, dict):
            cleaned = dict(res)
            cleaned.pop("data", None)
            cleaned.pop("binaryTransfer", None)
            cleaned.pop("url", None)
            cleaned.pop("payloadId", None)
            cleaned.pop("expiresAt", None)
            ctx["resource"] = cleaned
    elif isinstance(ctx, list):
        for item in ctx:
            if not isinstance(item, dict):
                continue
            res = item.get("resource")
            if not isinstance(res, dict):
                continue
            cleaned = dict(res)
            cleaned.pop("data", None)
            cleaned.pop("binaryTransfer", None)
            cleaned.pop("url", None)
            cleaned.pop("payloadId", None)
            cleaned.pop("expiresAt", None)
            item["resource"] = cleaned
    return json.dumps(n2)


def _iter_http_payload_chunks(
    token: str,
    raw: bytes,
    file_name: str,
    chunk_size: int,
):
    """Yield payload bytes in large chunks; log real wire time after last chunk."""
    started_at = time.monotonic()
    nbytes = len(raw)
    offset = 0
    while offset < nbytes:
        end = min(offset + chunk_size, nbytes)
        yield raw[offset:end]
        offset = end
    elapsed = max(time.monotonic() - started_at, 0.0)
    throughput = (
        f"{(nbytes / (1024 * 1024)) / elapsed:.2f}"
        if elapsed > 0
        else "n/a"
    )
    cast_hub.log(
        f"Served http payload token={token[:8]}... "
        f"bytes={nbytes} elapsed={elapsed:.2f}s "
        f"throughput={throughput} MB/s file={file_name or '(unnamed)'}"
    )


@app.get("/api/hub/payloads/{token}")
async def get_hub_payload(token: str):
    """Serve a stored http binaryTransfer payload by token."""
    entry = _http_payload_store.get(token)
    if entry is None:
        raise HTTPException(status_code=404, detail="payload not found or expired")
    headers = {
        "Content-Length": str(entry.byte_length),
        "Cache-Control": "no-store",
    }
    if entry.file_name:
        # RFC 5987 fallback would be nicer for non-ASCII but the existing
        # nifti-send filenames are ASCII in practice.
        headers["Content-Disposition"] = (
            f'attachment; filename="{entry.file_name}"'
        )
    return StreamingResponse(
        _iter_http_payload_chunks(
            token,
            entry.raw,
            entry.file_name or "",
            CAST_HUB_HTTP_PAYLOAD_SEND_CHUNK_BYTES,
        ),
        media_type=entry.mime_type or "application/octet-stream",
        headers=headers,
    )


async def _http_payload_reaper() -> None:
    """Background task: evict expired http payload entries on a fixed cadence."""
    sleep_seconds = max(
        5.0, min(30.0, CAST_HUB_HTTP_PAYLOAD_TTL_SECONDS / 4.0)
    )
    while True:
        try:
            await asyncio.sleep(sleep_seconds)
            evicted = _http_payload_store.evict_expired()
            if evicted:
                cast_hub.log(
                    f"http payload reaper evicted {evicted} entry/entries "
                    f"(inflight={_http_payload_store.total_bytes()}B, "
                    f"count={_http_payload_store.count()})"
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            cast_hub.log(f"http payload reaper error: {exc}")


async def _read_multipart_part_bytes(part: Any) -> bytes:
    if part is None:
        return b""
    read = getattr(part, "read", None)
    if callable(read):
        data = read()
        if asyncio.iscoroutine(data):
            data = await data
        return bytes(data or b"")
    if isinstance(part, (bytes, bytearray)):
        return bytes(part)
    return str(part).encode("utf-8")


def _parse_multipart_related_parts(
    body: bytes, content_type: str
) -> List[Tuple[str, bytes]]:
    """Parse ``multipart/related`` body into ``(content_type, payload)`` pairs."""
    if not body:
        return []
    header = (
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n"
    ).encode("ascii", errors="replace")
    msg = BytesParser(policy=policy.default).parsebytes(header + body)
    parts: List[Tuple[str, bytes]] = []
    for part in msg.iter_parts():
        ct = part.get_content_type() or "application/octet-stream"
        payload = part.get_payload(decode=True)
        if payload is None:
            payload = b""
        elif isinstance(payload, str):
            payload = payload.encode("utf-8")
        else:
            payload = bytes(payload)
        parts.append((ct, payload))
    return parts


async def _parse_stow_batch_publish(request: Request) -> tuple:
    """Parse STOW-shaped ``multipart/related`` batch (JSON + N file parts)."""
    content_type = request.headers.get("content-type", "")
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="multipart/related body is empty")

    parts = _parse_multipart_related_parts(body, content_type)
    if not parts:
        raise HTTPException(status_code=400, detail="multipart/related has no parts")

    json_ct, json_bytes = parts[0]
    json_ct_lower = json_ct.lower()
    if "json" not in json_ct_lower:
        raise HTTPException(
            status_code=400,
            detail=(
                "STOW batch first part must be JSON "
                f"(application/dicom+json); got {json_ct!r}"
            ),
        )
    if not json_bytes:
        raise HTTPException(status_code=400, detail="STOW batch JSON part is empty")
    try:
        notification = json.loads(json_bytes.decode("utf-8"))
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail=f"STOW batch JSON part is not valid JSON: {exc}"
        ) from exc

    if not isinstance(notification, dict):
        raise HTTPException(status_code=400, detail="STOW batch JSON must be an object")

    event = notification.get("event")
    if not isinstance(event, dict):
        raise HTTPException(status_code=400, detail="STOW batch JSON missing event object")
    if not is_cast_binary_event(event.get("hub.event")):
        raise HTTPException(
            status_code=400,
            detail=(
                "STOW batch publish is only supported for binary-family events "
                "(hub.event starting with dicom, nifti, jpg, png, or nrrd)"
            ),
        )

    files_meta = _context_files_from_notification(notification)
    if not files_meta:
        raise HTTPException(
            status_code=400,
            detail="STOW batch publish requires non-empty event.context.files[]",
        )

    file_parts = parts[1:]
    if len(file_parts) != len(files_meta):
        raise HTTPException(
            status_code=400,
            detail=(
                f"STOW batch expects {len(files_meta)} file part(s) after JSON, "
                f"got {len(file_parts)}"
            ),
        )

    allowed_part_types = (
        "application/dicom",
        "application/octet-stream",
        "application/vnd.unknown.nifti-1",
    )
    blobs: List[bytes] = []
    for idx, (part_ct, raw) in enumerate(file_parts):
        if not raw:
            raise HTTPException(
                status_code=400, detail=f"STOW batch file part {idx} is empty"
            )
        part_ct_lower = part_ct.lower().split(";")[0].strip()
        entry = files_meta[idx]
        expected_mime = str(entry.get("mimeType") or "application/octet-stream").strip().lower()
        if expected_mime:
            expected_mime = expected_mime.split(";")[0].strip()
        if part_ct_lower not in allowed_part_types and (
            not expected_mime or part_ct_lower != expected_mime
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"STOW batch file part {idx} has unsupported Content-Type "
                    f"{part_ct!r} (expected {expected_mime or 'application/dicom'})"
                ),
            )
        expected = entry.get("byteLength")
        if isinstance(expected, int) and expected >= 0 and expected != len(raw):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"STOW batch file {idx} size {len(raw)} does not match "
                    f"context.files[{idx}].byteLength {expected}"
                ),
            )
        blobs.append(raw)

    _enforce_binary_transfer_filenames(notification, require_name=True)
    return notification, blobs


def _optional_str_field(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _publisher_from_notification(notification: dict) -> Optional[Dict[str, Optional[str]]]:
    """Publisher identity from a publish notification (envelope subscriber fields)."""
    name = ""
    for key in ("subscriber.name", "subscriber", "publisher"):
        v = notification.get(key)
        if v is not None and str(v).strip():
            name = str(v).strip()
            break
    product = _optional_str_field(notification.get("subscriber.product.name"))
    if not name and not product:
        return None
    return {"name": name or None, "product": product}


def _publisher_name(publisher: Optional[Dict[str, Optional[str]]]) -> str:
    if not publisher or not isinstance(publisher, dict):
        return ""
    return (publisher.get("name") or "").strip()


def _publisher_for_cast_request(
    subscriber: str, product_name: Optional[str] = None
) -> Dict[str, Optional[str]]:
    product = _optional_str_field(product_name)
    if product == "*":
        product = None
    return {
        "name": (subscriber or "").strip() or None,
        "product": product,
    }


def _audit_publisher_name(entry: Dict) -> str:
    """Display/filter name for a log row's publisher (supports legacy ``user`` rows)."""
    pub = entry.get("publisher")
    if isinstance(pub, dict):
        name = (pub.get("name") or "").strip()
        if name:
            return name
        product = (pub.get("product") or "").strip()
        if product:
            return product
    legacy = (entry.get("subscriber") or entry.get("user") or "").strip()
    return legacy


async def _handle_publish_notification(
    notification: dict,
    predecoded_binary_list: Optional[List[bytes]] = None,
):
    """Handle publish notification payload and broadcast to subscribers."""
    notification.pop("subscriber.product.version", None)
    message_id = notification.get("id", "unknown")
    event = notification.get("event", {})
    event_type = event.get("hub.event", "unknown")
    topic_name = event.get("hub.topic", "").strip()

    if not topic_name:
        raise HTTPException(status_code=400, detail="Missing event.hub.topic for publish request")

    cast_hub.log(f"Received message ID: {message_id} for topic {topic_name}, event: {event_type}")

    # Broadcast to subscribers (sync method, but WebSocket sending needs async)
    context = event.get("context", {})

    # Full JSON for HMAC + WebSub; WebSocket uses payloadId rewrite when bytes stored
    notification_json = json.dumps(notification)
    ws_text = _prepare_websocket_fanout_text(
        notification,
        notification_json,
        predecoded_binary_list=predecoded_binary_list,
    )

    def _audit_context():
        if is_cast_binary_event(event_type) and ws_text != notification_json:
            try:
                return json.loads(ws_text).get("event", {}).get("context", context)
            except Exception:
                return {"hub.event": event_type, "payloadId": "(stored)"}
        return context

    audit_ctx = _audit_context()
    publisher = _publisher_from_notification(notification)
    publisher_subscriber = _publisher_name(publisher)

    cast_hub.add_audit_log(
        subscriber=publisher_subscriber or topic_name,
        topic=topic_name,
        event_name=event_type,
        event_data=audit_ctx,
        direction="received",
        publisher=publisher,
    )

    # Track endpoints that have already received the message to prevent duplicates
    sent_endpoints = set()
    
    # Send to matching subscriptions
    publish_target_actor = _target_actor_from_payload(notification)
    publish_target_product = _target_product_name_from_payload(notification)

    for sub in cast_hub.subscriptions[:]:  # Copy to allow removal
        if not _subscription_handles_event(sub, topic_name, event_type):
            continue
        if not _subscription_accepts_target_actor(sub, publish_target_actor):
            continue
        if not _subscription_accepts_target_product(sub, publish_target_product):
            continue

        secret = sub.get("secret", "")
        channel = sub.get("channel", "websub")
        
        # Calculate HMAC
        hmac_sig = ""
        if secret:
            hmac_sig = hmac.new(secret.encode(), notification_json.encode(), hashlib.sha256).hexdigest()
        
        if channel == "websocket":
            sub_name = (sub.get("subscriber") or "").strip()
            if (
                publisher_subscriber
                and sub_name.lower() == publisher_subscriber.lower()
            ):
                continue
            # WebSocket delivery - async
            endpoint = sub.get("websocket_endpoint")
            if endpoint and endpoint in cast_hub.websocket_connections:
                try:
                    websocket = cast_hub.websocket_connections[endpoint]
                    await websocket.send_text(ws_text)
                    if (
                        is_cast_binary_event(event_type)
                        and ws_text != notification_json
                    ):
                        cast_hub.log(
                            f"Sent WebSocket message to {sub.get('subscriber')} "
                            f"via endpoint {endpoint} mode=payloadId "
                            f"event={event_type}"
                        )
                    else:
                        cast_hub.log(
                            f"Sent WebSocket message to {sub.get('subscriber')} "
                            f"via endpoint {endpoint}"
                        )
                    # Track this endpoint as having received the message
                    sent_endpoints.add(endpoint)
                    # Log sent message
                    cast_hub.add_audit_log(
                        subscriber=sub.get("subscriber", "unknown"),
                        topic=topic_name,
                        event_name=event_type,
                        event_data=audit_ctx,
                        direction="sent",
                        publisher=publisher,
                    )
                except Exception as e:
                    cast_hub.log(f"WebSocket send error for {endpoint}: {type(e).__name__}: {e}")
                    cast_hub.log(f"Removing failed WebSocket connection and subscription")
                    if endpoint in cast_hub.websocket_connections:
                        del cast_hub.websocket_connections[endpoint]
                    if sub in cast_hub.subscriptions:
                        cast_hub.subscriptions.remove(sub)
            else:
                if not endpoint:
                    cast_hub.log(f"WebSocket endpoint not set for subscription: {sub.get('subscriber')}")
                else:
                    cast_hub.log(f"WebSocket not bound for subscription: {sub.get('subscriber')}")
        else:
            sub_name = (sub.get("subscriber") or "").strip()
            if (
                publisher_subscriber
                and sub_name.lower() == publisher_subscriber.lower()
            ):
                continue
            # WebSub delivery - HTTP POST to callback (can be async but using sync for now)
            callback = sub.get("callback")
            if callback:
                try:
                    req = urllib.request.Request(callback)
                    req.add_header("Content-Type", "application/json")
                    req.add_header("X-Hub-Signature", f"sha256={hmac_sig}")
                    req.data = notification_json.encode()
                    req.get_method = lambda: "POST"
                    
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, lambda: urllib.request.urlopen(req, timeout=5))
                    cast_hub.log(f"Sent WebSub notification to {callback}")
                    # Log sent message
                    cast_hub.add_audit_log(
                        subscriber=sub.get("subscriber", "unknown"),
                        topic=topic_name,
                        event_name=event_type,
                        event_data=audit_ctx,
                        direction="sent",
                        publisher=publisher,
                    )
                except Exception as e:
                    cast_hub.log(f"WebSub delivery error to {callback}: {e}")
    
    # Handle conferences - broadcast to attendees (skip if already sent)
    for conference in cast_hub.conferences:
        conference_user = conference.get("user")
        attendee_topics = conference.get("topics", [])
        
        # Check if message is from any conference participant (host or attendee)
        is_participant = (conference_user == topic_name) or (topic_name in attendee_topics)
        
        if is_participant:
            # Send to all participants (host + all attendees)
            all_participants = [conference_user] + attendee_topics
            
            for participant_topic in all_participants:
                # Find subscriptions for participant
                for sub in cast_hub.subscriptions:
                    if sub.get("topic") == participant_topic and sub.get("channel") == "websocket":
                        sub_name = (sub.get("subscriber") or "").strip()
                        if (
                            publisher_subscriber
                            and sub_name.lower() == publisher_subscriber.lower()
                        ):
                            continue
                        endpoint = sub.get("websocket_endpoint")
                        if endpoint and endpoint in cast_hub.websocket_connections:
                            # Skip if already sent to this endpoint
                            if endpoint in sent_endpoints:
                                continue
                            try:
                                websocket = cast_hub.websocket_connections[endpoint]
                                await websocket.send_text(ws_text)
                                cast_hub.log(f"Sent conference message to participant: {participant_topic}")
                                # Track this endpoint as having received the message
                                sent_endpoints.add(endpoint)
                                # Log sent conference message
                                cast_hub.add_audit_log(
                                    subscriber=sub.get("subscriber", "unknown"),
                                    topic=participant_topic,
                                    event_name=event_type,
                                    event_data=audit_ctx,
                                    direction="sent",
                                    publisher=publisher,
                                )
                            except Exception as e:
                                cast_hub.log(f"Conference WebSocket error: {e}")

    return {"status": "received"}


async def _handle_multipart_publish(request: Request):
    """Route multipart/related STOW batch publish."""
    content_type = request.headers.get("content-type", "")
    ct_lower = content_type.lower()
    if "multipart/related" not in ct_lower:
        raise HTTPException(
            status_code=400,
            detail=(
                "binary publish requires multipart/related STOW "
                "(application/dicom+json manifest + file parts)"
            ),
        )
    notification, blobs = await _parse_stow_batch_publish(request)
    return await _handle_publish_notification(
        notification,
        predecoded_binary_list=blobs,
    )


@app.post("/api/hub/")
@app.post("/api/hub")
async def post_hub(request: Request):
    """Handle subscribe/unsubscribe and publish requests."""
    content_type = request.headers.get("content-type", "")
    if "multipart" in content_type.lower():
        try:
            return await _handle_multipart_publish(request)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        finally:
            await cast_hub.send_admin_refresh_command()

    try:
        request_data = await _parse_request_body(request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse request: {e}")
    
    # Also get query parameters
    query_params = dict(request.query_params)
    request_data.update(query_params)
    
    # Add host header for WebSocket URL generation
    request_data["host"] = request.headers.get("host", request.headers.get("Host", ""))
    
    try:
        hub_mode = request_data.get("hub.mode", request_data.get("hub_mode", "")).strip().lower()
        if hub_mode == "subscribe" or hub_mode == "unsubscribe":
            subscription_data = request_data
            if hub_mode == "unsubscribe":
                # Handle unsubscribe
                result = cast_hub.add_subscription(subscription_data)
                return {"status": "unsubscribed", "removed": result.get("removed", 0)}
            # Handle subscribe
            result = cast_hub.add_subscription(subscription_data)
            
            # Return appropriate response - 202 Accepted for subscription requests
            if result.get("websocket_url"):
                return JSONResponse(
                    content={"hub.channel.endpoint": result["websocket_url"]},
                    status_code=202
                )
            return JSONResponse(
                content={"status": "subscribed", "subscription": result["subscription"]},
                status_code=202
            )

        # Publish via /api/hub or /api/hub/
        event = request_data.get("event")
        if isinstance(event, dict):
            _reject_json_binary_bytes_publish(request_data)
            return await _handle_publish_notification(request_data)

        raise HTTPException(
            status_code=400,
            detail="Invalid /api/hub POST payload: expected hub.mode for subscribe/unsubscribe or event payload for publish"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        # Send admin refresh command (rate limited)
        await cast_hub.send_admin_refresh_command()


@app.delete("/api/hub/")
@app.delete("/api/hub")
async def delete_hub(request: Request):
    """Handle DELETE /api/hub/ - clear all subscriptions"""
    try:
        unsubscribe_data = await _parse_request_body(request)
    except Exception:
        unsubscribe_data = {}
    
    # If unsubscribe data provided, remove specific subscriptions
    if unsubscribe_data:
        endpoint = unsubscribe_data.get("hub.channel.endpoint") or unsubscribe_data.get("hub_channel_endpoint")
        callback = unsubscribe_data.get("hub.callback") or unsubscribe_data.get("hub_callback")
        topic = unsubscribe_data.get("hub.topic") or unsubscribe_data.get("hub_topic")
        
        if endpoint or (callback and topic):
            removed_count = cast_hub.remove_subscription(
                endpoint=endpoint.split("/bind/")[-1] if endpoint and "/bind/" in endpoint else None,
                callback=callback,
                topic=topic
            )
            await cast_hub.send_admin_refresh_command()
            return {"status": "unsubscribed", "removed": removed_count}
    
    # Otherwise, clear all subscriptions
    cast_hub.subscriptions.clear()
    cast_hub.log("All subscriptions cleared")
    await cast_hub.send_admin_refresh_command()
    return {"status": "cleared"}


@app.websocket("/bind/{endpoint}")
async def websocket_endpoint(websocket: WebSocket, endpoint: str):
    """WebSocket endpoint for event delivery"""
    await websocket.accept()
    cast_hub.log(f"WebSocket connection accepted for endpoint: {endpoint}")
    
    # Register WebSocket connection
    cast_hub.register_websocket(endpoint, websocket)
    
    # Send initial connection confirmation
    try:
        await websocket.send_json({
            "type": "connection.established",
            "endpoint": endpoint,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        cast_hub.log(f"Error sending connection confirmation: {e}")
    
    # Send admin refresh on connection
    await cast_hub.send_admin_refresh_command()
    
    keepalive_task = None
    if CAST_HUB_WS_KEEPALIVE:
        interval = max(1.0, CAST_HUB_WS_KEEPALIVE_INTERVAL_SECONDS)

        async def keepalive():
            while True:
                try:
                    await asyncio.sleep(interval)
                    await websocket.send_json({
                        "type": "ping",
                        "timestamp": datetime.now().isoformat(),
                    })
                except Exception as e:
                    cast_hub.log(f"Keepalive error for {endpoint}: {e}")
                    break

        keepalive_task = asyncio.create_task(keepalive())

    try:
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                msg_type = (
                    message.get("type") if isinstance(message, dict) else None
                )
                if msg_type in ("ping", "pong"):
                    if CAST_HUB_WS_KEEPALIVE:
                        summary = _summarize_websocket_message(message)
                        cast_hub.log(
                            f"WebSocket {endpoint}: {summary}",
                            level=logging.DEBUG,
                        )
                    continue

                summary = _summarize_websocket_message(message)
                cast_hub.log(f"WebSocket {endpoint}: {summary}")
                
                # Check if this is a response to a pending request (in event format).
                # hub.event values ending with ``-response`` are treated as Cast responses.
                event = message.get("event", {}) if isinstance(message, dict) else {}
                hub_event_name = event.get("hub.event") if isinstance(event, dict) else None
                if isinstance(hub_event_name, str) and is_response_event(hub_event_name):
                    context = event.get("context", {}) if isinstance(event, dict) else {}
                    request_id = context.get("id") if isinstance(context, dict) else None
                    record = (
                        cast_hub.pending_requests.get(request_id) if isinstance(request_id, str) else None
                    )
                    if record is None:
                        cast_hub.log(
                            f"Received {hub_event_name} for unknown id {request_id}"
                        )
                    else:
                        envelope_id = (
                            str(message.get("id")).strip() if message.get("id") is not None else ""
                        )
                        async with record["lock"]:
                            if envelope_id and envelope_id in record["seen_envelope_ids"]:
                                cast_hub.log(
                                    f"Ignoring duplicate {hub_event_name} envelope id={envelope_id} "
                                    f"for id {request_id}"
                                )
                            else:
                                if envelope_id:
                                    record["seen_envelope_ids"].add(envelope_id)
                                identity = _response_identity_from_ws_message(
                                    message, endpoint=endpoint
                                )
                                responder_name = identity["subscriber"]
                                actor_text = identity["actor"] or (
                                    record.get("actor") or ""
                                )
                                product_name = (
                                    record["subscriber_products"].get(responder_name)
                                    or None
                                )
                                response_data = (
                                    context.get("data") if isinstance(context, dict) else None
                                )
                                record["responses"].append(
                                    {
                                        "id": envelope_id or None,
                                        "subscriber": responder_name or None,
                                        "actor": actor_text or record.get("actor") or None,
                                        "productName": product_name,
                                        "data": response_data,
                                    }
                                )
                                if responder_name:
                                    record["expected"].discard(responder_name)
                                if not record["expected"]:
                                    record["completion"].set()
                                cast_hub.log(
                                    f"Routed {hub_event_name} for id {request_id} "
                                    f"from subscriber='{responder_name}' "
                                    f"({len(record['responses'])} response(s) so far, "
                                    f"{len(record['expected'])} still expected)"
                                )
            except json.JSONDecodeError:
                preview = data if len(data) <= 120 else data[:120] + "..."
                cast_hub.log(f"WebSocket {endpoint}: non-JSON ({len(data)} bytes): {preview}")
    except WebSocketDisconnect:
        cast_hub.log(f"WebSocket disconnected for endpoint: {endpoint}")
    except Exception as e:
        cast_hub.log(f"WebSocket error for endpoint {endpoint}: {type(e).__name__}: {e}")
        # Send admin refresh on error
        await cast_hub.send_admin_refresh_command()
    finally:
        if keepalive_task is not None:
            keepalive_task.cancel()
            try:
                await keepalive_task
            except asyncio.CancelledError:
                pass

        # Unregister WebSocket connection
        cast_hub.unregister_websocket(endpoint)
        cast_hub.log(f"WebSocket cleanup completed for endpoint: {endpoint}")
        
        # Send admin refresh on disconnect
        await cast_hub.send_admin_refresh_command()


@app.websocket("/ws/admin")
async def admin_websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for admin page - receives refresh commands"""
    await websocket.accept()
    
    # Extract location information from request headers
    location = "unknown"
    try:
        # Try to get client IP and other location info
        client_host = websocket.client.host if websocket.client else "unknown"
        client_port = websocket.client.port if websocket.client else "unknown"
        location = f"{client_host}:{client_port}"
    except Exception as e:
        cast_hub.log(f"Could not extract location info: {e}", level=logging.DEBUG)
        location = "unknown"
    
    cast_hub.log(
        f"Admin WebSocket connection accepted from {location}",
        level=logging.DEBUG,
    )
    
    # Register admin WebSocket with location
    cast_hub.register_admin_websocket(websocket, location)
    
    # Send initial connection confirmation
    try:
        await websocket.send_json({
            "type": "connection.established",
            "role": "admin",
            "revision": cast_hub.admin_revision,
            "timestamp": datetime.now().isoformat(),
        })
    except Exception as e:
        cast_hub.log(
            f"Error sending admin connection confirmation: {e}",
            level=logging.WARNING,
        )
    
    try:
        while True:
            # Receive messages from admin client (pong, etc.)
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                summary = _summarize_websocket_message(message)
                cast_hub.log(
                    f"Admin WebSocket {location}: {summary}",
                    level=logging.DEBUG,
                )
            except json.JSONDecodeError:
                preview = data if len(data) <= 120 else data[:120] + "..."
                cast_hub.log(
                    f"Admin WebSocket {location}: non-JSON ({len(data)} bytes): {preview}",
                    level=logging.DEBUG,
                )
    except WebSocketDisconnect:
        cast_hub.log("Admin WebSocket disconnected", level=logging.DEBUG)
    except Exception as e:
        cast_hub.log(
            f"Admin WebSocket error: {type(e).__name__}: {e}",
            level=logging.WARNING,
        )
    finally:
        # Unregister admin WebSocket
        cast_hub.unregister_admin_websocket(websocket)
        cast_hub.log("Admin WebSocket cleanup completed", level=logging.DEBUG)


@app.options("/api/hub/")
@app.options("/api/hub")
async def options_hub():
    """Handle CORS preflight requests"""
    return Response(status_code=204)


# -----------------------------------------------------------------------------
# OAuth (dev/mock — may be extracted to a separate module later)
# -----------------------------------------------------------------------------

JWT_ALGORITHM = "HS256"
JWT_DEFAULT_SECRET = os.environ.get("CAST_HUB_JWT_SECRET", "cast-hub-dev-secret-change-me")
JWT_ISSUER = os.environ.get("CAST_HUB_JWT_ISSUER", "cast-hub")
JWT_AUDIENCE = os.environ.get("CAST_HUB_JWT_AUDIENCE", "cast-clients")
JWT_EXPIRY_SECONDS = int(os.environ.get("CAST_HUB_JWT_EXPIRY_SECONDS", "3600"))


def _b64url_encode(data: bytes) -> str:
    """Base64URL without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _build_hs256_jwt(claims: Dict[str, Any], secret: str) -> str:
    """Build a compact HS256 JWT using only stdlib."""
    header = {"alg": JWT_ALGORITHM, "typ": "JWT"}
    header_b64 = _b64url_encode(
        json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    payload_b64 = _b64url_encode(
        json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    sig_b64 = _b64url_encode(signature)
    return f"{header_b64}.{payload_b64}.{sig_b64}"


def _b64url_decode(data: str) -> bytes:
    """Base64URL decode tolerating missing padding."""
    pad = (-len(data)) % 4
    return base64.urlsafe_b64decode(data + ("=" * pad))


def _verify_hs256_jwt(
    token: str, secret: str, *, allow_expired: bool = False
) -> Dict[str, Any]:
    """Validate signature; return claims.

    Lenient on iss/aud (mock context). When allow_expired=True, expired tokens
    still return claims (caller decides what to do). Raises ValueError on
    malformed JWT, signature mismatch, or expired-token-when-not-allowed.
    """
    if not isinstance(token, str) or token.count(".") != 2:
        raise ValueError("Malformed JWT")
    header_b64, payload_b64, sig_b64 = token.split(".")
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    expected = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    try:
        actual = _b64url_decode(sig_b64)
    except Exception as exc:
        raise ValueError("Invalid signature encoding") from exc
    if not hmac.compare_digest(expected, actual):
        raise ValueError("Signature mismatch")
    try:
        claims = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except Exception as exc:
        raise ValueError("Invalid JWT payload") from exc
    if not isinstance(claims, dict):
        raise ValueError("Invalid JWT claims")
    exp = claims.get("exp")
    if isinstance(exp, (int, float)) and exp < time.time() and not allow_expired:
        raise ValueError("Token expired")
    return claims


def _issue_jwt_pair(
    *,
    token_type: str,
    count: int,
    topic: str,
    user_name: str,
) -> str:
    """Issue signed JWT token."""
    now = int(time.time())
    claims = {
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "iat": now,
        "nbf": now,
        "exp": now + JWT_EXPIRY_SECONDS,
        "jti": str(uuid.uuid4()),
        "typ": token_type,
        "token_counter": count,
        "topic": topic,
        "user_name": user_name,
        "scope": "openid",
    }
    return _build_hs256_jwt(claims, JWT_DEFAULT_SECRET)


_AUTH_CODE_TTL_SECONDS = 60


def _resolve_authorize_identity(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """Resolve user_name / topic for /oauth/authorize.

    Precedence: id_token (claims) > user_name (asserted) > server-generated.
    Always honours single_user_mode and an explicit body 'topic' override.
    Subscriber names are chosen by clients on subscribe, not OAuth.
    Returns { user_name, topic, count }.
    """
    user_name: Optional[str] = None
    topic: Optional[str] = None

    id_token = request_data.get("id_token")
    if isinstance(id_token, str) and id_token.strip():
        try:
            claims = _verify_hs256_jwt(
                id_token.strip(), JWT_DEFAULT_SECRET, allow_expired=True
            )
        except ValueError as exc:
            # Map signature/format failures to OAuth invalid_token (401).
            raise HTTPException(
                status_code=401,
                detail={"error": "invalid_token", "error_description": str(exc)},
            ) from exc
        user_name = claims.get("user_name") or None
        topic = claims.get("topic") or None

    asserted_user = request_data.get("user_name")
    if not user_name and isinstance(asserted_user, str) and asserted_user.strip():
        user_name = asserted_user.strip()

    if user_name is None:
        # Server-generated identity (mirrors legacy /oauth/token fallback).
        cast_hub.user_count += 1
        count = cast_hub.user_count
        user_name = f"USER-{count}"
        topic = topic or user_name
    else:
        cast_hub.user_count += 1
        count = cast_hub.user_count
        topic = topic or user_name

    if cast_hub.single_user_mode:
        user_name = "SINGLE-USER"
        topic = "SINGLE-USER"

    body_topic = request_data.get("topic")
    if isinstance(body_topic, str) and body_topic.strip() and not cast_hub.single_user_mode:
        topic = body_topic.strip()

    return {
        "user_name": user_name,
        "topic": topic,
        "count": count,
    }


def _purge_expired_auth_codes() -> None:
    """Drop expired/used codes to keep the in-memory store bounded."""
    now = time.time()
    expired = [
        code for code, info in cast_hub.auth_codes.items()
        if info.get("used") or info.get("expires_at", 0) <= now
    ]
    for code in expired:
        cast_hub.auth_codes.pop(code, None)


@app.post("/oauth/authorize")
async def post_oauth_authorize(request: Request):
    """Mock OAuth 2.0 authorization endpoint.

    Accepts a previously-issued id_token (expired or not), a caller-asserted
    user_name, or nothing (server generates USER-N). Returns a one-time code
    bound to the resolved identity. Not for production.

    Note: real OAuth /authorize is GET + browser redirect. This is a
    programmatic POST + JSON mock used by Cast clients.
    """
    try:
        request_data = await _parse_request_body(request)
    except Exception:
        request_data = {}
    request_data.update(dict(request.query_params))

    identity = _resolve_authorize_identity(request_data)

    _purge_expired_auth_codes()
    code = secrets.token_urlsafe(32)
    cast_hub.auth_codes[code] = {
        "user_name": identity["user_name"],
        "topic": identity["topic"],
        "count": identity["count"],
        "expires_at": time.time() + _AUTH_CODE_TTL_SECONDS,
        "used": False,
    }

    await cast_hub.send_admin_refresh_command()

    return {
        "user_name": identity["user_name"],
        "code": code,
        "expires_in": _AUTH_CODE_TTL_SECONDS,
    }


@app.post("/oauth/token")
async def post_oauth_token(request: Request):
    """OAuth token endpoint. Supports grant_type=authorization_code.

    Redeems a one-time code issued by POST /oauth/authorize and returns
    access_token + id_token bound to the same identity.
    """
    try:
        request_data = await _parse_request_body(request)
    except Exception:
        request_data = {}
    request_data.update(dict(request.query_params))

    grant_type = (request_data.get("grant_type") or "").strip()
    code = (request_data.get("code") or "").strip()

    if grant_type and grant_type != "authorization_code":
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_grant_type",
                "error_description": f"grant_type '{grant_type}' is not supported; use authorization_code",
            },
        )
    if not code:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_request",
                "error_description": "Missing 'code' (obtain one via POST /oauth/authorize)",
            },
        )

    info = cast_hub.auth_codes.get(code)
    if info is None or info.get("used") or info.get("expires_at", 0) <= time.time():
        raise HTTPException(
            status_code=401,
            detail={
                "error": "invalid_grant",
                "error_description": "Authorization code is missing, used, or expired",
            },
        )
    info["used"] = True

    user_name = info["user_name"]
    topic = info["topic"]
    count = info["count"]

    access_token = _issue_jwt_pair(
        token_type="access",
        count=count,
        topic=topic,
        user_name=user_name,
    )
    id_token = _issue_jwt_pair(
        token_type="id",
        count=count,
        topic=topic,
        user_name=user_name,
    )

    response = {
        "token_type": "Bearer",
        "expires_in": JWT_EXPIRY_SECONDS,
        "scope": "openid",
        "topic": topic,
        "id_token": id_token,
        "access_token": access_token,
    }
    await cast_hub.send_admin_refresh_command()
    return response


def main():
    """Main function to run the Cast Hub server"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Standalone Cast Hub Server")
    parser.add_argument("--port", type=int, default=2018, help="Server port (default: 2018)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Server host (default: 0.0.0.0)")
    args = parser.parse_args()
    
    cast_hub.set_server_port(args.port)
    
    print("=" * 60)
    print("Standalone Cast Hub Server")
    print("=" * 60)
    print(f"Server running on http://{args.host}:{args.port}")
    print(f"Hub API endpoint: http://{args.host}:{args.port}/api/hub/")
    print("")
    print("Test endpoints:")
    print(f"  GET    http://{args.host}:{args.port}/")
    print(f"  GET    http://{args.host}:{args.port}/api/hub/admin (admin status page)")
    print(f"  POST   http://{args.host}:{args.port}/api/hub/")
    print(f"  DELETE http://{args.host}:{args.port}/api/hub/")
    print(f"  GET    http://{args.host}:{args.port}/api/hub/conference-topics")
    print(f"  GET    http://{args.host}:{args.port}/api/hub/conference")
    print(f"  POST   http://{args.host}:{args.port}/api/hub/conference")
    print(f"  DELETE http://{args.host}:{args.port}/api/hub/conference")
    print(f"  GET    http://{args.host}:{args.port}/api/hub/conference-client")
    print("")
    print("OAuth (mock):")
    print(f"  POST   http://{args.host}:{args.port}/oauth/authorize")
    print(f"  POST   http://{args.host}:{args.port}/oauth/token")
    print("")
    print(f"WebSocket connections: ws://{args.host}:{args.port}/bind/<endpoint>")
    print("")
    print("Using FastAPI with built-in WebSocket support")
    print("Press Ctrl+C to stop the server")
    print("=" * 60)
    
    # Run the server
    # Note: On Windows, if Ctrl+C doesn't work:
    #   - Press Ctrl+C twice (second press forces interrupt)
    #   - Use Ctrl+Break instead
    #   - Close the terminal window
    #   - Or use: taskkill /F /PID <process_id>
    try:
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            log_level="info",
            ws_max_size=None,
            ws_ping_interval=CAST_HUB_UVICORN_WS_PING_INTERVAL,
            ws_ping_timeout=CAST_HUB_UVICORN_WS_PING_TIMEOUT,
        )
    except KeyboardInterrupt:
        print("\n[LOG] Server stopped by user")


if __name__ == "__main__":
    main()
