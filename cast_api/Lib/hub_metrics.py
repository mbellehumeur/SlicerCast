"""Process and hub connection metrics for the admin portal."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

_process: Optional[Any] = None


def _hub_process() -> Any:
    global _process
    if _process is None:
        import psutil

        _process = psutil.Process(os.getpid())
        _process.cpu_percent(interval=None)
    return _process


def collect_hub_metrics(cast_hub: Any, payload_store: Any) -> Dict[str, Any]:
    """Sample RSS, CPU, WebSocket counts, and optional TCP connections."""
    proc = _hub_process()
    mem = proc.memory_info()
    memory_rss_mb = round(mem.rss / (1024 * 1024), 2)
    cpu_percent = round(proc.cpu_percent(interval=None), 2)

    payload_bytes = 0
    if payload_store is not None and hasattr(payload_store, "total_bytes"):
        try:
            payload_bytes = int(payload_store.total_bytes())
        except (TypeError, ValueError):
            payload_bytes = 0
    memory_payload_mb = round(payload_bytes / (1024 * 1024), 2)

    ws_subscribers = len(getattr(cast_hub, "websocket_connections", {}) or {})
    ws_admin = len(getattr(cast_hub, "admin_websockets", []) or [])
    websocket_total = ws_subscribers + ws_admin

    tcp_connections: Optional[int] = None
    try:
        conns = proc.net_connections(kind="inet")
        tcp_connections = len(conns)
    except (AttributeError, OSError, PermissionError):
        tcp_connections = None

    sockets: Dict[str, Any] = {
        "websocket_total": websocket_total,
        "websocket_subscribers": ws_subscribers,
        "websocket_admin": ws_admin,
    }
    if tcp_connections is not None:
        sockets["tcp_connections"] = tcp_connections

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "memory_rss_mb": memory_rss_mb,
        "memory_payload_mb": memory_payload_mb,
        "cpu_percent": cpu_percent,
        "sockets": sockets,
    }
