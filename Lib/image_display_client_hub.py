"""Image Display Client hub connection (daemon thread + SlicerCastClient)."""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, Callable, Dict, Optional

from .cast_client import CastClientOptions, HubConfig, SessionConfig, SlicerCastClient
from .CastServiceProviders import HUBS, USER_NAME
from .service_provider_hub import format_connect_failure

LOGGER = logging.getLogger("CastInterface.ImageDisplay")
LOGGER.setLevel(logging.INFO)

DISPLAY_PRODUCT_NAME = "3DSLICER-ID"
DISPLAY_PRODUCT_VERSION = "1.0"
DISPLAY_ACTORS = ["ID"]
DISPLAY_EVENTS = ["imagingstudy-open", "imagingstudy-close"]
AUTO_RECONNECT = True

_active_connection: Optional["ImageDisplayClientConnection"] = None


def disconnect_image_display_client() -> None:
    global _active_connection
    if _active_connection is not None:
        _active_connection.disconnectHub()
        _active_connection = None


def build_image_display_client(
    hub_name: str,
    topic: str,
    subscriber_name: str,
    product_name: str,
    product_version: str,
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
        topic=topic,
        subscriber_name=subscriber_name,
        product_name=(product_name or "").strip() or DISPLAY_PRODUCT_NAME,
        product_version=(product_version or "").strip() or DISPLAY_PRODUCT_VERSION,
        actors=list(DISPLAY_ACTORS),
        events=list(DISPLAY_EVENTS),
        lease=int(hub_def["lease"]),
        user_name=USER_NAME,
        default_target_actor="ID",
    )
    options = CastClientOptions(
        auto_reconnect=AUTO_RECONNECT,
        preserve_session_topic_from_token=True,
    )
    return SlicerCastClient(hub, session, options)


class ImageDisplayClientConnection:
    """Single Image Display Client hub session."""

    def __init__(self, post_ui: Callable[[Callable[[], None]], None]) -> None:
        self._post_ui = post_ui
        self._hub_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._want_hub_unsubscribe = False
        self._hub_subscribed = False
        self._hub_live = False
        self._client: Optional[SlicerCastClient] = None
        self._status_callback: Optional[
            Callable[[str, Optional[Dict[str, Any]]], None]
        ] = None
        self._hub_name = ""
        self._topic = ""
        self._subscriber_name = ""
        self._product_name = ""
        self._product_version = ""
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._connect_failed = False
        self._message_count = 0

    def isHubThreadRunning(self) -> bool:
        return self._hub_thread is not None and self._hub_thread.is_alive()

    def isHubConnected(self) -> bool:
        return self.isHubThreadRunning() and self._hub_live

    def get_message_count(self) -> int:
        return self._message_count

    def get_subscriber_name(self) -> str:
        if self._client is None:
            return ""
        return (self._client._session_cfg.subscriber_name or "").strip()

    def connectHub(
        self,
        hub_name: str,
        topic: str,
        subscriber_name: str,
        product_name: str,
        product_version: str,
        status_callback: Callable[[str, Optional[Dict[str, Any]]], None],
    ) -> None:
        global _active_connection

        if self.isHubThreadRunning():
            LOGGER.warning("Image Display Client hub thread already running")
            return

        self._hub_name = hub_name
        self._topic = topic
        self._subscriber_name = subscriber_name
        self._product_name = product_name
        self._product_version = product_version
        self._status_callback = status_callback
        self._want_hub_unsubscribe = False
        self._hub_subscribed = False
        self._hub_live = False
        self._connect_failed = False
        self._message_count = 0
        self._stop_event.clear()

        _active_connection = self

        self._hub_thread = threading.Thread(
            target=self._hub_thread_main,
            name="CastHub-ImageDisplay",
            daemon=True,
        )
        self._hub_thread.start()

    def disconnectHub(self) -> None:
        global _active_connection

        self._want_hub_unsubscribe = True
        self._stop_event.set()
        if self._hub_thread:
            self._hub_thread.join(timeout=20.0)
            self._hub_thread = None
        self._client = None
        self._hub_subscribed = False
        self._hub_live = False
        self._want_hub_unsubscribe = False
        self._status_callback = None
        if _active_connection is self:
            _active_connection = None

    def _hub_thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._hub_async_main())
        except Exception as exc:
            if not self._connect_failed:
                reason = format_connect_failure(exc)
                LOGGER.warning(
                    "Cannot connect Image Display Client hub=%s: %s",
                    self._hub_name,
                    reason,
                )
                self._emit_status("failed", {"reason": reason})
        finally:
            self._loop = None
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.close()

    def _emit_status(
        self, state: str, detail: Optional[Dict[str, Any]] = None
    ) -> None:
        callback = self._status_callback
        if not callback:
            return

        def run() -> None:
            try:
                callback(state, detail)
            except Exception as exc:
                LOGGER.warning("status callback error: %s", exc)

        self._post_ui(run)

    async def _hub_async_main(self) -> None:
        self._client = build_image_display_client(
            self._hub_name,
            self._topic,
            self._subscriber_name,
            self._product_name,
            self._product_version,
        )
        if self._status_callback:
            initial_sub = (self._client._session_cfg.subscriber_name or "").strip()
            if initial_sub:
                self._emit_status("connecting", {"subscriber_name": initial_sub})

        def on_state(state: str, detail: Optional[Dict[str, Any]] = None) -> None:
            LOGGER.info("Image Display Client connection state: %s", state)
            if state == "connected":
                self._hub_live = True
            elif state in ("disconnected", "reconnecting", "error"):
                self._hub_live = False
            if (
                not self._hub_subscribed
                and state in ("disconnected", "error", "reconnecting")
            ):
                return
            merged = dict(detail or {})
            sub = (self._client._session_cfg.subscriber_name or "").strip()
            if sub:
                merged.setdefault("subscriber_name", sub)
            self._emit_status(state, merged or None)

        self._client.on_connection_state_change(on_state)

        try:
            if self._stop_event.is_set():
                return

            auth = await self._client.authenticate()
            LOGGER.info(
                "Image Display Client authenticated topic=%s user_name=%s",
                self._topic,
                auth.get("user_name"),
            )
            if self._stop_event.is_set():
                return

            code = auth.get("code")
            if not code:
                raise RuntimeError("authenticate did not return a code")
            if not await self._client.get_token(code):
                raise RuntimeError("token exchange failed")
            if self._stop_event.is_set():
                return

            status = await self._client.subscribe()
            if status != 202:
                if status == 0:
                    raise RuntimeError(
                        "subscribe failed (hub unreachable or network error)"
                    )
                raise RuntimeError(f"subscribe failed with HTTP {status}")
            self._hub_subscribed = True
            subscriber = (self._client._session_cfg.subscriber_name or "").strip()
            LOGGER.info(
                "Image Display Client subscribed hub=%s topic=%s subscriber=%s",
                self._hub_name,
                self._topic,
                subscriber,
            )
            self._emit_status(
                "connected",
                {"message_count": 0, "subscriber_name": subscriber},
            )

            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(
                        self._client.message_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                self._message_count += 1
                event = message.get("event") or {}
                hub_event = event.get("hub.event", "")
                LOGGER.info(
                    "Image Display Client received event=%s id=%s topic=%s",
                    hub_event,
                    message.get("id"),
                    event.get("hub.topic", ""),
                )
                self._emit_status(
                    "connected",
                    {
                        "message_count": self._message_count,
                        "subscriber_name": (
                            self._client._session_cfg.subscriber_name or ""
                        ).strip(),
                    },
                )
        except Exception as exc:
            self._connect_failed = True
            reason = format_connect_failure(exc)
            LOGGER.warning(
                "Cannot connect Image Display Client hub=%s: %s",
                self._hub_name,
                reason,
            )
            self._emit_status("failed", {"reason": reason})
        finally:
            if self._client:
                await self._client.close(hub_unsubscribe=self._want_hub_unsubscribe)
                self._client = None
            self._hub_subscribed = False
            self._hub_live = False
            if not self._connect_failed:
                self._emit_status("disconnected")
