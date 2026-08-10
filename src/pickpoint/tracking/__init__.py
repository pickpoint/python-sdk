"""Realtime tracking (WebSocket by default, gRPC supported)."""

from .backoff import new_backoff, next_delay, reset_backoff, BackoffState
from .client import (
    MAX_EVENT_BYTES,
    MAX_EVENT_HZ,
    MAX_PUBLISH_HZ,
    MIN_EVENT_INTERVAL,
    MIN_PUBLISH_INTERVAL,
    SUBPROTOCOL,
    Client,
    connect,
)
from .codec import client_resume, decode_server_msg, encode_client_msg, stamp_lat_lng
from .errors import Error, is_auth_error, is_fatal_resume_error, new_error
from .queue import OfflineQueue, QueuedPoint
from .rate import MIN_PUBLISH_INTERVAL_MS, can_accept_publish, next_publish_allowed_at
from .types import (
    Config,
    ConnectionState,
    DeviceAuth,
    ListenerAuth,
    RefreshAuthFn,
    Transport,
)
from .url import build_ws_url

__all__ = [
    "BackoffState",
    "Client",
    "Config",
    "ConnectionState",
    "DeviceAuth",
    "Error",
    "ListenerAuth",
    "MAX_EVENT_BYTES",
    "MAX_EVENT_HZ",
    "MAX_PUBLISH_HZ",
    "MIN_EVENT_INTERVAL",
    "MIN_PUBLISH_INTERVAL",
    "MIN_PUBLISH_INTERVAL_MS",
    "OfflineQueue",
    "QueuedPoint",
    "RefreshAuthFn",
    "SUBPROTOCOL",
    "Transport",
    "build_ws_url",
    "can_accept_publish",
    "client_resume",
    "connect",
    "decode_server_msg",
    "encode_client_msg",
    "is_auth_error",
    "is_fatal_resume_error",
    "new_backoff",
    "new_error",
    "next_delay",
    "next_publish_allowed_at",
    "reset_backoff",
    "stamp_lat_lng",
]
