from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Awaitable, Callable


class Transport(str, Enum):
    WS = "ws"
    GRPC = "grpc"


class ConnectionState(str, Enum):
    CONNECTING = "connecting"
    OPEN = "open"
    RECONNECTING = "reconnecting"
    CLOSED = "closed"


@dataclass
class DeviceAuth:
    client_id: str
    client_secret: str


@dataclass
class ListenerAuth:
    access_token: str


RefreshAuthFn = Callable[
    [],
    Awaitable[tuple[DeviceAuth | None, ListenerAuth | None]],
]


@dataclass
class Config:
    endpoint: str = ""
    transport: Transport = Transport.WS
    device: DeviceAuth | None = None
    listener: ListenerAuth | None = None
    ws_path: str = ""
    disable_reconnect: bool = False
    reconnect_min_delay: float = 0.0
    reconnect_max_delay: float = 0.0
    reconnect_max_attempts: int = 0
    refresh_auth: RefreshAuthFn | None = None
    max_queue_size: int = 10_000
    hello_timeout: float = 10.0
