from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Awaitable, Callable

PROTOCOL_VERSION = 2
SUBPROTOCOL = "tracking.v2"
DEFAULT_WS_PATH = "/v2/ws"
MAX_STRING = 4096
MAX_LOC_POINTS = 100
MAX_BUFFER_POINTS = 10_000
MAX_IN_FLIGHT_FRAMES = 8


class ErrorCode(IntEnum):
    AUTH = 1
    TRACK_NOT_FOUND = 2
    FENCED = 3
    TRY_AGAIN = 4
    INVALID = 5
    UNAUTHORIZED = 6

    # Aliases used by older tests / call sites.
    ERROR_CODE_AUTH = 1
    ERROR_CODE_TRACK_NOT_FOUND = 2
    ERROR_CODE_FENCED = 3
    ERROR_CODE_TRY_AGAIN = 4
    ERROR_CODE_INVALID = 5
    ERROR_CODE_UNAUTHORIZED = 6


class CommandAckStatus(IntEnum):
    UNSPECIFIED = 0
    OK = 1
    REJECTED = 2
    FAILED = 3


class Transport(str, Enum):
    WS = "ws"


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
    transport: str = Transport.WS
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
    subscribe: list[str] = field(default_factory=list)


@dataclass
class LatLng:
    latitude: float = 0.0
    longitude: float = 0.0
    altitude: float | None = None
    accuracy: float | None = None
    heading: float | None = None
    speed: float | None = None
    timestamp_ms: int | None = None


@dataclass
class Resume:
    track_uid: str = ""
    last_seq: int = 0
    last_client_seq: int = 0  # alias used by older tests

    def __post_init__(self) -> None:
        if self.last_client_seq and not self.last_seq:
            self.last_seq = self.last_client_seq
        elif self.last_seq and not self.last_client_seq:
            self.last_client_seq = self.last_seq


@dataclass
class TrackStart:
    location: LatLng | None = None
    route: list[LatLng] = field(default_factory=list)
    metadata: bytes = b""


@dataclass
class TrackStop:
    pass


@dataclass
class Loc:
    seq: int = 0
    points: list[LatLng] = field(default_factory=list)


@dataclass
class Subscribe:
    device_uid: str = ""
    include_events: bool = True
    min_interval_ms: int = 0


@dataclass
class Unsubscribe:
    sub: int = 0


@dataclass
class Event:
    payload: bytes = b""
    timestamp_ms: int = 0


@dataclass
class CommandAck:
    command_id: str = ""
    status: int = CommandAckStatus.OK
    message: str = ""


@dataclass
class Hello:
    version: int = PROTOCOL_VERSION
    shard: int = 0
    node_id: str = ""


@dataclass
class Relocate:
    retry_after_ms: int = 0
    endpoint: str = ""


@dataclass
class ResumeOk:
    track_uid: str = ""
    last_acked: int = 0
    last_acked_seq: int = 0

    def __post_init__(self) -> None:
        if self.last_acked_seq and not self.last_acked:
            self.last_acked = self.last_acked_seq
        elif self.last_acked and not self.last_acked_seq:
            self.last_acked_seq = self.last_acked


@dataclass
class TrackStarted:
    track_uid: str = ""
    metadata: bytes = b""


@dataclass
class TrackStopped:
    track_uid: str = ""


@dataclass
class Ack:
    seq: int = 0


@dataclass
class ServerLoc:
    sub: int = 0
    seq: int = 0
    point: LatLng = field(default_factory=LatLng)


@dataclass
class Subscribed:
    sub: int = 0
    device_uid: str = ""
    track_uid: str = ""
    online: bool = False
    last_location: LatLng | None = None
    last_seen_ms: int | None = None
    route: list[LatLng] = field(default_factory=list)
    est_distance: float = 0.0
    est_duration: float = 0.0
    start_name: str = ""
    end_name: str = ""
    metadata: bytes = b""


@dataclass
class WireError:
    code: int = ErrorCode.INVALID
    retry_after_ms: int = 0
    track_uid: str = ""
    message: str = ""


@dataclass
class EventAdded:
    sub: int = 0
    payload: bytes = b""
    timestamp_ms: int = 0


@dataclass
class Command:
    command_id: str = ""
    payload: bytes = b""
    timestamp_ms: int = 0


@dataclass
class Presence:
    sub: int = 0
    online: bool = False
    last_seen_ms: int = 0


@dataclass
class ClientMsg:
    resume: Resume | None = None
    track_start: TrackStart | None = None
    track_stop: TrackStop | None = None
    loc: Loc | None = None
    subscribe: Subscribe | None = None
    unsubscribe: Unsubscribe | None = None
    event: Event | None = None
    command_ack: CommandAck | None = None
    unknown: int | None = None

    def kind(self) -> str | None:
        if self.resume is not None:
            return "resume"
        if self.track_start is not None:
            return "track_start"
        if self.track_stop is not None:
            return "track_stop"
        if self.loc is not None:
            return "loc"
        if self.subscribe is not None:
            return "subscribe"
        if self.unsubscribe is not None:
            return "unsubscribe"
        if self.event is not None:
            return "event"
        if self.command_ack is not None:
            return "command_ack"
        return None

    def WhichOneof(self, _name: str = "body") -> str | None:
        return self.kind()


@dataclass
class ServerMsg:
    hello: Hello | None = None
    relocate: Relocate | None = None
    resume_ok: ResumeOk | None = None
    track_started: TrackStarted | None = None
    track_stopped: TrackStopped | None = None
    ack: Ack | None = None
    loc: ServerLoc | None = None
    subscribed: Subscribed | None = None
    error: WireError | None = None
    event_added: EventAdded | None = None
    command: Command | None = None
    presence: Presence | None = None

    def kind(self) -> str | None:
        if self.hello is not None:
            return "hello"
        if self.relocate is not None:
            return "relocate"
        if self.resume_ok is not None:
            return "resume_ok"
        if self.track_started is not None:
            return "track_started"
        if self.track_stopped is not None:
            return "track_stopped"
        if self.ack is not None:
            return "ack"
        if self.loc is not None:
            return "loc"
        if self.subscribed is not None:
            return "subscribed"
        if self.error is not None:
            return "error"
        if self.event_added is not None:
            return "event_added"
        if self.command is not None:
            return "command"
        if self.presence is not None:
            return "presence"
        return None

    def WhichOneof(self, _name: str = "body") -> str | None:
        return self.kind()
