from __future__ import annotations

from urllib.parse import urlencode, urlsplit, urlunsplit

from .types import Config


def build_ws_url(cfg: Config) -> str:
    raw = (cfg.endpoint or "").strip()
    if not raw:
        raise ValueError("tracking: Endpoint is required")
    if "://" not in raw:
        raw = "ws://" + raw
    parts = urlsplit(raw)
    scheme = parts.scheme
    if scheme == "http":
        scheme = "ws"
    elif scheme == "https":
        scheme = "wss"
    elif scheme not in ("ws", "wss"):
        raise ValueError(f"tracking: unsupported scheme {scheme!r}")
    path = cfg.ws_path or "/v2/tracking/ws"
    q: dict[str, str] = {}
    if cfg.device is not None:
        q["client-id"] = cfg.device.client_id
        q["client-secret"] = cfg.device.client_secret
    elif cfg.listener is not None:
        q["access-token"] = cfg.listener.access_token
    return urlunsplit((scheme, parts.netloc, path, urlencode(q), ""))
