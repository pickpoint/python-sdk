# pickpoint (Python SDK)

Official Python SDK for [Pickpoint](https://pickpoint.io) — a geolocation platform with four APIs under one key:

| API | What it does |
|-----|----------------|
| **Geocoding** | Address ↔ coordinates (forward, reverse, place lookup) |
| **Address search** | Typeahead / autocomplete for address inputs |
| **Routing** | Routes, matrices, optimized multi-stop, elevation |
| **Device tracking** | Register devices over HTTP; stream live GPS over WebSocket / gRPC |

Built for maps, delivery, logistics, and anything that needs places, routes, or live location. Data is OpenStreetMap-backed; HTTP responses are plain JSON / GeoJSON. Docs: [pickpoint.io/docs](https://pickpoint.io/docs).

**This package** is the idiomatic async Python client for that platform:

| Module | Import | Role |
|--------|--------|------|
| root | `pickpoint` | HTTP: geocode, search, routing, devices, client-tokens |
| [`tracking`](#tracking) | `pickpoint.tracking` | Realtime tracks (WebSocket by default, gRPC supported) |
| `tracking.v2` | `pickpoint.tracking.v2` | Generated protobuf (`tracking.v2`) |

Apache-2.0. Go sibling: [`github.com/pickpoint/go-sdk`](https://github.com/pickpoint/go-sdk). Rust sibling: [`github.com/pickpoint/rust-sdk`](https://github.com/pickpoint/rust-sdk). JS sibling: [`@pickpoint/sdk`](https://github.com/pickpoint/pickpoint-js). Wire schema: [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto).

```bash
pip install pickpoint
```

Requires Python **3.10+**.

```
python-sdk/
  src/pickpoint/           # HTTP client
  src/pickpoint/tracking/  # tracking session client
  src/pickpoint/tracking/v2/  # protobuf stubs
```

---

## Public API

One `Client`, one auth session, whole public HTTP surface:

```python
import asyncio
import os
from pickpoint import Client, Config

async def main() -> None:
    async with Client(Config(api_key=os.environ["PICKPOINT_API_KEY"])) as pp:
        places = await pp.forward({"q": "Berlin", "limit": "5"})
        print(places)

        await pp.reverse({"lat": "52.52", "lon": "13.405"})
        await pp.search({"q": "Alexanderplatz"})
        await pp.route({
            "locations": [
                {"lat": 52.52, "lon": 13.40},
                {"lat": 52.53, "lon": 13.42},
            ],
            "costing": "auto",
        })

        devices = await pp.devices.list()
        print(devices.total)

asyncio.run(main())
```

### API map

| Method | HTTP | Notes |
|--------|------|--------|
| `forward` / `geocoding.forward` | `GET /v2/geocode/forward` | Nominatim-style; returns `list` |
| `reverse` / `geocoding.reverse` | `GET /v2/geocode/reverse` | `dict \| None` |
| `lookup` / `geocoding.lookup` | `GET /v2/address/lookup` | e.g. `osm_ids` |
| `forward_batch` / `reverse_batch` / `lookup_batch` | same | Geocoding **only**; conveyor ≤20 in flight |
| `search` / `address.search` | `GET /v2/address/search` | Photon autocomplete |
| `route` / `optimized_route` / `matrix` / `locate` / `elevation` | `POST /v2/route…` | Valhalla JSON body |
| `devices.list` / `get` / `create` / `update` / `delete` | `/v2/devices` | Typed dataclasses |
| `devices.command` | `POST …/command` | Payload `bytes` (SDK base64-encodes) |
| `mint_client_tokens` | `POST /v2/client-tokens` | Package helper; needs secret `api_key` |

Query params for geocode/address are plain `dict[str, str]`.

### Auth

Provide **exactly one** of:

| Field | Header | Use |
|-------|--------|-----|
| `api_key` | `x-api-key` | Backends, workers, CLIs |
| `client_auth` | `Authorization: Bearer` | Short-lived pair; auto-refresh |
| `access_token` | `Authorization: Bearer` | Static token, no refresh |

Keep the secret API key on the server. For client apps mint **client-tokens** and pass `client_auth`.

```python
from pickpoint import Client, ClientAuth, Config, mint_client_tokens

pair = await mint_client_tokens(
    Config(api_key=os.environ["PICKPOINT_API_KEY"]),
    scopes=["geocoding", "address", "routing", "devices"],
    ttl_sec=600,
)

async with Client(
    Config(
        client_auth=ClientAuth(
            access_token=pair.access_token,
            refresh_token=pair.refresh_token,
            expires_at=pair.expires_at,
        )
    )
) as pp:
    ...
```

Refresh behavior (same as Go/Rust/JS):

1. Proactive refresh at **~50% of access TTL** (single-flight).
2. On HTTP **401**, one refresh + retry.
3. If refresh fails → auth error.

### Config

```python
Config(
    api_key="…",
    base_url="https://api.pickpoint.io",  # default
    timeout=30.0,
    max_retries=3,
    retry_base=1.0,
    concurrency=20,
)
```

| Constant | Value |
|----------|--------|
| `DEFAULT_BASE_URL` | `https://api.pickpoint.io` |
| `DEFAULT_TIMEOUT` | 30s |
| `DEFAULT_MAX_RETRIES` | 3 |
| `DEFAULT_RETRY_BASE` | 1s (`MIN_RETRY_BASE` = 0.2s) |
| `MAX_CONCURRENCY` | 20 |

---

## Tracking

Realtime publisher / listener over **binary WebSocket** (`tracking.v2.proto` subprotocol). **gRPC** via `Transport.GRPC`.

```python
import asyncio
from pickpoint import tracking
from pickpoint.tracking.v2 import LatLng

async def main() -> None:
    client = await tracking.connect(
        tracking.Config(
            endpoint="wss://tracking.pickpoint.io",  # local: "ws://127.0.0.1:3100"
            device=tracking.DeviceAuth(
                client_id=device_uid,
                client_secret=device_secret,
            ),
        )
    )
    try:
        track_uid = await client.start_track(
            LatLng(latitude=55.75, longitude=37.61)
        )
        seq, ok = await client.publish(LatLng(latitude=55.76, longitude=37.62))
        # managed client_seq; ok=False if rate-limited locally
        await client.stop_track()
    finally:
        await client.close()

asyncio.run(main())
```

### Auth modes

| Config | Role |
|--------|------|
| `device=DeviceAuth(…)` | Publisher (device) |
| `listener=ListenerAuth(…)` | Dashboard / subscriber JWT |

Exactly one of `device` / `listener` is required.

### Main methods

| Method | Purpose |
|--------|---------|
| `start_track` | Open a track; returns `track_uid` |
| `publish` | Point on active track (managed `client_seq`); capped at **50 Hz** |
| `resume` | Manual resume; auto-reconnect also resumes |
| `stop_track` | End track |
| `send_event` | Opaque event ≤4 KiB; capped at **1 Hz** |
| `subscribe` | Listener: subscribe to a device UID |
| `recv` | Next `ServerMsg` |
| `recv_command` / `ack_command` | Inbound commands |
| `close` | Tear down session |

Limits enforced client-side: `MAX_PUBLISH_HZ = 50`, `MAX_EVENT_BYTES = 4 KiB`, `MAX_EVENT_HZ = 1`.

---

## Develop

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

Live geocode batch e2e (skipped unless the key is set; 1000 requests each):

```bash
PICKPOINT_API_KEY=… pytest tests/test_e2e_geocode_batch.py -q --tb=short
# optional: PICKPOINT_BASE_URL=https://api.pickpoint.io  (default: https://beta-api.pickpoint.io)
```

Protobuf stubs under `src/pickpoint/tracking/v2` are generated from [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto). Regenerate:

```bash
python -m grpc_tools.protoc \
  -I ../pickpoint-proto \
  --python_out=src/pickpoint/tracking/v2 \
  --grpc_python_out=src/pickpoint/tracking/v2 \
  ../pickpoint-proto/tracking/v2/*.proto
# then flatten nested tracking/v2/ paths and fix imports to `from . import …_pb2`
```
