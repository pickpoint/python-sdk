# pickpoint (Python SDK)

Official Python SDK for [Pickpoint](https://pickpoint.io) — a geolocation platform with four APIs under one key:

| API | What it does |
|-----|----------------|
| **Geocoding** | Address ↔ coordinates (forward, reverse, place lookup) |
| **Address search** | Typeahead / autocomplete for address inputs |
| **Routing** | Routes, matrices, optimized multi-stop, elevation |
| **Device tracking** | Register devices over HTTP; stream live GPS over WebSocket |

Built for maps, delivery, logistics, and anything that needs places, routes, or live location. Data is OpenStreetMap-backed; HTTP responses are plain JSON / GeoJSON. Docs: [pickpoint.io/docs](https://pickpoint.io/docs).

**This package** is the idiomatic async Python client for that platform:

| Module | Import | Role |
|--------|--------|------|
| root | `pickpoint` | HTTP: geocode, search, routing, devices, client-tokens |
| [`tracking`](#tracking) | `pickpoint.tracking` | Live GPS over WebSocket (`tracking.v2`) |

Apache-2.0. Go sibling: [`github.com/pickpoint/go-sdk`](https://github.com/pickpoint/go-sdk). Rust sibling: [`github.com/pickpoint/rust-sdk`](https://github.com/pickpoint/rust-sdk). JS sibling: [`@pickpoint/sdk`](https://github.com/pickpoint/pickpoint-js). Wire schema: [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto).

```bash
pip install pickpoint
```

Requires Python **3.10+**.

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

Live GPS is a **separate** WebSocket session: `wss://tracking.pickpoint.io/v2/ws`, subprotocol `tracking.v2`. It is not the HTTP `Client`.

A dropped socket is not a new trip. The SDK reconnects and **Resumes** the same `track_uid`.

First `publish` starts the trip if none is live. `close` sends `TrackStop` then hangs up. Call `start_track` only to supersede (new order / `TRACK_NOT_FOUND`) or to set a route.

### Device (publisher)

```python
import asyncio
from pickpoint.tracking import Config, DeviceAuth, LatLng, connect

async def main() -> None:
    session = await connect(
        Config(
            endpoint="wss://tracking.pickpoint.io",  # host; SDK appends /v2/ws
            device=DeviceAuth(client_id=device_uid, client_secret=device_secret),
        )
    )
    await session.publish(LatLng(latitude=55.75, longitude=37.61))  # TrackStart if idle
    await session.close()  # TrackStop + hang up

asyncio.run(main())
```

### Listener (dashboard)

The JWT is the **client-token** `access_token` — same one as HTTP `client_auth`. Mint it on your backend with scope `devices`.

```python
import os
from pickpoint import Config, mint_client_tokens
from pickpoint.tracking import Config as TrackingConfig, ListenerAuth, connect

pair = await mint_client_tokens(
    Config(api_key=os.environ["PICKPOINT_API_KEY"]),
    scopes=["devices"],
    ttl_sec=600,
)
session = await connect(
    TrackingConfig(
        endpoint="wss://tracking.pickpoint.io",
        listener=ListenerAuth(access_token=pair.access_token),
        subscribe=[device_uid],
    )
)
while True:
    msg = await session.recv()
    if msg.loc:  # live fan-out; publisher never sees Loc
        print(msg.loc.point.latitude, msg.loc.point.longitude)
```

Wire format: [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto).

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

### CI & release

- **PR to `dev`** → `.github/workflows/ci.yml` (pytest on Python 3.10 / 3.12 / 3.13)
- **Merge `dev` → `main`** (untagged HEAD) → bump **patch**, tag `vX.Y.Z`, PyPI publish (OIDC) + GitHub Release in the same job  
  (tag push via `GITHUB_TOKEN` does not start new workflows — publish cannot wait on the tag event)
- **Manual tag `v*`** (pushed by a human) → publish + GitHub Release

Minor/major: bump `version` in `pyproject.toml` and `__version__` in a PR, merge with `[skip release]` in the commit message, then:

```bash
git tag v2.1.0
git push origin v2.1.0
```

PyPI Trusted Publishing must match this workflow: repo `python-sdk`, workflow `release.yml`, environment `pypi`.

## Contributing

Fork and open a PR against **`dev`**. [CONTRIBUTING.md](CONTRIBUTING.md).
