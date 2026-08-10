from __future__ import annotations

import asyncio
import time

import httpx
import pytest
import respx

from pickpoint import Client, ClientAuth, Config, mint_client_tokens


def expires_at_ms(from_now: float) -> int:
    return int(time.time() * 1000) + int(from_now * 1000)


@pytest.mark.asyncio
@respx.mock
async def test_client_auth_refresh_on_401() -> None:
    base = "https://api.test"
    n = {"i": 0}

    def forward(request: httpx.Request) -> httpx.Response:
        n["i"] += 1
        auth = request.headers.get("Authorization", "")
        if n["i"] == 1:
            assert auth == "Bearer access-1"
            return httpx.Response(401, json={})
        assert auth == "Bearer access-2"
        return httpx.Response(200, json=[{"ok": True}])

    respx.post(f"{base}/v2/client-tokens/refresh").mock(
        return_value=httpx.Response(
            200,
            json={
                "accessToken": "access-2",
                "refreshToken": "refresh-2",
                "expiresAt": expires_at_ms(60),
            },
        )
    )
    respx.get(url__regex=r".*/v2/geocode/forward.*").mock(side_effect=forward)

    async with Client(
        Config(
            base_url=base,
            client_auth=ClientAuth(
                access_token="access-1",
                refresh_token="refresh-1",
                expires_at=expires_at_ms(60),
            ),
        )
    ) as c:
        out = await c.forward({"q": "a"})
        assert len(out) == 1


@pytest.mark.asyncio
@respx.mock
async def test_api_key_header() -> None:
    base = "https://api.test"

    def check(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("x-api-key") == "k"
        return httpx.Response(200, json=[{"place": 1}])

    respx.get(url__regex=r".*/v2/geocode/forward.*").mock(side_effect=check)
    async with Client(Config(base_url=base, api_key="k")) as c:
        out = await c.forward({"q": "Berlin"})
        assert out[0]["place"] == 1


@pytest.mark.asyncio
@respx.mock
async def test_devices_list() -> None:
    base = "https://api.test"
    respx.get(f"{base}/v2/devices").mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"uid": "d1", "name": "n"}], "total": 1},
        )
    )
    async with Client(Config(base_url=base, api_key="k")) as c:
        result = await c.devices.list()
        assert result.total == 1
        assert result.data[0].uid == "d1"


@pytest.mark.asyncio
@respx.mock
async def test_mint_client_tokens() -> None:
    base = "https://api.test"
    respx.post(f"{base}/v2/client-tokens").mock(
        return_value=httpx.Response(
            200,
            json={
                "accessToken": "a",
                "refreshToken": "r",
                "expiresAt": expires_at_ms(120),
            },
        )
    )
    pair = await mint_client_tokens(
        Config(base_url=base, api_key="secret"),
        scopes=["geocoding"],
        ttl_sec=120,
    )
    assert pair.access_token == "a"
    assert pair.refresh_token == "r"


@pytest.mark.asyncio
@respx.mock
async def test_forward_batch() -> None:
    base = "https://api.test"
    calls = {"n": 0}

    def one(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=[{"i": calls["n"]}])

    respx.get(url__regex=r".*/v2/geocode/forward.*").mock(side_effect=one)
    async with Client(Config(base_url=base, api_key="k", concurrency=5)) as c:
        out = await c.forward_batch([{"q": "a"}, {"q": "b"}, {"q": "c"}])
        assert len(out) == 3
        assert calls["n"] == 3
