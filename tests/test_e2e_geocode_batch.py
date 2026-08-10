"""Live e2e geocode batch (skipped unless PICKPOINT_API_KEY is set)."""

from __future__ import annotations

import os
import time

import pytest

from pickpoint import Client, Config

E2E_BATCH_SIZE = 1000


def _e2e_config() -> Config | None:
    key = os.environ.get("PICKPOINT_API_KEY")
    if not key:
        return None
    base = os.environ.get("PICKPOINT_BASE_URL", "https://beta-api.pickpoint.io")
    return Config(api_key=key, base_url=base, timeout=60.0)


@pytest.mark.asyncio
async def test_e2e_forward_batch_1000() -> None:
    cfg = _e2e_config()
    if cfg is None:
        pytest.skip("PICKPOINT_API_KEY not set")
    qs = [{"q": "Berlin", "limit": "1"} for _ in range(E2E_BATCH_SIZE)]
    async with Client(cfg) as c:
        start = time.perf_counter()
        out = await c.forward_batch(qs)
        wall = time.perf_counter() - start
    assert len(out) == E2E_BATCH_SIZE
    for i, slot in enumerate(out):
        assert slot, f"slot {i} empty"
    print(f"forward batch n={E2E_BATCH_SIZE} wall={wall:.3f}s")


@pytest.mark.asyncio
async def test_e2e_reverse_batch_1000() -> None:
    cfg = _e2e_config()
    if cfg is None:
        pytest.skip("PICKPOINT_API_KEY not set")
    qs = [{"lat": "52.52", "lon": "13.405"} for _ in range(E2E_BATCH_SIZE)]
    async with Client(cfg) as c:
        start = time.perf_counter()
        out = await c.reverse_batch(qs)
        wall = time.perf_counter() - start
    assert len(out) == E2E_BATCH_SIZE
    for i, slot in enumerate(out):
        assert slot is not None, f"slot {i} empty"
    print(f"reverse batch n={E2E_BATCH_SIZE} wall={wall:.3f}s")
