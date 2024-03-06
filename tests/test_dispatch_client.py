# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.client.dispatch package."""

import grpc
from pytest import raises

from frequenz.client.dispatch.test.client import FakeClient, to_create_params
from frequenz.client.dispatch.test.generator import DispatchGenerator


async def test_list_create_dispatches() -> None:
    """Test listing dispatches."""
    sampler = DispatchGenerator()
    client = FakeClient()

    assert await anext(client.list(microgrid_id=1), None) is None

    for i in range(100):
        sample = sampler.generate_dispatch()
        await client.create(**to_create_params(sample))

        dispatch = None
        async for _dispatch in client.list(microgrid_id=sample.microgrid_id):
            dispatch = _dispatch

        if dispatch is None:
            raise AssertionError("Dispatch not found")

        sample.id = dispatch.id
        sample.create_time = dispatch.create_time
        sample.update_time = dispatch.update_time
        assert dispatch == sample


async def test_update_dispatch() -> None:
    """Test updating a dispatch."""
    sampler = DispatchGenerator()
    client = FakeClient()
    sample = sampler.generate_dispatch()

    await client.create(**to_create_params(sample))

    dispatch = await anext(client.list(microgrid_id=sample.microgrid_id), None)
    assert dispatch is not None

    sample.id = dispatch.id
    sample.create_time = dispatch.create_time
    sample.update_time = dispatch.update_time
    assert dispatch == sample

    await client.update(dispatch.id, {"recurrence.interval": 4})


async def test_get_dispatch() -> None:
    """Test getting a dispatch."""
    sampler = DispatchGenerator()
    client = FakeClient()
    sample = sampler.generate_dispatch()

    await client.create(**to_create_params(sample))

    dispatch = await anext(client.list(microgrid_id=sample.microgrid_id), None)
    assert dispatch is not None

    sample.id = dispatch.id
    sample.create_time = dispatch.create_time
    sample.update_time = dispatch.update_time
    assert dispatch == sample

    assert await client.get(dispatch.id) == dispatch


async def test_get_dispatch_fail() -> None:
    """Test getting a non-existent dispatch."""
    client = FakeClient()

    with raises(grpc.RpcError):
        await client.get(1)


async def test_delete_dispatch() -> None:
    """Test deleting a dispatch."""
    client = FakeClient()
    sampler = DispatchGenerator()
    sample = sampler.generate_dispatch()

    await client.create(**to_create_params(sample))

    dispatch = await anext(client.list(microgrid_id=sample.microgrid_id), None)
    assert dispatch is not None

    sample.id = dispatch.id
    sample.create_time = dispatch.create_time
    sample.update_time = dispatch.update_time
    assert dispatch == sample

    await client.delete(dispatch.id)

    assert await anext(client.list(microgrid_id=sample.microgrid_id), None) is None


async def test_delete_dispatch_fail() -> None:
    """Test deleting a non-existent dispatch."""
    client = FakeClient()

    with raises(grpc.RpcError):
        await client.delete(1)
