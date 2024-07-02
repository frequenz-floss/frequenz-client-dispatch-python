# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.client.dispatch package."""

import random
from dataclasses import replace

import grpc
from pytest import raises

from frequenz.client.dispatch.test.client import FakeClient, to_create_params
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.client.dispatch.types import Dispatch


def _update(dispatch: Dispatch, created: Dispatch) -> Dispatch:
    """Update the dispatch.

    Updates the id, create_time and update_time of the local dispatch
    with the values from the created dispatch.

    Args:
        dispatch: The local dispatch.
        created: The created dispatch.

    Returns:
        The updated dispatch.
    """
    return replace(
        dispatch,
        id=created.id,
        create_time=created.create_time,
        update_time=created.update_time,
    )


async def test_create_dispatch() -> None:
    """Test creating a dispatch."""
    sampler = DispatchGenerator()
    client = FakeClient()
    sample = sampler.generate_dispatch()
    microgrid_id = random.randint(1, 100)

    await client.create(**to_create_params(microgrid_id, sample))

    stored_dispatches = client.dispatches(microgrid_id)

    # Before we can compare, we need to set the create_time and update_time
    # as they are generated by the (fake) remote point
    sample = _update(sample, stored_dispatches[0])

    assert len(stored_dispatches) == 1
    assert stored_dispatches[0] == sample


async def test_create_return_dispatch() -> None:
    """Test creating a dispatch and returning the created dispatch."""
    # Make sure we don't rely on the order of the dispatches
    generator = DispatchGenerator()
    client = FakeClient()

    for _ in range(100):
        sample = generator.generate_dispatch()

        dispatch = await client.create(**to_create_params(1, sample))

        # Before we can compare, we need to set the create_time and update_time
        # as they are generated by the (fake) remote point
        sample = _update(sample, dispatch)

        assert dispatch == sample


async def test_list_dispatches() -> None:
    """Test listing dispatches."""
    sampler = DispatchGenerator()
    client = FakeClient()

    client.set_dispatches(
        microgrid_id=1, value=[sampler.generate_dispatch() for _ in range(100)]
    )

    async for dispatch in client.list(microgrid_id=1):
        assert dispatch in client.dispatches(microgrid_id=1)


async def test_list_create_dispatches() -> None:
    """Test listing dispatches."""
    sampler = DispatchGenerator()
    client = FakeClient()

    assert await anext(client.list(microgrid_id=1), None) is None

    for i in range(100):
        sample = sampler.generate_dispatch()
        await client.create(**to_create_params(1, sample))

        dispatch = None
        async for _dispatch in client.list(microgrid_id=1):
            dispatch = _dispatch

        if dispatch is None:
            raise AssertionError("Dispatch not found")

        sample = _update(sample, dispatch)
        assert dispatch == sample


async def test_update_dispatch() -> None:
    """Test updating a dispatch."""
    sampler = DispatchGenerator()
    client = FakeClient()
    sample = sampler.generate_dispatch()
    microgrid_id = random.randint(1, 100)

    await client.create(**to_create_params(microgrid_id, sample))

    dispatch = await anext(client.list(microgrid_id=microgrid_id), None)
    assert dispatch is not None

    sample = _update(sample, dispatch)
    assert dispatch == sample

    await client.update(
        microgrid_id=microgrid_id,
        dispatch_id=dispatch.id,
        new_fields={"recurrence.interval": 4},
    )
    assert client.dispatches(microgrid_id)[0].recurrence.interval == 4


async def test_update_dispatch_fail() -> None:
    """Test updating the type and dry_run fields of a dispatch."""
    sampler = DispatchGenerator()
    client = FakeClient()
    sample = sampler.generate_dispatch()

    response = await client.create(**to_create_params(1, sample))
    dispatch = client.dispatches(1)[0]

    assert dispatch is not None
    assert response == dispatch

    sample = _update(sample, dispatch)
    assert dispatch == sample

    for field, value in [
        ("type", "new_type"),
        ("dry_run", True),
        ("is_dry_run", True),
    ]:
        with raises(ValueError):
            await client.update(
                microgrid_id=1, dispatch_id=dispatch.id, new_fields={field: value}
            )


async def test_get_dispatch() -> None:
    """Test getting a dispatch."""
    sampler = DispatchGenerator()
    client = FakeClient()
    sample = sampler.generate_dispatch()
    microgrid_id = random.randint(1, 100)

    await client.create(**to_create_params(microgrid_id, sample))

    dispatch = await anext(client.list(microgrid_id=microgrid_id), None)
    assert dispatch is not None

    sample = _update(sample, dispatch)
    assert dispatch == sample

    assert (
        await client.get(microgrid_id=microgrid_id, dispatch_id=dispatch.id) == dispatch
    )


async def test_get_dispatch_fail() -> None:
    """Test getting a non-existent dispatch."""
    client = FakeClient()

    with raises(grpc.RpcError):
        await client.get(microgrid_id=1, dispatch_id=1)


async def test_delete_dispatch() -> None:
    """Test deleting a dispatch."""
    client = FakeClient()
    sampler = DispatchGenerator()
    sample = sampler.generate_dispatch()
    microgrid_id = 5

    await client.create(**to_create_params(microgrid_id, sample))

    dispatch = await anext(client.list(microgrid_id=microgrid_id), None)
    assert dispatch is not None

    sample = _update(sample, dispatch)
    assert dispatch == sample

    await client.delete(microgrid_id=microgrid_id, dispatch_id=dispatch.id)

    assert await anext(client.list(microgrid_id=microgrid_id), None) is None


async def test_delete_dispatch_fail() -> None:
    """Test deleting a non-existent dispatch."""
    client = FakeClient()

    with raises(grpc.RpcError):
        await client.delete(microgrid_id=1, dispatch_id=1)
