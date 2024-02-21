# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.client.dispatch package."""

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import grpc

# pylint: disable=no-name-in-module
from frequenz.api.dispatch.v1.dispatch_pb2 import Dispatch as PBDispatch
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    DispatchCreateRequest as PBDispatchCreateRequest,
)
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    DispatchDeleteRequest,
    DispatchGetRequest,
    DispatchList,
)
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    DispatchListRequest as PBDispatchListRequest,
)
from frequenz.api.dispatch.v1.dispatch_pb2 import DispatchUpdateRequest
from frequenz.client.common.microgrid.components import ComponentCategory
from google.protobuf.empty_pb2 import Empty
from google.protobuf.timestamp_pb2 import Timestamp
from pytest import raises

from frequenz.client.dispatch import Client
from frequenz.client.dispatch._internal_types import DispatchCreateRequest
from frequenz.client.dispatch.types import (
    Dispatch,
    EndCriteria,
    Frequency,
    RecurrenceRule,
    Weekday,
)


class DispatchSampler:
    """Generates random dispatch messages."""

    def __init__(self, seed: int = 0) -> None:
        """Initialize the sampler.

        Args:
            seed: seed to initialize the rng with
        """
        self._rng = random.Random(seed)
        self._last_id: int = 0

    def recurrence_rule(self) -> RecurrenceRule:
        """Generate a random recurrence rule.

        Returns:
            a random recurrence rule
        """
        return RecurrenceRule(
            frequency=self._rng.choice(list(Frequency)[1:]),
            interval=self._rng.randint(1, 100),
            end_criteria=self._rng.choice(
                [
                    None,
                    self._rng.choice(
                        [
                            EndCriteria(count=self._rng.randint(1, 1000)),
                            EndCriteria(
                                until=datetime.fromtimestamp(
                                    self._rng.randint(0, 1000000),
                                    tz=timezone.utc,
                                )
                            ),
                        ]
                    ),
                ]
            ),
            byminutes=[
                self._rng.randint(0, 59) for _ in range(self._rng.randint(0, 10))
            ],
            byhours=[self._rng.randint(0, 23) for _ in range(self._rng.randint(0, 10))],
            byweekdays=[
                self._rng.choice(list(Weekday)[1:])
                for _ in range(self._rng.randint(0, 7))
            ],
            bymonthdays=[
                self._rng.randint(1, 31) for _ in range(self._rng.randint(0, 10))
            ],
            bymonths=[
                self._rng.randint(1, 12) for _ in range(self._rng.randint(0, 12))
            ],
        )

    def __call__(self) -> Dispatch:
        """Generate a random dispatch instance.

        Returns:
            a random dispatch instance
        """
        self._last_id += 1
        create_time = datetime.fromtimestamp(
            self._rng.randint(0, 1000000), tz=timezone.utc
        )

        return Dispatch(
            id=self._last_id,
            create_time=create_time,
            update_time=create_time + timedelta(seconds=self._rng.randint(0, 1000000)),
            microgrid_id=self._rng.randint(0, 100),
            type=str(self._rng.randint(0, 100_000)),
            start_time=datetime.now().astimezone(timezone.utc)
            + timedelta(seconds=self._rng.randint(0, 1000000)),
            duration=timedelta(seconds=self._rng.randint(0, 1000000)),
            selector=self._rng.choice(  # type: ignore
                [
                    self._rng.choice(list(ComponentCategory)[1:]),
                    [
                        self._rng.randint(1, 100)
                        for _ in range(self._rng.randint(1, 10))
                    ],
                ]
            ),
            active=self._rng.choice([True, False]),
            dry_run=self._rng.choice([True, False]),
            payload={
                f"key_{i}": self._rng.choice(
                    [
                        self._rng.randint(0, 100),
                        self._rng.uniform(0, 100),
                        self._rng.choice([True, False]),
                        self._rng.choice(["a", "b", "c"]),
                    ]
                )
                for i in range(self._rng.randint(0, 10))
            },
            recurrence=self.recurrence_rule(),
        )


def _to_dt(dt: Timestamp) -> datetime:
    """Convert a timestamp to a datetime."""
    return datetime.fromtimestamp(dt.seconds, tz=timezone.utc)


def dispatch_from_request(
    _request: DispatchCreateRequest,
    _id: int,
    create_time: datetime,
    update_time: datetime,
) -> Dispatch:
    """Initialize a Dispatch object from a request.

    Args:
        _request: The dispatch create request.
        _id: The unique identifier for the dispatch.
        create_time: The creation time of the dispatch in UTC.
        update_time: The last update time of the dispatch in UTC.

    Returns:
        The initialized dispatch.
    """
    params = _request.__dict__
    params["active"] = params.pop("is_active")
    params["dry_run"] = params.pop("is_dry_run")

    return Dispatch(
        id=_id,
        create_time=create_time,
        update_time=update_time,
        **params,
    )


@dataclass
class DispatchMockService:
    """Dispatch mock service for testing."""

    dispatches: list[Dispatch]

    last_update_request: DispatchUpdateRequest | None = None

    _last_id: int = 0

    def __init__(self) -> None:
        """Initialize the mock service."""
        self.dispatches = []

    # pylint: disable=invalid-name
    async def ListMicrogridDispatches(
        self, request: PBDispatchListRequest
    ) -> DispatchList:
        """List microgrid dispatches.

        Args:
            request: The request.

        Returns:
            The dispatch list.
        """
        return DispatchList(
            dispatches=map(
                lambda d: d.to_protobuf(),
                filter(
                    lambda d: DispatchMockService._filter_dispatch(d, request),
                    self.dispatches,
                ),
            )
        )

    # pylint: disable=too-many-branches
    @staticmethod
    def _filter_dispatch(dispatch: Dispatch, request: PBDispatchListRequest) -> bool:
        """Filter a dispatch based on the request."""
        if dispatch.microgrid_id != request.microgrid_id:
            return False

        if request.HasField("filter"):
            _filter = request.filter
            for selector in _filter.selectors:
                if selector != dispatch.selector:
                    return False
            if _filter.HasField("time_interval"):
                if start_from := _filter.time_interval.start_from:
                    if dispatch.start_time < _to_dt(start_from):
                        return False
                if start_to := _filter.time_interval.start_to:
                    if dispatch.start_time >= _to_dt(start_to):
                        return False
                if end_from := _filter.time_interval.end_from:
                    if dispatch.start_time + dispatch.duration < _to_dt(end_from):
                        return False
                if end_to := _filter.time_interval.end_to:
                    if dispatch.start_time + dispatch.duration >= _to_dt(end_to):
                        return False
            if _filter.HasField("is_active"):
                if dispatch.active != _filter.is_active:
                    return False
            if _filter.HasField("is_dry_run"):
                if dispatch.dry_run != _filter.is_dry_run:
                    return False

        return True

    async def CreateMicrogridDispatch(
        self,
        request: PBDispatchCreateRequest,
    ) -> Empty:
        """Create a new dispatch."""
        self._last_id += 1

        self.dispatches.append(
            dispatch_from_request(
                DispatchCreateRequest.from_protobuf(request),
                self._last_id,
                create_time=datetime.now(tz=timezone.utc),
                update_time=datetime.now(tz=timezone.utc),
            )
        )
        return Empty()

    async def UpdateMicrogridDispatch(
        self,
        request: DispatchUpdateRequest,
    ) -> Empty:
        """Update a dispatch."""
        dispatch = next((d for d in self.dispatches if d.id == request.id), None)

        if dispatch is None:
            return Empty()

        pb_dispatch = dispatch.to_protobuf()

        # Go through the paths in the update mask and update the dispatch
        for path in request.update_mask.paths:
            split_path = path.split(".")

            match split_path[0]:
                # Fields that can be assigned directly
                case "is_active" | "is_dry_run" | "duration":
                    setattr(
                        pb_dispatch,
                        split_path[0],
                        getattr(request.update, split_path[0]),
                    )
                # Fields that need to be copied
                case "start_time" | "selector" | "payload":
                    getattr(pb_dispatch, split_path[0]).CopyFrom(
                        getattr(request.update, split_path[0])
                    )
                case "recurrence":
                    match split_path[1]:
                        case "end_criteria":
                            pb_dispatch.recurrence.end_criteria.CopyFrom(
                                request.update.recurrence.end_criteria
                            )
                        case "freq" | "interval":
                            setattr(
                                pb_dispatch.recurrence,
                                split_path[1],
                                getattr(request.update.recurrence, split_path[1]),
                            )
                        # Fields of type list that need to be copied
                        case (
                            "byminutes"
                            | "byhours"
                            | "byweekdays"
                            | "bymonthdays"
                            | "bymonths"
                        ):
                            getattr(pb_dispatch.recurrence, split_path[1])[:] = getattr(
                                request.update.recurrence, split_path[1]
                            )[:]

        dispatch = Dispatch.from_protobuf(pb_dispatch)
        dispatch.update_time = datetime.now(tz=timezone.utc)

        return Empty()

    async def GetMicrogridDispatch(
        self,
        request: DispatchGetRequest,
    ) -> PBDispatch:
        """Get a single dispatch."""
        dispatch = next((d for d in self.dispatches if d.id == request.id), None)

        if dispatch is None:
            error = grpc.RpcError()
            # pylint: disable=protected-access
            error._code = grpc.StatusCode.NOT_FOUND  # type: ignore
            error._details = "Dispatch not found"  # type: ignore
            # pylint: enable=protected-access
            raise error

        return dispatch.to_protobuf()

    async def DeleteMicrogridDispatch(
        self,
        request: DispatchDeleteRequest,
    ) -> Empty:
        """Delete a given dispatch."""
        num_dispatches = len(self.dispatches)
        self.dispatches = [d for d in self.dispatches if d.id != request.id]

        if len(self.dispatches) == num_dispatches:
            error = grpc.RpcError()
            # pylint: disable=protected-access
            error._code = grpc.StatusCode.NOT_FOUND  # type: ignore
            error._details = "Dispatch not found"  # type: ignore
            # pylint: enable=protected-access
            raise error

        return Empty()

    # pylint: enable=invalid-name


class MockClient(Client):
    """Mock client for testing."""

    def __init__(self) -> None:
        """Initialize the mock client."""
        super().__init__(grpc.aio.insecure_channel("mock"), "mock")
        self._stub = DispatchMockService()  # type: ignore


def _to_create_params(dispatch: Dispatch) -> dict[str, Any]:
    """Convert a dispatch to create parameters."""
    return {
        "microgrid_id": dispatch.microgrid_id,
        "_type": dispatch.type,
        "start_time": dispatch.start_time,
        "duration": dispatch.duration,
        "selector": dispatch.selector,
        "active": dispatch.active,
        "dry_run": dispatch.dry_run,
        "payload": dispatch.payload,
        "recurrence": dispatch.recurrence,
    }


async def test_list_create_dispatches() -> None:
    """Test listing dispatches."""
    sampler = DispatchSampler()
    client = MockClient()

    assert await anext(client.list(microgrid_id=1), None) is None

    for i in range(100):
        sample = sampler()
        await client.create(**_to_create_params(sample))

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
    sampler = DispatchSampler()
    client = MockClient()
    sample = sampler()

    await client.create(**_to_create_params(sample))

    dispatch = await anext(client.list(microgrid_id=sample.microgrid_id), None)
    assert dispatch is not None

    sample.id = dispatch.id
    sample.create_time = dispatch.create_time
    sample.update_time = dispatch.update_time
    assert dispatch == sample

    await client.update(dispatch.id, {"recurrence.interval": 4})


async def test_get_dispatch() -> None:
    """Test getting a dispatch."""
    sampler = DispatchSampler()
    client = MockClient()
    sample = sampler()

    await client.create(**_to_create_params(sample))

    dispatch = await anext(client.list(microgrid_id=sample.microgrid_id), None)
    assert dispatch is not None

    sample.id = dispatch.id
    sample.create_time = dispatch.create_time
    sample.update_time = dispatch.update_time
    assert dispatch == sample

    assert await client.get(dispatch.id) == dispatch


async def test_get_dispatch_fail() -> None:
    """Test getting a non-existent dispatch."""
    client = MockClient()

    with raises(grpc.RpcError):
        await client.get(1)


async def test_delete_dispatch() -> None:
    """Test deleting a dispatch."""
    client = MockClient()
    sampler = DispatchSampler()
    sample = sampler()

    await client.create(**_to_create_params(sample))

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
    client = MockClient()

    with raises(grpc.RpcError):
        await client.delete(1)
