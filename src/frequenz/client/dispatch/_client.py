# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Dispatch API client for Python."""
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Iterator

import grpc
from frequenz.api.dispatch.v1 import dispatch_pb2_grpc

# pylint: disable=no-name-in-module
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    DispatchDeleteRequest,
    DispatchFilter,
    DispatchGetRequest,
    DispatchListRequest,
    DispatchUpdateRequest,
)
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    TimeIntervalFilter as PBTimeIntervalFilter,
)
from google.protobuf.timestamp_pb2 import Timestamp

from ._internal_types import DispatchCreateRequest
from .types import (
    ComponentSelector,
    Dispatch,
    RecurrenceRule,
    component_selector_to_protobuf,
)

# pylint: enable=no-name-in-module


class Client:
    """Dispatch API client."""

    def __init__(self, grpc_channel: grpc.aio.Channel, svc_addr: str) -> None:
        """Initialize the client.

        Args:
            grpc_channel: gRPC channel to use for communication with the API.
            svc_addr: Address of the service to connect to.
        """
        self._svc_addr = svc_addr
        self._stub = dispatch_pb2_grpc.MicrogridDispatchServiceStub(grpc_channel)

    # pylint: disable=too-many-arguments, too-many-locals
    async def list(
        self,
        microgrid_id: int,
        component_selectors: Iterator[ComponentSelector] = iter(()),
        start_from: datetime | None = None,
        start_to: datetime | None = None,
        end_from: datetime | None = None,
        end_to: datetime | None = None,
        active: bool | None = None,
        dry_run: bool | None = None,
    ) -> AsyncIterator[Dispatch]:
        """List dispatches.

        Example usage:

        ```python
        grpc_channel = grpc.aio.insecure_channel("example")
        client = Client(grpc_channel, "localhost:50051")
        async for dispatch in client.list(microgrid_id=1):
            print(dispatch)
        ```

        Yields:
            Dispatch: The dispatches.

        Args:
            microgrid_id: The microgrid_id to list dispatches for.
            component_selectors: optional, list of component ids or categories to filter by.
            start_from: optional, filter by start_time >= start_from.
            start_to: optional, filter by start_time < start_to.
            end_from: optional, filter by end_time >= end_from.
            end_to: optional, filter by end_time < end_to.
            active: optional, filter by active status.
            dry_run: optional, filter by dry_run status.

        Returns:
            An async iterator of dispatches.
        """
        time_interval = None

        def to_timestamp(dt: datetime | None) -> Timestamp | None:
            if dt is None:
                return None

            ts = Timestamp()
            ts.FromDatetime(dt)
            return ts

        if start_from or start_to or end_from or end_to:
            time_interval = PBTimeIntervalFilter(
                start_from=to_timestamp(start_from),
                start_to=to_timestamp(start_to),
                end_from=to_timestamp(end_from),
                end_to=to_timestamp(end_to),
            )

        selectors = []

        for selector in component_selectors:
            selectors.append(component_selector_to_protobuf(selector))

        filters = DispatchFilter(
            selectors=selectors,
            time_interval=time_interval,
            is_active=active,
            is_dry_run=dry_run,
        )
        request = DispatchListRequest(microgrid_id=microgrid_id, filter=filters)

        response = await self._stub.ListMicrogridDispatches(request)  # type: ignore
        for dispatch in response.dispatches:
            yield Dispatch.from_protobuf(dispatch)

    async def create(
        self,
        microgrid_id: int,
        _type: str,
        start_time: datetime,
        duration: timedelta,
        selector: ComponentSelector,
        active: bool = True,
        dry_run: bool = False,
        payload: dict[str, Any] | None = None,
        recurrence: RecurrenceRule | None = None,
    ) -> None:
        """Create a dispatch.

        Args:
            microgrid_id: The microgrid_id to create the dispatch for.
            _type: User defined string to identify the dispatch type.
            start_time: The start time of the dispatch.
            duration: The duration of the dispatch.
            selector: The component selector for the dispatch.
            active: The active status of the dispatch.
            dry_run: The dry_run status of the dispatch.
            payload: The payload of the dispatch.
            recurrence: The recurrence rule of the dispatch.

        Raises:
            ValueError: If start_time is in the past.
        """
        if start_time <= datetime.now().astimezone(start_time.tzinfo):
            raise ValueError("start_time must not be in the past")

        request = DispatchCreateRequest(
            microgrid_id=microgrid_id,
            type=_type,
            start_time=start_time,
            duration=duration,
            selector=selector,
            is_active=active,
            is_dry_run=dry_run,
            payload=payload or {},
            recurrence=recurrence or RecurrenceRule(),
        ).to_protobuf()

        await self._stub.CreateMicrogridDispatch(request)  # type: ignore

    async def update(
        self,
        dispatch_id: int,
        new_fields: dict[str, Any],
    ) -> None:
        """Update a dispatch.

        The `new_fields` argument is a dictionary of fields to update. The keys are
        the field names, and the values are the new values for the fields.

        For recurrence fields, the keys are preceeded by "recurrence.".

        Args:
            dispatch_id: The dispatch_id to update.
            new_fields: The fields to update.
        """
        msg = DispatchUpdateRequest(id=dispatch_id)

        for key, val in new_fields.items():
            path = key.split(".")

            match path[0]:
                case "type":
                    msg.update.type = val
                case "start_time":
                    msg.update.start_time.FromDatetime(val)
                case "duration":
                    msg.update.duration = int(val.total_seconds())
                case "selector":
                    msg.update.selector.CopyFrom(component_selector_to_protobuf(val))
                case "is_active":
                    msg.update.is_active = val
                case "active":
                    msg.update.is_active = val
                    key = "is_active"
                case "is_dry_run":
                    msg.update.is_dry_run = val
                case "dry_run":
                    msg.update.is_dry_run = val
                    key = "is_dry_run"
                case "recurrence":
                    match path[1]:
                        case "freq":
                            msg.update.recurrence.freq = val
                        # Proto uses "freq" instead of "frequency"
                        case "frequency":
                            msg.update.recurrence.freq = val
                            # Correct the key to "recurrence.freq"
                            key = "recurrence.freq"
                        case "interval":
                            msg.update.recurrence.interval = val
                        case "end_criteria":
                            msg.update.recurrence.end_criteria.CopyFrom(
                                val.to_protobuf()
                            )
                        case "byminutes":
                            msg.update.recurrence.byminutes.extend(val)
                        case "byhours":
                            msg.update.recurrence.byhours.extend(val)
                        case "byweekdays":
                            msg.update.recurrence.byweekdays.extend(val)
                        case "bymonthdays":
                            msg.update.recurrence.bymonthdays.extend(val)
                        case "bymonths":
                            msg.update.recurrence.bymonths.extend(val)

            msg.update_mask.paths.append(key)

        await self._stub.UpdateMicrogridDispatch(msg)  # type: ignore

    async def get(self, dispatch_id: int) -> Dispatch:
        """Get a dispatch.

        Args:
            dispatch_id: The dispatch_id to get.

        Returns:
            Dispatch: The dispatch.
        """
        request = DispatchGetRequest(id=dispatch_id)
        response = await self._stub.GetMicrogridDispatch(request)  # type: ignore
        return Dispatch.from_protobuf(response)

    async def delete(self, dispatch_id: int) -> None:
        """Delete a dispatch.

        Args:
            dispatch_id: The dispatch_id to delete.
        """
        request = DispatchDeleteRequest(id=dispatch_id)
        await self._stub.DeleteMicrogridDispatch(request)  # type: ignore
