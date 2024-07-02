# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Dispatch API client for Python."""
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Awaitable, Iterator, cast

import grpc
from frequenz.api.dispatch.v1 import dispatch_pb2_grpc

# pylint: disable=no-name-in-module
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    CreateMicrogridDispatchResponse,
    DeleteMicrogridDispatchRequest,
    DispatchFilter,
    GetMicrogridDispatchRequest,
    GetMicrogridDispatchResponse,
    ListMicrogridDispatchesRequest,
    ListMicrogridDispatchesResponse,
)
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    TimeIntervalFilter as PBTimeIntervalFilter,
)
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    UpdateMicrogridDispatchRequest,
    UpdateMicrogridDispatchResponse,
)

from frequenz.client.base.conversion import to_timestamp

from ._internal_types import DispatchCreateRequest, rounded_start_time
from .types import (
    ComponentSelector,
    Dispatch,
    RecurrenceRule,
    component_selector_to_protobuf,
)

# pylint: enable=no-name-in-module


class Client:
    """Dispatch API client."""

    def __init__(
        self, *, grpc_channel: grpc.aio.Channel, svc_addr: str, key: str
    ) -> None:
        """Initialize the client.

        Args:
            grpc_channel: gRPC channel to use for communication with the API.
            svc_addr: Address of the service to connect to.
            key: API key to use for authentication.
        """
        self._svc_addr = svc_addr
        self._stub = dispatch_pb2_grpc.MicrogridDispatchServiceStub(grpc_channel)
        self._metadata = (("key", key),)

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
        client = Client(grpc_channel=grpc_channel, svc_addr="localhost:50051", key="key")
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
        request = ListMicrogridDispatchesRequest(
            microgrid_id=microgrid_id, filter=filters
        )

        response = await cast(
            Awaitable[ListMicrogridDispatchesResponse],
            self._stub.ListMicrogridDispatches(request, metadata=self._metadata),
        )
        for dispatch in response.dispatches:
            yield Dispatch.from_protobuf(dispatch)

    async def create(
        self,
        microgrid_id: int,
        type: str,  # pylint: disable=redefined-builtin
        start_time: datetime,
        duration: timedelta,
        selector: ComponentSelector,
        active: bool = True,
        dry_run: bool = False,
        payload: dict[str, Any] | None = None,
        recurrence: RecurrenceRule | None = None,
    ) -> Dispatch:
        """Create a dispatch.

        Will try to return the created dispatch, identifying it by
        the same fields as the request.

        Args:
            microgrid_id: The microgrid_id to create the dispatch for.
            type: User defined string to identify the dispatch type.
            start_time: The start time of the dispatch.
            duration: The duration of the dispatch.
            selector: The component selector for the dispatch.
            active: The active status of the dispatch.
            dry_run: The dry_run status of the dispatch.
            payload: The payload of the dispatch.
            recurrence: The recurrence rule of the dispatch.

        Returns:
            Dispatch: The created dispatch

        Raises:
            ValueError: If start_time is in the past.
            ValueError: If the created dispatch could not be found.
        """
        if start_time <= datetime.now(tz=start_time.tzinfo):
            raise ValueError("start_time must not be in the past")

        # Raise if it's not UTC
        if start_time.tzinfo is None or start_time.tzinfo.utcoffset(start_time) is None:
            raise ValueError("start_time must be timezone aware")

        request = DispatchCreateRequest(
            microgrid_id=microgrid_id,
            type=type,
            start_time=start_time,
            duration=duration,
            selector=selector,
            active=active,
            dry_run=dry_run,
            payload=payload or {},
            recurrence=recurrence or RecurrenceRule(),
        )

        response = await cast(
            Awaitable[CreateMicrogridDispatchResponse],
            self._stub.CreateMicrogridDispatch(
                request.to_protobuf(), metadata=self._metadata
            ),
        )

        if dispatch := await self._try_fetch_created_dispatch(request):
            return dispatch

        raise ValueError("Could not find the created dispatch")

    async def update(
        self,
        *,
        dispatch_id: int,
        new_fields: dict[str, Any],
    ) -> None:
        """Update a dispatch.

        The `new_fields` argument is a dictionary of fields to update. The keys are
        the field names, and the values are the new values for the fields.

        For recurrence fields, the keys are preceeded by "recurrence.".

        Note that updating `type` and `dry_run` is not supported.

        Args:
            dispatch_id: The dispatch_id to update.
            new_fields: The fields to update.

        Raises:
            ValueError: If updating `type` or `dry_run`.
        """
        msg = UpdateMicrogridDispatchRequest(
            dispatch_id=dispatch_id, microgrid_id=microgrid_id
        )

        for key, val in new_fields.items():
            path = key.split(".")

            match path[0]:
                case "type":
                    raise ValueError("Updating type is not supported")
                case "start_time":
                    msg.update.start_time.CopyFrom(to_timestamp(val))
                case "duration":
                    msg.update.duration = int(val.total_seconds())
                case "selector":
                    msg.update.selector.CopyFrom(component_selector_to_protobuf(val))
                case "is_active":
                    msg.update.is_active = val
                case "payload":
                    msg.update.payload.update(val)
                case "active":
                    msg.update.is_active = val
                    key = "is_active"
                case "is_dry_run" | "dry_run":
                    raise ValueError("Updating dry_run is not supported")
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

        response = await cast(
            Awaitable[UpdateMicrogridDispatchResponse],
            self._stub.UpdateMicrogridDispatch(msg, metadata=self._metadata),
        )

    async def get(self, dispatch_id: int) -> Dispatch:
        """Get a dispatch.

        Args:
            dispatch_id: The dispatch_id to get.

        Returns:
            Dispatch: The dispatch.
        """
        request = GetMicrogridDispatchRequest(
            dispatch_id=dispatch_id, microgrid_id=microgrid_id
        )
        response = await cast(
            Awaitable[GetMicrogridDispatchResponse],
            self._stub.GetMicrogridDispatch(request, metadata=self._metadata),
        )
        return Dispatch.from_protobuf(response.dispatch)

    async def delete(self, dispatch_id: int) -> None:
        """Delete a dispatch.

        Args:
            dispatch_id: The dispatch_id to delete.
        """
        request = DeleteMicrogridDispatchRequest(
            dispatch_id=dispatch_id, microgrid_id=microgrid_id
        )
        await cast(
            Awaitable[None],
            self._stub.DeleteMicrogridDispatch(request, metadata=self._metadata),
        )

    async def _try_fetch_created_dispatch(
        self,
        request: DispatchCreateRequest,
    ) -> Dispatch | None:
        """Try to fetch the created dispatch.

        Will return the created dispatch if it was found, otherwise None.

        Args:
            request: The dispatch create request.

        Returns:
            Dispatch: The created dispatch, if it was found.
        """
        async for dispatch in self.list(microgrid_id=request.microgrid_id):
            found = True
            for key, value in request.__dict__.items():
                if key == "start_time":
                    value = rounded_start_time(value)

                if not (found := getattr(dispatch, key) == value):
                    break

            if found:
                return dispatch

        return None
