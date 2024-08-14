# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Dispatch API client for Python."""
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Awaitable, Iterator, cast

import grpc

# pylint: disable=no-name-in-module
from frequenz.api.common.v1.pagination.pagination_params_pb2 import PaginationParams
from frequenz.api.dispatch.v1 import dispatch_pb2_grpc
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
        page_size: int | None = None,
        page_token: str | None = None,
    ) -> AsyncIterator[Dispatch]:
        """List dispatches.

        This function handles pagination internally and returns an async iterator
        over the dispatches. Pagination parameters like `page_size` and `page_token`
        can be used, but they are mutually exclusive.

        The `page_token` argument can be used to continue iterating from a
        specific token, however there is currently no way to get the internal
        next page token, so this parameter is mainly for internal use.

        Create an issue if you need this feature.

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
            page_size: optional, number of dispatches to return per page. Conflicts with page_token.
            page_token: optional, token for the next page, will continue iterating from this token.

        Returns:
            An async iterator of dispatches.

        Raises:
            ValueError: If both page_size and page_token are provided.
        """
        if page_size is not None and page_token is not None:
            raise ValueError("page_size and page_token are mutually exclusive")

        def to_interval(
            from_: datetime | None, to: datetime | None
        ) -> PBTimeIntervalFilter | None:
            return (
                PBTimeIntervalFilter(
                    **{"from": to_timestamp(from_)}, to=to_timestamp(to)
                )
                if from_ or to
                else None
            )

        # Setup parameters
        start_time_interval = to_interval(start_from, start_to)
        end_time_interval = to_interval(end_from, end_to)
        selectors = list(map(component_selector_to_protobuf, component_selectors))
        filters = DispatchFilter(
            selectors=selectors,
            start_time_interval=start_time_interval,
            end_time_interval=end_time_interval,
            is_active=active,
            is_dry_run=dry_run,
        )

        # Iterate over pages
        while True:
            pagination = PaginationParams(page_size=page_size, page_token=page_token)
            request = ListMicrogridDispatchesRequest(
                microgrid_id=microgrid_id, filter=filters, pagination_params=pagination
            )

            response = await cast(
                Awaitable[ListMicrogridDispatchesResponse],
                self._stub.ListMicrogridDispatches(request, metadata=self._metadata),
            )
            for dispatch in response.dispatches:
                yield Dispatch.from_protobuf(dispatch)

            if next_page_token := response.pagination_info.next_page_token:
                page_token = next_page_token
                page_size = None
            else:
                break

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

        return Dispatch.from_protobuf(response.dispatch)

    async def update(
        self,
        *,
        microgrid_id: int,
        dispatch_id: int,
        new_fields: dict[str, Any],
    ) -> Dispatch:
        """Update a dispatch.

        The `new_fields` argument is a dictionary of fields to update. The keys are
        the field names, and the values are the new values for the fields.

        For recurrence fields, the keys are preceeded by "recurrence.".

        Note that updating `type` and `dry_run` is not supported.

        Args:
            microgrid_id: The microgrid_id to update the dispatch for.
            dispatch_id: The dispatch_id to update.
            new_fields: The fields to update.

        Returns:
            Dispatch: The updated dispatch.

        Raises:
            ValueError: If updating `type` or `dry_run`.
        """
        msg = UpdateMicrogridDispatchRequest(
            dispatch_id=dispatch_id, microgrid_id=microgrid_id
        )

        for key, val in new_fields.items():
            path = key.split(".")

            match path[0]:
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
                        case _:
                            raise ValueError(f"Unknown recurrence field: {path[1]}")
                case _:
                    raise ValueError(f"Unknown field: {path[0]}")

            msg.update_mask.paths.append(key)

        response = await cast(
            Awaitable[UpdateMicrogridDispatchResponse],
            self._stub.UpdateMicrogridDispatch(msg, metadata=self._metadata),
        )

        return Dispatch.from_protobuf(response.dispatch)

    async def get(self, *, microgrid_id: int, dispatch_id: int) -> Dispatch:
        """Get a dispatch.

        Args:
            microgrid_id: The microgrid_id to get the dispatch for.
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

    async def delete(self, *, microgrid_id: int, dispatch_id: int) -> None:
        """Delete a dispatch.

        Args:
            microgrid_id: The microgrid_id to delete the dispatch for.
            dispatch_id: The dispatch_id to delete.
        """
        request = DeleteMicrogridDispatchRequest(
            dispatch_id=dispatch_id, microgrid_id=microgrid_id
        )
        await cast(
            Awaitable[None],
            self._stub.DeleteMicrogridDispatch(request, metadata=self._metadata),
        )
