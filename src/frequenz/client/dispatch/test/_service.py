# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Mock classes for the dispatch api.

Useful for testing.
"""
import dataclasses
from dataclasses import dataclass
from datetime import datetime, timezone

import grpc
import grpc.aio

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
from google.protobuf.empty_pb2 import Empty

# pylint: enable=no-name-in-module
from frequenz.client.base.conversion import to_datetime as _to_dt

from .._internal_types import DispatchCreateRequest
from ..types import Dispatch


@dataclass
class FakeService:
    """Dispatch mock service for testing."""

    dispatches: list[Dispatch] = dataclasses.field(default_factory=list)
    """List of dispatches."""

    _last_id: int = 0
    """Last used dispatch id."""

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
                    lambda d: self._filter_dispatch(d, request),
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
            _dispatch_from_request(
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


def _dispatch_from_request(
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
