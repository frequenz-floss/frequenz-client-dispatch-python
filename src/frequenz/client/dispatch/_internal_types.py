# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Type wrappers for the generated protobuf messages."""


from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

# pylint: disable=no-name-in-module
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    CreateMicrogridDispatchRequest as PBDispatchCreateRequest,
)

# pylint: enable=no-name-in-module
from google.protobuf.json_format import MessageToDict

from frequenz.client.base.conversion import to_datetime, to_timestamp

from .types import (
    ComponentSelector,
    RecurrenceRule,
    component_selector_from_protobuf,
    component_selector_to_protobuf,
)


# pylint: disable=too-many-instance-attributes
@dataclass(kw_only=True)
class DispatchCreateRequest:
    """Request to create a new dispatch."""

    microgrid_id: int
    """The identifier of the microgrid to which this dispatch belongs."""

    type: str
    """User-defined information about the type of dispatch.

    This is understood and processed by downstream applications."""

    start_time: datetime
    """The start time of the dispatch in UTC."""

    duration: timedelta
    """The duration of the dispatch, represented as a timedelta."""

    selector: ComponentSelector
    """The component selector specifying which components the dispatch targets."""

    active: bool
    """Indicates whether the dispatch is active and eligible for processing."""

    dry_run: bool
    """Indicates if the dispatch is a dry run.

    Executed for logging and monitoring without affecting actual component states."""

    payload: dict[str, Any]
    """The dispatch payload containing arbitrary data.

    It is structured as needed for the dispatch operation."""

    recurrence: RecurrenceRule | None
    """The recurrence rule for the dispatch.

    Defining any repeating patterns or schedules."""

    @classmethod
    def from_protobuf(
        cls, pb_object: PBDispatchCreateRequest
    ) -> "DispatchCreateRequest":
        """Convert a protobuf dispatch create request to a dispatch.

        Args:
            pb_object: The protobuf dispatch create request to convert.

        Returns:
            The converted dispatch.
        """
        return DispatchCreateRequest(
            microgrid_id=pb_object.microgrid_id,
            type=pb_object.dispatch_data.type,
            start_time=rounded_start_time(
                to_datetime(pb_object.dispatch_data.start_time)
            ),
            duration=timedelta(seconds=pb_object.dispatch_data.duration),
            selector=component_selector_from_protobuf(pb_object.dispatch_data.selector),
            active=pb_object.dispatch_data.is_active,
            dry_run=pb_object.dispatch_data.is_dry_run,
            payload=MessageToDict(pb_object.dispatch_data.payload),
            recurrence=RecurrenceRule.from_protobuf(pb_object.dispatch_data.recurrence),
        )

    def to_protobuf(self) -> PBDispatchCreateRequest:
        """Convert a dispatch to a protobuf dispatch create request.

        Returns:
            The converted protobuf dispatch create request.
        """
        pb_request = PBDispatchCreateRequest()

        pb_request.microgrid_id = self.microgrid_id
        pb_request.dispatch_data.type = self.type
        pb_request.dispatch_data.start_time.CopyFrom(to_timestamp(self.start_time))
        pb_request.dispatch_data.duration = round(self.duration.total_seconds())
        pb_request.dispatch_data.selector.CopyFrom(
            component_selector_to_protobuf(self.selector)
        )
        pb_request.dispatch_data.is_active = self.active
        pb_request.dispatch_data.is_dry_run = self.dry_run
        pb_request.dispatch_data.payload.update(self.payload)
        if self.recurrence:
            pb_request.dispatch_data.recurrence.CopyFrom(self.recurrence.to_protobuf())
        else:
            pb_request.dispatch_data.ClearField("recurrence")

        return pb_request


def rounded_start_time(start_time: datetime) -> datetime:
    """Round the start time to the nearest second.

    Args:
        start_time: The start time to round.

    Returns:
        The rounded start time.
    """
    # Round start_time seconds to have the same behavior as the gRPC server
    # https://github.com/frequenz-io/frequenz-service-dispatch/issues/77
    new_seconds = start_time.second + start_time.microsecond / 1_000_000
    start_time = start_time.replace(microsecond=0, second=0)
    start_time += timedelta(seconds=round(new_seconds))
    return start_time
