# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Type wrappers for the generated protobuf messages."""


from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

# pylint: disable=no-name-in-module
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    DispatchCreateRequest as PBDispatchCreateRequest,
)

# pylint: enable=no-name-in-module
from google.protobuf.json_format import MessageToDict

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

    is_active: bool
    """Indicates whether the dispatch is active and eligible for processing."""

    is_dry_run: bool
    """Indicates if the dispatch is a dry run.

    Executed for logging and monitoring without affecting actual component states."""

    payload: dict[str, Any]
    """The dispatch payload containing arbitrary data.

    It is structured as needed for the dispatch operation."""

    recurrence: RecurrenceRule
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
            type=pb_object.type,
            start_time=pb_object.start_time.ToDatetime().replace(tzinfo=timezone.utc),
            duration=timedelta(seconds=pb_object.duration),
            selector=component_selector_from_protobuf(pb_object.selector),
            is_active=pb_object.is_active,
            is_dry_run=pb_object.is_dry_run,
            payload=MessageToDict(pb_object.payload),
            recurrence=RecurrenceRule.from_protobuf(pb_object.recurrence),
        )

    def to_protobuf(self) -> PBDispatchCreateRequest:
        """Convert a dispatch to a protobuf dispatch create request.

        Returns:
            The converted protobuf dispatch create request.
        """
        pb_request = PBDispatchCreateRequest()

        pb_request.microgrid_id = self.microgrid_id
        pb_request.type = self.type
        pb_request.start_time.FromDatetime(self.start_time)
        pb_request.duration = int(self.duration.total_seconds())
        pb_request.selector.CopyFrom(component_selector_to_protobuf(self.selector))
        pb_request.is_active = self.is_active
        pb_request.is_dry_run = self.is_dry_run
        pb_request.payload.update(self.payload)
        pb_request.recurrence.CopyFrom(self.recurrence.to_protobuf())

        return pb_request
