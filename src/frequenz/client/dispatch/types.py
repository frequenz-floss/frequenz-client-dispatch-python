# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Type wrappers for the generated protobuf messages."""


from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import IntEnum
from typing import Any, cast

# pylint: disable=no-name-in-module
from frequenz.api.dispatch.v1.dispatch_pb2 import (
    ComponentSelector as PBComponentSelector,
)
from frequenz.api.dispatch.v1.dispatch_pb2 import Dispatch as PBDispatch
from frequenz.api.dispatch.v1.dispatch_pb2 import RecurrenceRule as PBRecurrenceRule
from frequenz.api.dispatch.v1.dispatch_pb2 import StreamMicrogridDispatchesResponse
from google.protobuf.json_format import MessageToDict

from frequenz.client.base.conversion import to_datetime, to_timestamp

# pylint: enable=no-name-in-module
from frequenz.client.common.microgrid.components import ComponentCategory

ComponentSelector = list[int] | list[ComponentCategory]
"""A component selector specifying which components a dispatch targets.

A component selector can be a list of component IDs or a list of categories.
"""


def component_selector_from_protobuf(
    pb_selector: PBComponentSelector,
) -> ComponentSelector:
    """Convert a protobuf component selector to a component selector.

    Args:
        pb_selector: The protobuf component selector to convert.

    Raises:
        ValueError: If the protobuf component selector is invalid.

    Returns:
        The converted component selector.
    """
    match pb_selector.WhichOneof("selector"):
        case "component_ids":
            id_list: list[int] = list(pb_selector.component_ids.ids)
            return id_list
        case "component_categories":
            category_list: list[ComponentCategory] = list(
                map(
                    ComponentCategory.from_proto,
                    pb_selector.component_categories.categories,
                )
            )
            return category_list
        case _:
            raise ValueError("Invalid component selector")


def component_selector_to_protobuf(
    selector: ComponentSelector,
) -> PBComponentSelector:
    """Convert a component selector to a protobuf component selector.

    Args:
        selector: The component selector to convert.

    Raises:
        ValueError: If the component selector is invalid.

    Returns:
        The converted protobuf component selector.
    """
    pb_selector = PBComponentSelector()
    match selector:
        case list(component_ids) if all(isinstance(id, int) for id in component_ids):
            pb_selector.component_ids.ids.extend(cast(list[int], component_ids))
        case list(categories) if all(
            isinstance(cat, ComponentCategory) for cat in categories
        ):
            pb_selector.component_categories.categories.extend(
                map(
                    lambda cat: cat.to_proto(),
                    cast(list[ComponentCategory], categories),
                )
            )
        case _:
            raise ValueError("Invalid component selector")
    return pb_selector


class Weekday(IntEnum):
    """Enum representing the day of the week."""

    UNSPECIFIED = PBRecurrenceRule.WEEKDAY_UNSPECIFIED
    MONDAY = PBRecurrenceRule.WEEKDAY_MONDAY
    TUESDAY = PBRecurrenceRule.WEEKDAY_TUESDAY
    WEDNESDAY = PBRecurrenceRule.WEEKDAY_WEDNESDAY
    THURSDAY = PBRecurrenceRule.WEEKDAY_THURSDAY
    FRIDAY = PBRecurrenceRule.WEEKDAY_FRIDAY
    SATURDAY = PBRecurrenceRule.WEEKDAY_SATURDAY
    SUNDAY = PBRecurrenceRule.WEEKDAY_SUNDAY


class Frequency(IntEnum):
    """Enum representing the frequency of the recurrence."""

    UNSPECIFIED = PBRecurrenceRule.FREQUENCY_UNSPECIFIED
    MINUTELY = PBRecurrenceRule.FREQUENCY_MINUTELY
    HOURLY = PBRecurrenceRule.FREQUENCY_HOURLY
    DAILY = PBRecurrenceRule.FREQUENCY_DAILY
    WEEKLY = PBRecurrenceRule.FREQUENCY_WEEKLY
    MONTHLY = PBRecurrenceRule.FREQUENCY_MONTHLY


@dataclass(kw_only=True)
class EndCriteria:
    """Controls when a recurring dispatch should end."""

    count: int | None = None
    """The number of times this dispatch should recur."""
    until: datetime | None = None
    """The end time of this dispatch in UTC."""

    @classmethod
    def from_protobuf(cls, pb_criteria: PBRecurrenceRule.EndCriteria) -> "EndCriteria":
        """Convert a protobuf end criteria to an end criteria.

        Args:
            pb_criteria: The protobuf end criteria to convert.

        Returns:
            The converted end criteria.
        """
        instance = cls()

        match pb_criteria.WhichOneof("count_or_until"):
            case "count":
                instance.count = pb_criteria.count
            case "until":
                instance.until = to_datetime(pb_criteria.until)
        return instance

    def to_protobuf(self) -> PBRecurrenceRule.EndCriteria:
        """Convert an end criteria to a protobuf end criteria.

        Returns:
            The converted protobuf end criteria.
        """
        pb_criteria = PBRecurrenceRule.EndCriteria()

        if self.count is not None:
            pb_criteria.count = self.count
        elif self.until is not None:
            pb_criteria.until.CopyFrom(to_timestamp(self.until))

        return pb_criteria


# pylint: disable=too-many-instance-attributes
@dataclass(kw_only=True)
class RecurrenceRule:
    """Ruleset governing when and how a dispatch should re-occur.

    Attributes follow the iCalendar specification (RFC5545) for recurrence rules.
    """

    frequency: Frequency = Frequency.UNSPECIFIED
    """The frequency specifier of this recurring dispatch."""

    interval: int = 0
    """How often this dispatch should recur, based on the frequency."""

    end_criteria: EndCriteria | None = None
    """When this dispatch should end.

    Can recur a fixed number of times or until a given timestamp."""

    byminutes: list[int] = field(default_factory=list)
    """On which minute(s) of the hour the event occurs."""

    byhours: list[int] = field(default_factory=list)
    """On which hour(s) of the day the event occurs."""

    byweekdays: list[Weekday] = field(default_factory=list)
    """On which day(s) of the week the event occurs."""

    bymonthdays: list[int] = field(default_factory=list)
    """On which day(s) of the month the event occurs."""

    bymonths: list[int] = field(default_factory=list)
    """On which month(s) of the year the event occurs."""

    @classmethod
    def from_protobuf(cls, pb_rule: PBRecurrenceRule) -> "RecurrenceRule":
        """Convert a protobuf recurrence rule to a recurrence rule.

        Args:
            pb_rule: The protobuf recurrence rule to convert.

        Returns:
            The converted recurrence rule.
        """
        return RecurrenceRule(
            frequency=Frequency(pb_rule.freq),
            interval=pb_rule.interval,
            end_criteria=(
                EndCriteria.from_protobuf(pb_rule.end_criteria)
                if pb_rule.HasField("end_criteria")
                else None
            ),
            byminutes=list(pb_rule.byminutes),
            byhours=list(pb_rule.byhours),
            byweekdays=[Weekday(day) for day in pb_rule.byweekdays],
            bymonthdays=list(pb_rule.bymonthdays),
            bymonths=list(pb_rule.bymonths),
        )

    def to_protobuf(self) -> PBRecurrenceRule:
        """Convert a recurrence rule to a protobuf recurrence rule.

        Returns:
            The converted protobuf recurrence rule.
        """
        pb_rule = PBRecurrenceRule()

        pb_rule.freq = self.frequency.value
        pb_rule.interval = self.interval
        if self.end_criteria is not None:
            pb_rule.end_criteria.CopyFrom(self.end_criteria.to_protobuf())
        pb_rule.byminutes.extend(self.byminutes)
        pb_rule.byhours.extend(self.byhours)
        pb_rule.byweekdays.extend([day.value for day in self.byweekdays])
        pb_rule.bymonthdays.extend(self.bymonthdays)
        pb_rule.bymonths.extend(self.bymonths)

        return pb_rule


@dataclass(frozen=True, kw_only=True)
class TimeIntervalFilter:
    """Filter for a time interval."""

    start_from: datetime | None
    """Filter by start_time >= start_from."""

    start_to: datetime | None
    """Filter by start_time < start_to."""

    end_from: datetime | None
    """Filter by end_time >= end_from."""

    end_to: datetime | None
    """Filter by end_time < end_to."""


@dataclass(kw_only=True, frozen=True)
class Dispatch:
    """Represents a dispatch operation within a microgrid system."""

    id: int
    """The unique identifier for the dispatch."""

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

    recurrence: RecurrenceRule
    """The recurrence rule for the dispatch.

    Defining any repeating patterns or schedules."""

    create_time: datetime
    """The creation time of the dispatch in UTC. Set when a dispatch is created."""

    update_time: datetime
    """The last update time of the dispatch in UTC. Set when a dispatch is modified."""

    @classmethod
    def from_protobuf(cls, pb_object: PBDispatch) -> "Dispatch":
        """Convert a protobuf dispatch to a dispatch.

        Args:
            pb_object: The protobuf dispatch to convert.

        Returns:
            The converted dispatch.
        """
        return Dispatch(
            id=pb_object.metadata.dispatch_id,
            type=pb_object.data.type,
            create_time=to_datetime(pb_object.metadata.create_time),
            update_time=to_datetime(pb_object.metadata.modification_time),
            start_time=to_datetime(pb_object.data.start_time),
            duration=timedelta(seconds=pb_object.data.duration),
            selector=component_selector_from_protobuf(pb_object.data.selector),
            active=pb_object.data.is_active,
            dry_run=pb_object.data.is_dry_run,
            payload=MessageToDict(pb_object.data.payload),
            recurrence=RecurrenceRule.from_protobuf(pb_object.data.recurrence),
        )

    def to_protobuf(self) -> PBDispatch:
        """Convert a dispatch to a protobuf dispatch.

        Returns:
            The converted protobuf dispatch.
        """
        pb_dispatch = PBDispatch()

        pb_dispatch.metadata.dispatch_id = self.id
        pb_dispatch.metadata.create_time.CopyFrom(to_timestamp(self.create_time))
        pb_dispatch.metadata.modification_time.CopyFrom(to_timestamp(self.update_time))
        pb_dispatch.data.type = self.type
        pb_dispatch.data.start_time.CopyFrom(to_timestamp(self.start_time))
        pb_dispatch.data.duration = int(self.duration.total_seconds())
        pb_dispatch.data.selector.CopyFrom(
            component_selector_to_protobuf(self.selector)
        )
        pb_dispatch.data.is_active = self.active
        pb_dispatch.data.is_dry_run = self.dry_run
        pb_dispatch.data.payload.update(self.payload)
        pb_dispatch.data.recurrence.CopyFrom(self.recurrence.to_protobuf())

        return pb_dispatch


class Event(IntEnum):
    """Enum representing the type of event that occurred during a dispatch operation."""

    UNSPECIFIED = StreamMicrogridDispatchesResponse.Event.EVENT_UNSPECIFIED
    CREATED = StreamMicrogridDispatchesResponse.Event.EVENT_CREATED
    UPDATED = StreamMicrogridDispatchesResponse.Event.EVENT_UPDATED
    DELETED = StreamMicrogridDispatchesResponse.Event.EVENT_DELETED


@dataclass(kw_only=True, frozen=True)
class DispatchEvent:
    """Represents an event that occurred during a dispatch operation."""

    dispatch: Dispatch
    """The dispatch associated with the event."""

    event: Event
    """The type of event that occurred."""

    @classmethod
    def from_protobuf(
        cls, pb_object: StreamMicrogridDispatchesResponse
    ) -> "DispatchEvent":
        """Convert a protobuf dispatch event to a dispatch event.

        Args:
            pb_object: The protobuf dispatch event to convert.

        Returns:
            The converted dispatch event.
        """
        return DispatchEvent(
            dispatch=Dispatch.from_protobuf(pb_object.dispatch),
            event=Event(pb_object.event),
        )
