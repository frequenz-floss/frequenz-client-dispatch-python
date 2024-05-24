# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Types for the CLI client."""

from datetime import datetime, timedelta, timezone
from typing import Any, cast

import asyncclick as click
import parsedatetime  # type: ignore
from tzlocal import get_localzone

from frequenz.client.common.microgrid.components import ComponentCategory

# Disable a false positive from pylint
# pylint: disable=inconsistent-return-statements


class FuzzyDateTime(click.ParamType):
    """Try to parse a string as a datetime.

    The parser is very permissive and can handle a wide range of time expressions.
    """

    name = "FuzzyDateTime"

    def __init__(self) -> None:
        """Initialize the parser."""
        self.cal: Any = parsedatetime.Calendar()
        self.local_tz = get_localzone()

    def convert(
        self, value: Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> datetime:
        """Convert the value to a datetime object."""
        if isinstance(value, datetime):
            return value

        try:
            parsed_dt, parse_status = self.cal.parseDT(value, tzinfo=self.local_tz)
            if parse_status == 0:
                self.fail(f"Invalid time expression: {value}", param, ctx)

            return cast(datetime, parsed_dt.astimezone(timezone.utc))
        except Exception as e:  # pylint: disable=broad-except
            self.fail(
                f"Could not parse time expression: '{value}'. Error: {e}", param, ctx
            )


class FuzzyTimeDelta(click.ParamType):
    """Try to parse a string as a timedelta.

    Uses parsedatetime and tries to parse it as relative datetime first that
    is then converted to a timedelta.

    If that fails, try to interpret the string as a number of seconds.

    Only returns timedeltas rounded to the nearest second.
    """

    name = "FuzzyTimeDelta"

    def __init__(self) -> None:
        """Initialize the parser."""
        self.cal: Any = parsedatetime.Calendar()
        self.local_tz = get_localzone()

    def convert(
        self, value: Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> timedelta:
        """Convert the value to a timedelta object."""
        if isinstance(value, timedelta):
            return value

        try:
            parsed_dt, parse_status = self.cal.parseDT(value, tzinfo=self.local_tz)
            if parse_status == 0:
                self.fail(f"Invalid time expression: {value}", param, ctx)

            td = cast(timedelta, parsed_dt - datetime.now(self.local_tz))

            # Round to the nearest second
            return timedelta(seconds=round(td.total_seconds()))
        except click.ClickException as e:
            try:
                return timedelta(seconds=int(value))
            except ValueError:
                self.fail(
                    f"Could not parse time expression: '{value}'. Error: {e}",
                    param,
                    ctx,
                )


class FuzzyIntRange(click.ParamType):
    """Try to parse a string as a simple integer range.

    Possible formats:
    - "1"
    - "1,2,3"
    - "1-3"
    - "1..3"
    """

    name = "int_range"

    def convert(
        self, value: Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> list[int]:
        """Convert the value to a list of integers."""
        if isinstance(value, list):
            return value

        try:
            if "," in value:
                return [int(id) for id in value.split(",")]

            if "-" in value:
                start, end = [int(id) for id in value.split("-")]
                return list(range(start, end + 1))

            if ".." in value:
                start, end = [int(id) for id in value.split("..")]
                return list(range(start, end + 1))

            return [int(value)]
        except ValueError:
            self.fail(f"Invalid integer range: {value}", param, ctx)


class SelectorParamType(click.ParamType):
    """Click parameter type for selectors."""

    name = "selector"

    def convert(
        self, value: Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> ComponentCategory | list[int]:
        """Convert the input value into a ComponentCategory or a list of IDs.

        Args:
            value: The input value (string or list).
            param: The Click parameter object.
            ctx: The Click context object.

        Returns:
            A ComponentCategory if the value is a valid category name,
            otherwise a list of integers if the value is a comma-separated list of IDs.
        """
        if isinstance(value, ComponentCategory):  # Already a ComponentCategory
            return value

        # Attempt to parse as a ComponentCategory
        try:
            return ComponentCategory[value.upper()]
        except KeyError:
            pass

        # If not a valid category, try parsing as a list of IDs
        try:
            return [int(id) for id in value.split(",")]
        except ValueError:
            self.fail(
                f"Invalid component category or ID list: {value}. "
                "Possible categories: BATTERY, GRID, METER, INVERTER, EV_CHARGER, CHP ",
                param,
                ctx,
            )
