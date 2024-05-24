# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Test the dispatch CLI."""

from datetime import datetime, timedelta, timezone
from typing import Any, Generator
from unittest.mock import patch

import pytest
from asyncclick.testing import CliRunner
from tzlocal import get_localzone

from frequenz.client.common.microgrid.components import ComponentCategory
from frequenz.client.dispatch.__main__ import cli
from frequenz.client.dispatch.test.client import FakeClient
from frequenz.client.dispatch.types import Dispatch, RecurrenceRule


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for CLI Runner."""
    return CliRunner()


@pytest.fixture
def fake_client() -> FakeClient:
    """Fixture for Fake Client."""
    return FakeClient()


@pytest.fixture(autouse=True)
def mock_get_client(fake_client: FakeClient) -> Generator[None, None, None]:
    """Fixture to mock get_client with FakeClient."""
    with patch(
        "frequenz.client.dispatch.__main__.get_client", return_value=fake_client
    ):
        yield


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatches, microgrid_id, expected_output, expected_return_code",
    [
        (
            [
                Dispatch(
                    id=1,
                    microgrid_id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    selector=[1, 2, 3],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            1,
            "1 dispatches total.",
            0,
        ),
        ([], 1, "0 dispatches total.", 0),
        (
            [
                Dispatch(
                    id=2,
                    microgrid_id=2,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    selector=[1, 2, 3],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            1,
            "0 dispatches total.",
            0,
        ),
        (
            [
                Dispatch(
                    id=1,
                    microgrid_id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    selector=[1, 2, 3],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                ),
                Dispatch(
                    id=2,
                    microgrid_id=2,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    selector=[1, 2, 3],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                ),
            ],
            1,
            "1 dispatches total.",
            0,
        ),
        (
            [],
            "x",
            "Error: Invalid value for 'MICROGRID_ID': 'x' is not a valid integer.",
            2,
        ),
    ],
)
async def test_list_command(  # pylint: disable=too-many-arguments
    runner: CliRunner,
    fake_client: FakeClient,
    dispatches: list[Dispatch],
    microgrid_id: int,
    expected_output: str,
    expected_return_code: int,
) -> None:
    """Test the list command."""
    fake_client.dispatches = dispatches
    result = await runner.invoke(cli, ["list", str(microgrid_id)])
    assert result.exit_code == expected_return_code
    assert expected_output in result.output


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args, expected_microgrid_id, expected_type, "
    "expected_start_time_delta, expected_duration, "
    "expected_selector, expected_return_code",
    [
        (
            ["create", "1", "test", "in 1 hour", "1h", "BATTERY"],
            1,
            "test",
            timedelta(hours=1),
            timedelta(seconds=3600),
            ComponentCategory.BATTERY,
            0,
        ),
        (
            ["create", "1", "test", "in 2 hours", "1 hour", "1,2,3"],
            1,
            "test",
            timedelta(hours=2),
            timedelta(seconds=3600),
            [1, 2, 3],
            0,
        ),
        (
            ["create", "x"],
            0,
            "",
            timedelta(),
            timedelta(),
            [],
            2,
        ),
    ],
)
async def test_create_command(  # pylint: disable=too-many-arguments
    runner: CliRunner,
    fake_client: FakeClient,
    args: list[str],
    expected_microgrid_id: int,
    expected_type: str,
    expected_start_time_delta: timedelta,
    expected_duration: timedelta,
    expected_selector: list[int] | ComponentCategory,
    expected_return_code: int,
) -> None:
    """Test the create command."""
    start_time = (datetime.now(get_localzone()) + expected_start_time_delta).astimezone(
        timezone.utc
    )
    result = await runner.invoke(cli, args)

    assert result.exit_code == expected_return_code
    assert "id" in result.output

    if expected_return_code != 0:
        assert len(fake_client.dispatches) == 0
        return

    assert len(fake_client.dispatches) == 1
    created_dispatch = fake_client.dispatches[0]
    assert created_dispatch.microgrid_id == expected_microgrid_id
    assert created_dispatch.type == expected_type
    assert created_dispatch.start_time.timestamp() == pytest.approx(
        start_time.timestamp(), abs=2
    )
    assert created_dispatch.duration.total_seconds() == pytest.approx(
        expected_duration.total_seconds(), abs=2
    )
    assert created_dispatch.selector == expected_selector


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatches, field, value, update_field, update_value, expected_return_code, expected_output",
    [
        (
            [
                Dispatch(
                    id=1,
                    microgrid_id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    selector=ComponentCategory.BATTERY,
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            "--duration",
            "7200",
            "duration",
            timedelta(seconds=7200),
            0,
            "Dispatch updated.",
        ),
        (
            [
                Dispatch(
                    id=1,
                    microgrid_id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    selector=ComponentCategory.BATTERY,
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            "--active",
            "False",
            "active",
            False,
            0,
            "Dispatch updated.",
        ),
        (
            [
                Dispatch(
                    id=1,
                    microgrid_id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    selector=[500, 501],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            "--selector",
            "400, 401",
            "selector",
            [400, 401],
            0,
            "Dispatch updated.",
        ),
        (
            [],
            "--duration",
            "frankly my dear, I don't give a damn",
            "",
            None,
            2,
            "Error: Invalid value for '--duration': Could not parse time expression",
        ),
    ],
)
async def test_update_command(  # pylint: disable=too-many-arguments
    runner: CliRunner,
    fake_client: FakeClient,
    dispatches: list[Dispatch],
    field: str,
    value: str,
    update_field: str,
    update_value: Any,
    expected_return_code: int,
    expected_output: str,
) -> None:
    """Test the update command."""
    fake_client.dispatches = dispatches
    result = await runner.invoke(cli, ["update", "1", field, value])
    assert result.exit_code == expected_return_code
    assert expected_output in result.output
    if expected_return_code == 0:
        assert getattr(fake_client.dispatches[0], update_field) == update_value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatches, dispatch_id, expected_in_output",
    [
        (
            [
                Dispatch(
                    id=1,
                    microgrid_id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    selector=[1, 2, 3],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            1,
            "Dispatch(id=1,",
        ),
        ([], 999, "Error"),
        (
            [],
            "x",
            "Error: Invalid value for '[DISPATCH_IDS]...': 'x' is not a valid integer.",
        ),
    ],
)
async def test_get_command(
    runner: CliRunner,
    fake_client: FakeClient,
    dispatches: list[Dispatch],
    dispatch_id: int,
    expected_in_output: str,
) -> None:
    """Test the get command."""
    fake_client.dispatches = dispatches
    result = await runner.invoke(cli, ["get", str(dispatch_id)])
    assert result.exit_code == 0 if dispatches else 1
    assert expected_in_output in result.output


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatches, dispatch_id, expected_output, expected_return_code",
    [
        (
            [
                Dispatch(
                    id=1,
                    microgrid_id=1,
                    type="test",
                    start_time=datetime(2023, 1, 1, 0, 0, 0),
                    duration=timedelta(seconds=3600),
                    selector=[1, 2, 3],
                    active=True,
                    dry_run=False,
                    payload={},
                    recurrence=RecurrenceRule(),
                    create_time=datetime(2023, 1, 1, 0, 0, 0),
                    update_time=datetime(2023, 1, 1, 0, 0, 0),
                )
            ],
            1,
            "Dispatches deleted: [1]",
            0,
        ),
        ([], 999, "Error", 1),
        (
            [],
            "x",
            "Error: Invalid value for '[DISPATCH_IDS]...': Invalid integer",
            2,
        ),
    ],
)
async def test_delete_command(  # pylint: disable=too-many-arguments
    runner: CliRunner,
    fake_client: FakeClient,
    dispatches: list[Dispatch],
    dispatch_id: int,
    expected_output: str,
    expected_return_code: int,
) -> None:
    """Test the delete command."""
    fake_client.dispatches = dispatches
    result = await runner.invoke(cli, ["delete", str(dispatch_id)])
    assert result.exit_code == expected_return_code
    assert expected_output in result.output
    if dispatches:
        assert len(fake_client.dispatches) == 0
