# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Fake client for testing."""

from typing import Any, cast

import grpc.aio

from frequenz.client.dispatch import Client
from frequenz.client.dispatch.test._service import FakeService
from frequenz.client.dispatch.types import Dispatch


class FakeClient(Client):
    """Fake client for testing.

    This client uses a fake service to simulate the dispatch api.
    """

    def __init__(self) -> None:
        """Initialize the mock client."""
        super().__init__(grpc.aio.insecure_channel("mock"), "mock")
        self._stub = FakeService()  # type: ignore


def to_create_params(dispatch: Dispatch) -> dict[str, Any]:
    """Convert a dispatch to client.create parameters.

    Args:
        dispatch: The dispatch to convert.

    Returns:
        dict[str, Any]: The create parameters.
    """
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
