# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Fake client for testing."""

from typing import Any, cast
from unittest.mock import MagicMock

from .. import Client
from ..types import Dispatch
from ._service import ALL_KEY, NONE_KEY, FakeService

__all__ = ["FakeClient", "to_create_params", "ALL_KEY", "NONE_KEY"]


class FakeClient(Client):
    """Fake client for testing.

    This client uses a fake service to simulate the dispatch api.
    """

    def __init__(
        self,
        shuffle_after_create: bool = False,
    ) -> None:
        """Initialize the mock client.

        Args:
            shuffle_after_create: Whether to shuffle the dispatches after creating them.
        """
        super().__init__(grpc_channel=MagicMock(), svc_addr="mock", key=ALL_KEY)
        self._stub = FakeService()  # type: ignore
        self._service._shuffle_after_create = shuffle_after_create

    @property
    def dispatches(self) -> list[Dispatch]:
        """List of dispatches.

        Returns:
            list[Dispatch]: The list of dispatches
        """
        return self._service.dispatches

    @dispatches.setter
    def dispatches(self, value: list[Dispatch]) -> None:
        """Set the list of dispatches.

        Args:
            value: The list of dispatches to set.
        """
        self._service.dispatches = value

    @property
    def _service(self) -> FakeService:
        """The fake service.

        Returns:
            FakeService: The fake service.
        """
        return cast(FakeService, self._stub)


def to_create_params(dispatch: Dispatch) -> dict[str, Any]:
    """Convert a dispatch to client.create parameters.

    Args:
        dispatch: The dispatch to convert.

    Returns:
        dict[str, Any]: The create parameters.
    """
    return {
        "microgrid_id": dispatch.microgrid_id,
        "type": dispatch.type,
        "start_time": dispatch.start_time,
        "duration": dispatch.duration,
        "selector": dispatch.selector,
        "active": dispatch.active,
        "dry_run": dispatch.dry_run,
        "payload": dispatch.payload,
        "recurrence": dispatch.recurrence,
    }
