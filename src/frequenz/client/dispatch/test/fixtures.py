# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Useful fixtures for testing."""

from pytest import fixture

from ..types import Dispatch
from .client import FakeClient
from .generator import DispatchGenerator


@fixture
def sample(generator: DispatchGenerator) -> Dispatch:
    """Return a dispatch sample."""
    return generator.generate_dispatch()


@fixture
def generator() -> DispatchGenerator:
    """Return a dispatch generator."""
    return DispatchGenerator()


@fixture
def client() -> FakeClient:
    """Return a fake client."""
    return FakeClient()
