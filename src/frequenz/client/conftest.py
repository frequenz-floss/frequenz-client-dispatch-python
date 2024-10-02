# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Validate docstring code examples.

Code examples are often wrapped in triple backticks (```) within docstrings.
This plugin extracts these code examples and validates them using pylint.
"""

from frequenz.repo.config.pytest import examples
from sybil import Sybil

sybil_arguments = examples.get_sybil_arguments()
# Upstream includes "excludes" to work around a bug in Sybil.
# This bug seems to be fixed in our version of Sybil, so we remove it.
sybil_arguments.pop("excludes")

pytest_collect_file = Sybil(**sybil_arguments).pytest()
