# Frequenz Dispatch Client Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->

- `FakeService`: An update whose field mask referenced an unknown path was silently ignored, while the real service rejects it. It now raises an `INVALID_ARGUMENT` error too, so tests can no longer pass against updates that would fail in production. Tests relying on the old behaviour need to drop the unknown paths.
- `FakeService`: An update whose field mask contained a bare `recurrence` path, which the real service accepts to replace the whole recurrence rule, crashed with an `IndexError`. It now replaces the rule.
