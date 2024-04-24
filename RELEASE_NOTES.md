# Frequenz Dispatch Client Library Release Notes

## Summary

This release contains a few breaking changes that were required to enhance the
API.

## Upgrading

- The `Dispatch` class is now a frozen dataclass, meaning that it is immutable. Modifications can still be done using `replace`: `dispatch = replace(dispatch, start_time=new_start_time)`.
- The `Client.update()` method now requires named parameters.
- `Client.update()` no longer accepts changes to the `type` and `dry_run` fields.

## New Features

- A new module `frequenz.client.dispatch.test` has been added, providing a fake Service and Client as well as a `DispatchGenerator` to generate `Dispatch` instances filled with random data.
- The `DispatchGenerator.generate_dispatch` method now accepts a `microgrid_id` parameter to generate `Dispatch` instances with a specific microgrid ID.
- The `Client.create()` method now returns the newly created `Dispatch` object.
