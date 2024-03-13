# Frequenz Dispatch Client Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- A new module `frequenz.client.dispatch.test` has been added, providing a fake Service and Client as well as a `DispatchGenerator` to generate `Dispatch` instances filled with random data.
- The `DispatchGenerator.generate_dispatch` method now accepts a `microgrid_id` parameter to generate `Dispatch` instances with a specific microgrid ID.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
