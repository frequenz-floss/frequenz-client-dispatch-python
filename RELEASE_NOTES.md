# Frequenz Dispatch Client Library Release Notes

## Summary

Raise `ValueError` when `start_time` passed to `create()` is neither a `datetime` nor `"NOW"`.

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

- `TargetCategories` now supports the `STEAM_BOILER` component category. This
  requires `frequenz-client-common` 0.3.8 or newer. The minimum supported
  `grpcio` version is now 1.80.0, as required by its `frequenz-api-common`
  dependency.
- `dispatch-cli` accepts `FREQUENZ_API_KEY` and `FREQUENZ_API_SECRET` as a
  fallback pair for `DISPATCH_API_AUTH_KEY` and `DISPATCH_API_SIGN_SECRET`.

## Bug Fixes

- `DispatchApiClient.create()`: Passing an invalid `start_time` (not a `datetime` or `"NOW"`) previously silently created a dispatch with an epoch timestamp (1970-01-01). It now raises `ValueError` immediately.
