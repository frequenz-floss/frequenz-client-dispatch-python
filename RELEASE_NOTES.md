# Frequenz Dispatch Client Library Release Notes

## Summary

Require `frequenz-client-base` 0.11.3 so configured HTTP/2 keepalive settings
work, and clarify deprecations in the API reference.

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- Require `frequenz-client-base` 0.11.3 or newer so gRPC applies the configured
  HTTP/2 keepalive intervals.

## Documentation

- The API reference now highlights the deprecated `DispatchApiClient` `key`
  argument and `InverterType.SOLAR` member, including their replacements.
