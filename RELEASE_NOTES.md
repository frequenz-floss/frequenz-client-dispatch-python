# Frequenz Dispatch Client Library Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* `Client.stream()` will now raise an Exception when the connection is lost.
* `Client.create()` and `Client.list() now take all optional arguments as keyword-only arguments.

## New Features

* Added support for duration=None when creating a dispatch.
* The `FakeService` now supports the `stream()` method.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
