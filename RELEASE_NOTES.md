# Frequenz Dispatch Client Library Release Notes

## Summary

The client was updated to the latest API version and supports most of its features.

## Upgrading

All commands now require the `microgrid_id` parameter.

## New Features

* Added a new option to allow insecure connections, can be set using the flag "--insecure" or the environment variable `FREQUENZ_INSECURE` (default is `false`).
* The update command now returns the modified dispatch.
* We use our own root CA certificate to verify the server's certificate until we have a proper certificate chain.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
