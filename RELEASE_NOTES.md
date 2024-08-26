# Frequenz Dispatch Client Library Release Notes

## Summary

This release includes a new feature for pagination support in the dispatch list request as well as usage of the base-client for setting up the channel and client configuration.

## Upgrading

- The `Client.list()` function now yields a `list[Dispatch]` representing one page of dispatches
- `Client.__init__` no longer accepts a `grpc_channel` argument, instead a `server_url` argument is required.

## New Features

- Pagination support in the dispatch list request.
- `Client.__init__`:
 - Has a new parameter `connect` which is a boolean that determines if the client should connect to the server on initialization.
 - Automatically sets up the channel for encrypted TLS communication.
