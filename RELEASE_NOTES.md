# Frequenz Dispatch Client Library Release Notes

## Summary

This release includes a new feature for pagination support in the dispatch list request as well as usage of the base-client for setting up the channel and client configuration.

## Upgrading

- The `Client.list()` function now yields a `list[Dispatch]` representing one page of dispatches
- `Client.__init__` no longer accepts a `grpc_channel` argument, instead a `server_url` argument is required.
- For the dispatch-cli client, `DISPATCH_API_PORT` and `DISPATCH_API_HOST` environment variables have been replaced with `DISPATCH_API_URL` which should be a full URL including the protocol (e.g. `grpc://fz-0004.frequenz.io:50051`)

## New Features

- Pagination support in the dispatch list request.
- `Client.__init__`:
 - Has a new parameter `connect` which is a boolean that determines if the client should connect to the server on initialization.
 - Automatically sets up the channel for encrypted TLS communication.
- A new method `stream()` to receive dispatch events in real-time.
