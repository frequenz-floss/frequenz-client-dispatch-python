# Frequenz Dispatch Client Library Release Notes

## Summary

## New Features

* `dispatch-cli` supports now the parameter `--type` and `--running` to filter the list of running services by type and status, respectively.
* Every call now has a default timeout of 60 seconds, streams terminate after five minutes. This can be influenced by the two new parameters for`DispatchApiClient.__init__()`:
    * `default_timeout: timedelta` (default: 60 seconds)
    * `stream_timeout: timedelta` (default: 5 minutes)

* `frequenz.client.dispatch.TargetComponents` is now public, and its type has changed from `list[int] | list[ComponentCategory]` to `list[ComponentId] | list[ComponentCategory]`. This change introduces a new dependency on `frequenz-client-microgrid` (>= v0.7.0, < 0.8.0) for the `frequenz.client.microgrid.ComponentId` type.
