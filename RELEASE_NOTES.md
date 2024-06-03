# Frequenz Dispatch Client Library Release Notes

## Summary

Most notable feature is the addion of the CLI/REPL client.

## New Features

* A CLI/REPL client was added. Run it using `python -m frequenz.client.dispatch` or just `dispatch-cli` if the package was installed in your path. Use the `--help` parameter to get an overview of the available commands.

## Bug Fixes

* Fixed that client.update() was ignoring updates to "payload".
