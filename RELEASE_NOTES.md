# Frequenz Dispatch Client Library Release Notes

## Summary

This is a patch release that updates the default host to use the FQDN instead of the IP address.

## Bug Fixes

* The default host was updated to use the FQDN instead of the IP address. This is required as the client validates the host name against the certificate.
