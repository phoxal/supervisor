#!/bin/sh
# The supervisor passes --bundle-root followed by the isolated bundle root.
printf '%s\n' ready > "$2/child-ready"
exec sleep 30
