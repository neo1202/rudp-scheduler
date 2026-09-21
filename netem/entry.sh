#!/bin/sh
# Impair this container's outgoing packets with the kernel's netem qdisc, then
# run the given command. Every container does this, so loss and delay apply
# independently to each direction, the same model internal/lossy simulates.
set -e
if [ -n "$NETEM" ]; then
  # shellcheck disable=SC2086
  tc qdisc add dev eth0 root netem $NETEM
fi
exec "$@"
