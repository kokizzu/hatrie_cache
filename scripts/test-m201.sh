#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication \
  -run '^TestChangefeedFrontier(EmitsMonotonicProgress|ConcurrentAdvanceKeepsMaximum)$' \
  -count=1
