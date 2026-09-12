#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication \
  -run '^TestChangefeedCheckpoint(RoundTripBindsSourceAndAdvances|RejectsMalformedFrames)$' \
  -count=1
