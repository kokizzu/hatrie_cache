#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^TestConfigWatchFirstReplayOffsetUsesLogicalRingOrder$' -count=1
