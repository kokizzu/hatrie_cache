#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'TimestampOracle|GlobalTimestampOracle' -benchmem -count=5
