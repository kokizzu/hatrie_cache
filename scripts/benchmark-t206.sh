#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkT206(PrepareAndAbort|Snapshot128Members)$' -benchmem -count=5
