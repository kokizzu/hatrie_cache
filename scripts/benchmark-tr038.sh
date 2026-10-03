#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkT038ConflictEventLog' -benchmem -count=5
