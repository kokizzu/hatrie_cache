#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatReplication -run '^$' -bench '^BenchmarkTT006' -benchmem -count=5
