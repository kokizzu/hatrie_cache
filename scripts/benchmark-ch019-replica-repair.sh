#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkCH019' -benchmem -count=3
