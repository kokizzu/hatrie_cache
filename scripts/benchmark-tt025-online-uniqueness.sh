#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench '^(BenchmarkTT021OnlineSecondaryIndexBuild|BenchmarkTT025)' -benchmem -count=5
