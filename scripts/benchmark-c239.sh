#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench 'BenchmarkC239Compaction' -benchmem -count=5
