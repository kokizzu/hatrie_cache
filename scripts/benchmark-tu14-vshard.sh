#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^$' -bench 'BenchmarkTU14' -benchmem -count=5
