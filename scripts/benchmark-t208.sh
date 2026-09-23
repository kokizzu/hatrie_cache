#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^$' -bench 'BenchmarkTU208' -benchmem -count=5
go test ./hat/hatReplication -run '^$' -bench 'BenchmarkTU208' -benchmem -count=5
