#!/usr/bin/env bash
set -euo pipefail

export GOCACHE=/tmp/hatrie-tu19-go-cache
go test ./hat/hatDataStructure -run '^$' -bench 'BenchmarkTU19Compare' -benchmem -count=5
go test ./hat/hatDataStructure -run '^TestTU19CompareWireSize$' -v
