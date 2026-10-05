#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench '^BenchmarkConnectionPoolDo' -benchmem -benchtime=250ms -count=5
go test ./hat/hatPeer -run '^$' -bench '^BenchmarkTU47WriteCancellation$' -benchmem -benchtime=500ms -count=5
