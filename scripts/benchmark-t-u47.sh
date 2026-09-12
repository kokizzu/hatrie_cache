#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench '^BenchmarkConnectionPoolDo' -benchmem -benchtime=250ms -count=5
