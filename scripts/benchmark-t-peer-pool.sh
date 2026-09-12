#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench '^BenchmarkConnectionPool' -benchmem -count=5
