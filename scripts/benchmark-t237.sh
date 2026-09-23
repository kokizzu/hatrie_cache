#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench '^BenchmarkT237ConnectionPool(Baseline|HealthCheck)$' -benchmem -count=5
