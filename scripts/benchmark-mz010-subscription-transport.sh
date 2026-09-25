#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMZ010SQLSubscriptionWireTransport' -benchmem -benchtime=200ms -count=1
