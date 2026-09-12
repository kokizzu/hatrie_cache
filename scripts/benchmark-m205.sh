#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkQuerySubscriptionDeltaBatchOrder$' -benchmem -count=5
