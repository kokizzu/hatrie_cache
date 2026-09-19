#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench '^BenchmarkM050MarshalAndPlan$' -benchmem -count=5 ./hat/hatBackup
