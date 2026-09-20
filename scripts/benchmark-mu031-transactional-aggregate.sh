#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU031TransactionalAggregate$' -benchmem -count=5
