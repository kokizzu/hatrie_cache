#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu47 ./hat/hatSql -run '^$' -bench '^BenchmarkQuerySubscriptionProgressFrame' -benchmem -count=5
