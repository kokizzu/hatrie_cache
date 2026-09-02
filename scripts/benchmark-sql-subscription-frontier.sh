#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkQuerySubscriptionFrontier$' -benchmem -count=5
