#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkQuerySubscriptionsStatus$' -benchmem -count=5
