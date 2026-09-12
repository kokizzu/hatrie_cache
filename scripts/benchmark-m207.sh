#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^(BenchmarkDebeziumChangefeedApply|BenchmarkQuerySubscriptionDifferentialPayload)$' -benchmem -count=5
