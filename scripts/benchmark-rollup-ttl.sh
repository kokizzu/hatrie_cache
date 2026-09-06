#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench BenchmarkTimeBucketRollupExpireBefore -benchmem ./hat/hatSql
