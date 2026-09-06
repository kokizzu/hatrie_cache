#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench BenchmarkMergeColumnarParts -benchmem ./hat/hatSql
