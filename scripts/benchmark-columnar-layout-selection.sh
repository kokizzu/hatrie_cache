#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench BenchmarkColumnarBatchLayoutSelection -benchmem ./hat/hatSql
