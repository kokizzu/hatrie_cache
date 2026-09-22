#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM064BaselineRecursiveDataflow$' -benchmem -count=3
