#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLTextPrefixScanVsIndex$' -benchmem -count=5
