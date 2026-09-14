#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache \
  -run '^$' \
  -bench '^BenchmarkTR038SQLTransactionTimeoutGuard$' \
  -benchmem \
  -count=5
