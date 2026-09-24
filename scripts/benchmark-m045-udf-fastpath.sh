#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkM045UDFLiteralBatch$' -benchmem -count=5
