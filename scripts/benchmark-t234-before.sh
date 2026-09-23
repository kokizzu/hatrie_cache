#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT234RegularTransactionWriteBaseline$' -benchmem -count=5
