#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkExecuteReadQuorumFastPathC209$' -benchmem -count=3
