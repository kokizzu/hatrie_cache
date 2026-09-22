#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkT248(BaseNackCycle|RetryingNackCycle|RetryingDeadLetterCycle|BaseActive256LeaseAck|RetryingActive256LeaseAck)$' \
	-benchmem \
	-count=5
