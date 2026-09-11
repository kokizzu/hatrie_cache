#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache \
	-run '^$' \
	-bench '^BenchmarkMZ013(BaselinePersistenceBarrier|SourceCheckpointCommit)$' \
	-benchmem \
	-count=5
