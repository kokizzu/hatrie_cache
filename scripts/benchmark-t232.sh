#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkT232(SpacePutBaseline|SequentialBatchBaseline|SpaceTransaction|TransactionalBatch)$' \
	-benchmem \
	-count=5
