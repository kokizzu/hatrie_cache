#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^$' -bench '^BenchmarkPartitionOwnershipConsensus/(fingerprint_only|ownership_metadata)$' -benchmem -count=5 -cpu=1
