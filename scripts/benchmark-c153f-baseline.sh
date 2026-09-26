#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^$' -bench '^BenchmarkPartitionOwnershipConsensusCollectionBaseline$' -benchmem -count=5
