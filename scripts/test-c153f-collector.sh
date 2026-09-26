#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^TestCollectPartitionOwnershipConsensus' -count=1 -v
