#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatTopology -run '^TestCollectPartitionOwnershipConsensus' -count=1
