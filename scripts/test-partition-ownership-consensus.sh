#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^(TestEvaluatePartitionOwnershipConsensus|TestPartitionOwnershipConsensus|TestValidatePartitionOwnershipConsensusDecision)' -count=1
