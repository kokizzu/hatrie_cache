#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatTopology -run '^(TestEvaluatePartitionOwnershipConsensus|TestPartitionOwnershipConsensus|TestValidatePartitionOwnershipConsensusDecision)' -count=1
go vet ./hat/hatTopology
git diff --check
