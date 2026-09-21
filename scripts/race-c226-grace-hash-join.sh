#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestLargeEqualityJoinSpillsBoundedHashPartitions|TestLargeEqualityJoinSpillBudgetCleansTemporaryFiles|TestC229ExplicitSpillJoinPolicyPreservesRowsAndCleansFiles)$' -count=1
