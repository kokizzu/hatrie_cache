#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestLargeEqualityJoinSpillsBoundedHashPartitions|TestLargeEqualityJoinSpillBudgetCleansTemporaryFiles|TestC229ExplicitSpillJoinPolicyPreservesRowsAndCleansFiles)$' -count=1
