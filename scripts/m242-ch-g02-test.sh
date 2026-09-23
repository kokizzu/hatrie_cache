#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run 'TestLargeEqualityJoinSpillsBoundedHashPartitions|TestLargeEqualityJoinSpillBudgetCleansTemporaryFiles' -count=1
