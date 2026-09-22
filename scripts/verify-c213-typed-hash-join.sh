#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestC212|TestC213|TestExactIndexJoinPlan|TestInnerJoinPushesRightOnlyRangePredicateIntoIndex|TestRuntimeJoinBloomFilter)' -count=1
