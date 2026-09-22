#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestC212|TestC213|TestExactIndexJoinPlan|TestInnerJoinPushesRightOnlyRangePredicateIntoIndex|TestRuntimeJoinBloomFilter)' -count=1
