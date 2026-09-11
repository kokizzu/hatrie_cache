#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestPartitionPruningResolverUsesLiteralRangePredicate|TestSQLPartitionPredicateNormalizesLiteralComparisons' -count=1
