#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQL(BatchLeafPredicatePreservesNullAndLiteralValues|GroupRowsWithoutAggregatePreservesInputRows|SimpleFieldLiteralPredicateMatchesBatchEvaluator|QueryRowsMatchesMaterializedSimpleFilter|StreamSimpleFieldLiteralExpressionMatchesBatch)$' -count=1
