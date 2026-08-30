#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQL(BatchLeafPredicatePreservesNullAndLiteralValues|GroupRowsWithoutAggregatePreservesInputRows|SimpleFieldLiteralPredicateMatchesBatchEvaluator)$' -count=1
