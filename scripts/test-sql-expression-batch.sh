#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQL(BatchLeafPredicatePreservesNullAndLiteralValues|GroupRowsWithoutAggregatePreservesInputRows)$' -count=1
