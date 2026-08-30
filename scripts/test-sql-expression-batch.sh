#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLBatchLeafPredicatePreservesNullAndLiteralValues$' -count=1
