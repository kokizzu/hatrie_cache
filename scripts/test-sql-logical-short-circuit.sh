#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLLogicalBatchShortCircuit' -count=1
