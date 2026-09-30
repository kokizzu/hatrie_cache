#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestTypedTableCompositeSparsePrimaryMarkCacheRetainsTupleMarks$' -count=1
