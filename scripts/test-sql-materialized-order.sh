#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^(TestExecuteSQLQueryUsesOrderedStreamForMaterializedResult|TestExecuteSQLQueryOrderedStreamRetainsSourceRowBudget)$' -count=1
