#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^(TestExecuteSQLQueryUsesOrderedStreamForMaterializedResult|TestExecuteSQLQueryOrderedStreamRetainsSourceRowBudget|TestExecuteSQLQueryUsesTopNStreamForMaterializedResult|TestExecuteSQLQueryTopNFallsBackWhenOrderIndexIsUnavailable)$' -count=1
