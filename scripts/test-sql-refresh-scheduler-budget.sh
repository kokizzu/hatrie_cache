#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestManagedRefreshScheduler(CycleBudget|RejectsNegativeCycleBudget)' -count=1
