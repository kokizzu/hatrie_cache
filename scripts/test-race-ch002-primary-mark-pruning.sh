#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql ./hat/hatCache -run '^TestSQLOrderedRangePruning' -count=1
