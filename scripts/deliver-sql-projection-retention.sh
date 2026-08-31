#!/bin/sh
set -eu

paths='INCREMENTAL_PROJECTIONS.md Makefile hat/hatCache/journal.go hat/hatCache/journal_segments.go hat/hatCache/journal_projection_retention_test.go hat/hatCache/sql_incremental_projection.go hat/hatCache/sql_incremental_projection_test.go scripts/deliver-sql-projection-retention.sh scripts/format-sql-journal-projection-runner.sh scripts/format-sql-projection-retention.sh scripts/inspect-incremental-projection-docs.sh scripts/inspect-sql-journal-projection-runner.sh scripts/inspect-sql-projection-retention-delivery.sh scripts/inspect-sql-stream-contracts.sh scripts/test-race-sql-projection-retention.sh scripts/test-sql-projection-retention.sh'

if ! git diff --cached --quiet; then
	echo 'refusing to deliver with pre-existing staged changes' >&2
	exit 1
fi

git diff --check -- $paths
git add -- $paths
git diff --cached --check
git commit -m 'feat: protect journal retention for projections'
git push
