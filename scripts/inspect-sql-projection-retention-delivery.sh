#!/bin/sh
set -eu

git diff --check -- INCREMENTAL_PROJECTIONS.md Makefile hat/hatCache/journal.go hat/hatCache/journal_segments.go hat/hatCache/journal_projection_retention_test.go hat/hatCache/sql_incremental_projection.go hat/hatCache/sql_incremental_projection_test.go scripts/format-sql-journal-projection-runner.sh scripts/format-sql-projection-retention.sh scripts/inspect-incremental-projection-docs.sh scripts/inspect-sql-journal-projection-runner.sh scripts/inspect-sql-stream-contracts.sh scripts/test-race-sql-projection-retention.sh scripts/test-sql-projection-retention.sh
git diff --stat -- INCREMENTAL_PROJECTIONS.md Makefile hat/hatCache/journal.go hat/hatCache/journal_segments.go hat/hatCache/journal_projection_retention_test.go hat/hatCache/sql_incremental_projection.go hat/hatCache/sql_incremental_projection_test.go scripts/format-sql-journal-projection-runner.sh scripts/format-sql-projection-retention.sh scripts/inspect-incremental-projection-docs.sh scripts/inspect-sql-journal-projection-runner.sh scripts/inspect-sql-stream-contracts.sh scripts/test-race-sql-projection-retention.sh scripts/test-sql-projection-retention.sh
git diff -- Makefile
