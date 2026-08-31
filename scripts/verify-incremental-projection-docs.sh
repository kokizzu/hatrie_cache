#!/bin/sh
set -eu

test -f INCREMENTAL_PROJECTIONS.md
rg -q 'INCREMENTAL_PROJECTIONS.md' README.md
rg -q '^## Journal-Driven Incremental Projections$' BENCHMARK.md
rg -q 'NewSQLJournalProjectionRunner' INCREMENTAL_PROJECTIONS.md
rg -q 'FileProjectionCheckpointStore' INCREMENTAL_PROJECTIONS.md
rg -q 'runner.Rebuild' INCREMENTAL_PROJECTIONS.md
