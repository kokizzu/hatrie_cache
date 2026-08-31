#!/bin/sh
set -eu

rg -n -C 20 'SQLJournalProjectionRunner|IncrementalProjectionRunner|RunOnce|return runner\.runner\.Apply' hat/hatCache/sql_incremental_projection.go
rg -n -C 16 'projectionJournalTail|SQLJournalProjectionRunner|RunOnce' hat/hatCache/sql_incremental_projection_test.go
rg -n -C 12 'SaveSnapshot|ProjectionWatermark|Tail' hat/hatCache/journal_projection_retention_test.go
