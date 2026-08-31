#!/bin/sh
set -eu

gofmt -w hat/hatCache/journal.go hat/hatCache/journal_segments.go hat/hatCache/journal_projection_retention_test.go hat/hatCache/sql_incremental_projection.go
