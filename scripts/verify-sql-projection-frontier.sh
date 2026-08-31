#!/bin/sh
set -eu

go test ./hat/hatCache -run '^(TestSQLJournalProjectionRunner|TestSQLProjectionRetentionFrontier)' -count=1
go vet ./hat/hatCache
