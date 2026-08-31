#!/bin/sh
set -eu

go vet ./...
go test -race ./hat/hatCache -run '^(TestSQLJournalProjectionRunner|TestSQLProjectionRetentionFrontier)' -count=1
