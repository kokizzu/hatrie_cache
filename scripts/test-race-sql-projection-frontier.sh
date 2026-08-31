#!/bin/sh
set -eu

go test -race ./hat/hatCache -run '^(TestSQLJournalProjectionRunner|TestSQLProjectionRetentionFrontier)' -count=1
