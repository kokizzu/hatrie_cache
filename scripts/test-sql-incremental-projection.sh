#!/bin/sh
set -eu

go test ./hat/hatSql ./hat/hatCache -run 'Test(IncrementalProjectionRunner|SQLJournalProjectionRunner)' -count=1
