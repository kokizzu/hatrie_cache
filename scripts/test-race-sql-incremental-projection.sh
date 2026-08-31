#!/bin/sh
set -eu

go test -race ./hat/hatSql ./hat/hatCache -run 'Test(IncrementalProjectionRunner|FileProjectionCheckpointStore|SQLJournalProjectionRunner)' -count=1
