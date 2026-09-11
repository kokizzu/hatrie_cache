#!/bin/sh
set -eu

go test -race ./hat/hatCache -run 'Test(SQLJSONIndexRebuild(Checkpoint|Progress)|FileSQLJSONIndexRebuildCheckpointStore)' -count=1
