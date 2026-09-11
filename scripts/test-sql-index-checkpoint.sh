#!/bin/sh
set -eu

go test ./hat/hatCache -run 'Test(SQLJSONIndexRebuildCheckpoint|FileSQLJSONIndexRebuildCheckpointStore)' -count=1
