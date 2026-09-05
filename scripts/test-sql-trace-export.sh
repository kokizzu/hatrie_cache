#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestQueryTraceRecorder' -count=1
