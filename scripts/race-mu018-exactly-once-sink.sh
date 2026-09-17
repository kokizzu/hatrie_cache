#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'Test(SQLSinkExactlyOnce|FileSQLSinkExactlyOnce)' -count=1
