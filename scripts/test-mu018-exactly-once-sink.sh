#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'Test(SQLSinkExactlyOnce|FileSQLSinkExactlyOnce)' -count=1
