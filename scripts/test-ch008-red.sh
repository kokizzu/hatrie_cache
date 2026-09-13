#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestSQLResultCacheSettingsFingerprintSeparatesEntries$' -count=1
