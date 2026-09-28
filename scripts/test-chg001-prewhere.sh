#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLColumnarAutomaticPrewhere$' -count=1
