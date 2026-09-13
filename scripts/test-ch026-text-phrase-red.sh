#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQLContainsPhrase' -count=1
go test ./hat/hatCache -run 'TestSQLTextPhrase' -count=1
