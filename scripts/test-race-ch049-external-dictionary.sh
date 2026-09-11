#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestSQLExternalDictionary' -count=1
