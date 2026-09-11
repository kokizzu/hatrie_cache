#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQLArgExtreme' -count=1
