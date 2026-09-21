#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestC225|TestSQLWindow|TestSQLArgExtreme|TestSQLQualify)' -count=1
