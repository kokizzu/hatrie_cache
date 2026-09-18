#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQLPublication' -count=1
