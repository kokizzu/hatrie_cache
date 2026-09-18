#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestSQLPublication' -count=1
