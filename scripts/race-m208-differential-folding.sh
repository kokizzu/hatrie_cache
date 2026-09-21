#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestM206|TestM208|TestDebezium)' -count=1
