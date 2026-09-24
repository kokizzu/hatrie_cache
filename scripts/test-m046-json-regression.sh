#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestCH031|TestM046)' -count=1
