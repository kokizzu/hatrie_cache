#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLSource(Ingestion|Offset)' -count=1
