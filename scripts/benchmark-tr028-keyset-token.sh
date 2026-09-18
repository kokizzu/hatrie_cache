#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -v -run '^TestSQLKeysetTokenWireSize$' -bench '^BenchmarkSQLKeysetTokenEncodeDecode$' -benchmem -count=5
