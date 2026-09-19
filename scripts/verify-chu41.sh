#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestSQLPreparedQueryCachePersistence' -count=1
go test -race ./hat/hatSql -run '^TestSQLPreparedQueryCachePersistence' -count=1
go vet ./hat/hatSql ./hat/hatCache
