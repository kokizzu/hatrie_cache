#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestSQLResultCache(BypassesVolatileTopLevelQueries|MarksVolatileTopLevelQueries|CachesDeterministicTopLevelQueries|KeyRejectsVolatileSources)$' -count=1
