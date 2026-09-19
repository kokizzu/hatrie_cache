#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestSQLResultCache(BypassesVolatileTopLevelQueries|MarksVolatileTopLevelQueries|CachesDeterministicTopLevelQueries|KeyRejectsVolatileSources)$' -count=1
