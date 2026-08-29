#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestSQLExecutionArenaReusesRowsAndClearsDroppedTail$'
go test ./hat/hatCache -run '^TestSQLColumnarGenericFilterUsesExecutionArena$'
