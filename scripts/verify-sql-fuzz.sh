#!/usr/bin/env sh
set -eu

duration="${SQL_FUZZ_TIME:-1s}"

for target in FuzzSQLParsersDoNotPanic FuzzExecuteSQLQueryDoesNotPanic FuzzSQLUDFDiagnosticsDoNotPanic; do
	go test -run '^$' -fuzz="$target" -fuzztime="$duration" -parallel=1 ./hat/hatCache
done
