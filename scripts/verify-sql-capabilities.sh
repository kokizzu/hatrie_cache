#!/usr/bin/env sh
set -eu

go test ./hat/hatSchema
go test ./hat/hatSql
go test ./hat/hatCache -run 'TestSQLProduction|TestMonitoring|TestSQLHTTP|TestSQLQuery|TestSQLCompatibility'
go test ./cmd/hatrie-modelgen
pnpm --dir svelte-mpa test
