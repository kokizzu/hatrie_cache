#!/usr/bin/env bash
set -eu

go test ./hat/hatCache -run '^TestExecuteSQLQuerySupportsTypedIPValues$'
go test ./hat/hatSchema -run '^TestSQLIPSchemaTypesValidateAndGenerate$'
