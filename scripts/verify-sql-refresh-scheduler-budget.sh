#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestManagedRefreshScheduler' -count=1
go vet ./hat/hatSql
