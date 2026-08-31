#!/bin/sh
set -eu

go vet ./...
go test -race ./hat/hatSql -run '^TestManagedRefreshScheduler' -count=1
