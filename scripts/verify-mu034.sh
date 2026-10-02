#!/usr/bin/env bash
set -euo pipefail

gocache="$PWD/build/mu034-go-cache"
GOCACHE="${GOCACHE:-$gocache}" go test ./hat/hatSql -run 'Test(MU034|QuerySubscription|QueryDifferential|QuerySubscriptions)' -count=1
GOCACHE="${GOCACHE:-$gocache}" go test -race ./hat/hatSql -run 'TestMU034HistoricalSubscription' -count=1
GOCACHE="${GOCACHE:-$gocache}" go vet ./hat/hatSql
