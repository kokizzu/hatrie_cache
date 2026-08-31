#!/bin/sh
set -eu

go test -race ./hat/hatSql -run '^TestManagedRefreshScheduler' -count=1
