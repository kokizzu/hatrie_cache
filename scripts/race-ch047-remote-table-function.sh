#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestRemoteTableFunction' -count=1
