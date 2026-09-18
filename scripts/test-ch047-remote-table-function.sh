#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run '^TestRemoteTableFunction' -count=1
