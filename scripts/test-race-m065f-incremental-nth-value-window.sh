#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestIncrementalNthValueWindow' -count=1
