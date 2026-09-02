#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run 'TestTypedTableMVCC' -count=1
