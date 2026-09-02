#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run 'TestTypedTableMVCC' -count=1
