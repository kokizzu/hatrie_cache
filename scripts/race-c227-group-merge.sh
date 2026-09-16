#!/bin/sh
set -eu
go test -race ./hat/hatSql -run '^(TestC227|TestCHG01)' -count=1
