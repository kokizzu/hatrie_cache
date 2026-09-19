#!/bin/sh
set -eu
go test -race ./hat/hatSql -run 'TestCH042' -count=1
