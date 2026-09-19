#!/bin/sh
set -eu
go test ./hat/hatSql -run 'TestCH042' -count=1
