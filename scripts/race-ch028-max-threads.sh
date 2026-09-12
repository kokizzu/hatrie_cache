#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestCH028' -count=1
