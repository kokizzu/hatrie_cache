#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestCH028' -count=1
