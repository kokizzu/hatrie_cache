#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestCH029' -count=1
