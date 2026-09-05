#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLResultCache' -count=1
