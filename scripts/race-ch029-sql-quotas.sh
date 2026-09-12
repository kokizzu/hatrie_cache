#!/bin/sh
set -eu

go test -race ./hat/hatSql ./hat/hatCache -run 'TestCH029' -count=1
